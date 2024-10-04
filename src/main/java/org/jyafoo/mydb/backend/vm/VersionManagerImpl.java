package org.jyafoo.mydb.backend.vm;

import org.jyafoo.mydb.backend.common.AbstractCache;
import org.jyafoo.mydb.backend.dm.DataManager;
import org.jyafoo.mydb.backend.tm.TransactionManager;
import org.jyafoo.mydb.backend.tm.TransactionManagerImpl;
import org.jyafoo.mydb.common.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 版本管理器实现，同时VM的实现类还被设计为Entry的缓存
 *
 * @author jyafoo
 * @since 2024/10/3
 */
public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    /**
     * 事务管理器
     */
    TransactionManager tm;
    /**
     * 数据管理器
     */
    DataManager dm;
    /**
     * 正在执行中的事务，key：事务xid，value：事务快照
     */
    Map<Long, Transaction> activeTransaction;

    /**
     * 锁
     */
    Lock lock;

    /**
     * 依赖等待表
     */
    LockTable lockTable;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lockTable = new LockTable();
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if (entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }

    /**
     * 读取entry时注意判断可见性
     */
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        lock.unlock();

        if (transaction.err != null) {
            throw transaction.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch (Exception e) {
            if (e == Error.NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }

        try {
            if (Visibility.isVisible(tm, transaction, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        lock.unlock();

        if (transaction.err != null) {
            throw transaction.err;
        }

        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    /**
     * 删除实际上主要是前置的三件事：
     * 一是可见性判断，
     * 二是获取资源的锁，
     * 三是版本跳跃判断。
     * 删除的操作只有一个设置 XMAX
     */
    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        lock.unlock();

        if (transaction.err != null) {
            throw transaction.err;
        }

        // 1、获取entry对象
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch (Exception e) {
            if (e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }

        try {
            // 2、可见性判断
            if (!Visibility.isVisible(tm, transaction, entry)) {
                return false;
            }

            // 3、获取资源的锁
            Lock lock0 = null;
            try {
                lock0 = lockTable.add(xid, uid);
            } catch (Exception e) {
                transaction.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                transaction.autoAborted = true;
                throw transaction.err;
            }

            // TODO (jyafoo,2024/10/4,14:07) Q：为什么判空之后加锁又解锁？
            if(lock0 != null){
                lock0.lock();
                lock0.unlock();
            }

            if(entry.getXmax() == xid){
                return false;
            }

            // TODO (jyafoo,2024/10/4,14:10) Q：这里的版本跳跃判断理解的有些割裂，思路跟不上了
            // 4、版本跳跃判断
            if(Visibility.isVersionSkip(tm,transaction,entry)){
                transaction.err = Error.ConcurrentUpdateException;
                internAbort(xid,true);
                transaction.autoAborted = true;
                throw transaction.err;
            }

            entry.setXmax(xid);
            return true;
        } finally {
            entry.release();
        }
    }

    /**
     * 开启一个事务，并初始化事务的结构，将其存放在 activeTransaction 中，用于检查和快照使用
     */
    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction transaction = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, transaction);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 提交一个事务，主要就是 free 掉相关的结构，并且释放持有的锁，并修改 TM 状态
     */
    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        lock.unlock();

        try {
            if (transaction.err != null) {
                throw transaction.err;
            }
        } catch (Exception e) {
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        lockTable.remove(xid);
        tm.commit(xid);
    }

    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    /**
     * abort 事务的方法则有两种，手动和自动。
     * 手动指的是调用 abort() 方法，而自动，则是在事务被检测出出现死锁时，会自动撤销回滚事务；
     * 或者出现版本跳跃时，也会自动回滚
     *
     * @param xid         事务xid
     * @param autoAborted 事务是否是自动中止的标志
     */
    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        if (!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();

        if (transaction.autoAborted) {
            return;
        }

        lockTable.remove(xid);
        tm.abort(xid);
    }

    /**
     * 释放指定的Entry资源
     *
     * @param entry 要释放的Entry对象
     */
    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }
}
