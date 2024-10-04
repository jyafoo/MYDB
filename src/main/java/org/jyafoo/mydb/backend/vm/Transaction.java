package org.jyafoo.mydb.backend.vm;

import org.jyafoo.mydb.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * 抽象一个事务，以保存快照数据
 *
 * @author jyafoo
 * @since 2024/10/3
 */
public class Transaction {
    /**
     * 事务xid
     */
    public long xid;

    /**
     * 事务的隔离级别：0表示RC，1表示RR
     */
    public int level;

    /**
     * 事务快照，key：xid，value：isActive
     * 用于多版本并发控制(MVCC)，存储了在事务的活跃状态。
     * 当事务结束，会从快照中移除。
     */
    public Map<Long, Boolean> snapshot;

    /**
     * 事务在执行过程中遇到异常
     */
    public Exception err;

    /**
     * 标识事务是否自动中止
     * 在某些情况下，事务可能需要被系统自动中止，例如检测到死锁
     */
    public boolean autoAborted;

    /**
     * 创建并返回一个新的事务对象
     *
     * @param xid    事务的唯一标识符
     * @param level  事务的隔离级别，决定事务的快照视图范围
     * @param active 保存着当前所有 active 的事务的集合
     * @return 新事务对象
     */
    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction transaction = new Transaction();
        transaction.xid = xid;
        transaction.level = level;
        // TODO (jyafoo,2024/10/3,19:00) Q：没懂这个快照复制的含义？
        if (level != 0) {
            // 初始化当前事务的快照视图
            transaction.snapshot = new HashMap<>();
            // 遍历所有活跃事务，将当前所有活跃事务的唯一标识符添加到快照视图中
            for (Long x : active.keySet()) {
                transaction.snapshot.put(x, true);
            }
        }
        return transaction;
    }

    /**
     * 判断事务是否存在于快照中，当事务结束，会从快照中移除
     * @param xid 事务唯一标识
     * @return 如果事务在快照中，则返回true；否则返回false
     */
    public boolean isInSnapshot(long xid) {
        if (xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }


}
