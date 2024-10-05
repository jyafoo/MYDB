package org.jyafoo.mydb.backend.dm.dataItem;

import org.jyafoo.mydb.backend.common.SubArray;
import org.jyafoo.mydb.backend.dm.DataManagerImpl;
import org.jyafoo.mydb.backend.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 数据项实现
 *
 * @author jyafoo
 * @since 2024/10/2
 */
public class DataItemImpl implements DataItem {

    // dataItem 结构：[ValidFlag] [DataSize] [Data]
    /**
     * 偏移量：ValidFlag 占1字节，从第0字节开始，标识该 DataItem 是否有效。0为合法，1为非法
     */
    static final int OF_VALID = 0;
    /**
     * 偏移量：DataSize，占2字节，从第1字节开始。标识Data的长度
     */
    static final int OF_SIZE = 1;
    /**
     * 偏移量：数据开始的位置，从第3字节开始。
     */
    static final int OF_DATA = 3;

    /**
     * 原始数据的共享内存对象
     */
    private SubArray raw;
    /**
     * 旧的原始数据
     */
    private byte[] oldRaw;
    /**
     * 读锁
     */
    private Lock rLock;
    /**
     * 写锁
     */
    private Lock wLock;
    /**
     * 数据管理器
     */
    private DataManagerImpl dm;
    /**
     * 数据项唯一标识
     */
    private long uid;
    /**
     * 所属页面对象
     */
    private Page page;

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page page, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.dm = dm;
        this.uid = uid;
        this.page = page;
    }

    /**
     * 检查dataItem对象的有效性
     *
     * @return 如果对象有效，返回true；否则返回false
     */
    public boolean isValid() {
        return raw.raw[raw.start + OF_VALID] == (byte) 0;
    }

    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start + OF_DATA, raw.end);
    }

    @Override
    public void before() {
        wLock.lock();
        page.setDirty(true);
        // 复制当前原始数据，以便在数据变更后能够恢复到此状态，即记录旧数据
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();
    }

    @Override
    public void after(long xid) {
        dm.logDataItem(xid,this);
        wLock.unlock();
    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void wLock() {
        wLock.lock();
    }

    @Override
    public void wUnLock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return page;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
}
