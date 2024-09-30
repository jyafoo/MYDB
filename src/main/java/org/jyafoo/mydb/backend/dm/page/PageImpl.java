package org.jyafoo.mydb.backend.dm.page;

import org.jyafoo.mydb.backend.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 数据页实现
 *
 * @author jyafoo
 * @since 2024/9/30
 */
public class PageImpl implements Page {

    /**
     * 页号
     */
    private int pageNumber;

    /**
     * 页实际包含的字节数据
     */
    private byte[] data;

    /**
     * 脏页标志
     */
    private boolean dirty = false;

    /**
     * 锁
     */
    private Lock lock;

    /**
     * 保存了一个 PageCache 的引用，用来方便在拿到 Page 的引用时可以快速对这个页面的缓存进行释放操作
     */
    private PageCache pageCache;

    public PageImpl(int pageNumber, byte[] data, PageCache pageCache) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pageCache = pageCache;
        lock = new ReentrantLock();
    }


    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {
        pageCache.release(this);
    }

    @Override
    public void setDirty(boolean status) {
        dirty = status;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public int getPageNumber() {
        return pageNumber;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}
