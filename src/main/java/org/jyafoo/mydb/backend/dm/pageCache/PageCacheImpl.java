package org.jyafoo.mydb.backend.dm.pageCache;

import org.jyafoo.mydb.backend.common.AbstractCache;
import org.jyafoo.mydb.backend.dm.page.Page;
import org.jyafoo.mydb.backend.dm.page.PageImpl;
import org.jyafoo.mydb.backend.utils.Panic;
import org.jyafoo.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 页面缓存实现
 *
 * @author jyafoo
 * @since 2024/9/30
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    /**
     * 最小内存限制。这是系统或应用定义的一个阈值，用来确保分配的资源量至少达到某个最低标准
     */
    private static final int MEN_MIN_LIM = 10;

    /**
     * 数据库文件的后缀名
     */
    public static final String DB_SUFFIX = ".db";

    /**
     * 文件随机访问对象，用于读写文件
     */
    private RandomAccessFile file;

    /**
     * 文件通道对象，用于管理文件的读写通道
     */
    private FileChannel fileChannel;

    /**
     * 文件锁对象，用于同步访问文件
     */
    private Lock fileLock;

    /**
     * 记录当前打开的数据库文件有多少页。在数据库文件被打开时数量就会被计算，并在新建页面时自增
     */
    private AtomicInteger pageNumbers;

    public PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        // 检查最大资源限制是否小于最小内存限制
        if (maxResource < MEN_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }

        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }

        this.file = file;
        this.fileChannel = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int) length / PAGE_SIZE);
    }

    // TODO (jyafoo,2024/9/30,14:31) 注意：同一条数据是不允许跨页存储的，这一点会从后面的章节中体现。这意味着，单条数据的大小不能超过数据库页面的大小。

    @Override
    public int newPage(byte[] initDate) {
        int pgno = pageNumbers.incrementAndGet();
        PageImpl page = new PageImpl(pgno, initDate, null);
        flushPage(page); // 新建的页面需要立刻写回数据库文件
        return pgno;
    }

    @Override
    public Page getPage(int pgno) throws Exception {
        return get(pgno);
    }

    @Override
    public void release(Page page) {
        release(page.getPageNumber());
    }

    @Override
    public void truncateByPgno(int maxPgno) {
        // TODO (jyafoo,2024/9/30,17:41) Q2：这个截断过程不是很懂，为什么要截断，有什么用？
        long size = pageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }

        pageNumbers.set(maxPgno);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page page) {
        flush(page);
    }


    /**
     * 根据pageNumber从数据库文件中读取页数据，并包裹成Page
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int) key;
        long offset = PageCacheImpl.pageOffset(pgno);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();

        try {
            fileChannel.position(offset);
            fileChannel.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }

        return new PageImpl(pgno, buf.array(), this);
    }

    /**
     * 计算给定页码在在文件中的偏移量
     *
     * @param pgno 页码（从1开始）
     * @return 该页的偏移量
     */
    private static long pageOffset(int pgno) {
        return (long) (pgno - 1) * PAGE_SIZE;
    }

    /**
     * 驱逐页面，需要根据页面是否是脏页面，来决定是否需要写回文件系统
     */
    @Override
    protected void releasForCache(Page page) {
        if (page.isDirty()) {
            flush(page);
            page.setDirty(false);
        }
    }

    /**
     * 将页面刷新到数据库文件（磁盘中）
     *
     * @param page 要刷盘的内存页
     */
    void flush(Page page) {
        int pgno = page.getPageNumber();
        long offset = pageOffset(pgno);

        fileLock.lock();

        try {
            ByteBuffer buf = ByteBuffer.wrap(page.getData());
            fileChannel.position(offset);
            fileChannel.write(buf);
            fileChannel.force(false);  // 将文件更改强制写入磁盘，false表示不阻塞其他写操作，true表示阻塞直到写操作完成
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    @Override
    public void close() {
        super.close();
        try {
            fileChannel.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

}
