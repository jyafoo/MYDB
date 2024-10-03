package org.jyafoo.mydb.backend.dm.pageCache;

import org.jyafoo.mydb.backend.dm.page.Page;
import org.jyafoo.mydb.backend.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.jyafoo.mydb.common.Error;

/**
 * 页面缓存（内存）操作接口
 *
 * @author jyafoo
 * @since 2024/9/30
 */
public interface PageCache {

    /**
     * 默认内存页大小为 8K
     */
    static final int PAGE_SIZE = 1 << 13;

    /**
     * 在数据库文件中创建一个新的页面
     *
     * @param initDate 初始化数据
     * @return 新创建页面的页面编号
     */
    int newPage(byte[] initDate);

    /**
     * 根据页号获取指定页面
     *
     * @param pgno 页号
     * @return 页对象
     * @throws Exception 异常
     */
    Page getPage(int pgno) throws Exception;

    /**
     * 关闭页面缓存对象
     */
    void close();

    /**
     * 释放指定页面的资源
     *
     * @param page 数据页
     */
    void release(Page page);

    /**
     * 根据页号截断文件
     * 此方法的目的是将文件截断至指定的页面背景号（bgno），确保文件长度不超过该背景号对应的字节位置
     *
     * @param maxPgno 最大页号，文件将截断至该页号对应的位置
     */
    void truncateByPgno(int maxPgno);

    /**
     * 获取缓存页数量
     *
     * @return 缓存页数量
     */
    int getPageNumbers();

    /**
     * 将页面刷新到数据库文件（磁盘中）
     *
     * @param page 要刷盘的内存页
     */
    void flushPage(Page page);

    /**
     * 创建页面缓存操作对象
     *
     * @param path   存储数据库文件的路径
     * @param memory 页面缓存大小
     * @return 页面缓存操作对象
     */
    static PageCacheImpl create(String path, long memory) {
        File file = new File(path + PageCacheImpl.DB_SUFFIX);
        try {
            if (!file.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fileChannel = null;
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(randomAccessFile, fileChannel, (int) memory / PAGE_SIZE);
    }

    /**
     * @param path   存储数据库文件的路径
     * @param memory 页面缓存大小
     * @return 页面缓存操作对象
     */
    static PageCacheImpl open(String path, long memory) {
        File file = new File(path + PageCacheImpl.DB_SUFFIX);
        if (!file.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fileChannel = null;
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(randomAccessFile, fileChannel, (int) memory / PAGE_SIZE);
    }
}
