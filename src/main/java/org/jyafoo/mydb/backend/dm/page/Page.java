package org.jyafoo.mydb.backend.dm.page;

/**
 * 数据页接口
 *
 * @author jyafoo
 * @since 2024/9/30
 */
public interface Page {

    /**
     * 加锁
     */
    void lock();

    /**
     * 解锁
     */
    void unlock();

    /**
     * 释放该页面内存
     */
    void release();

    /**
     * 设置脏页标志
     *
     * @param dirty true：脏页；false：不是脏页
     */
    void setDirty(boolean dirty);

    /**
     * 判断该内存页是否为脏页
     *
     * @return true：脏页；false：不是脏页
     */
    boolean isDirty();

    /**
     * 获取页号
     *
     * @return 页号
     */
    int getPageNumber();

    /**
     * 获取页实际包含的字节数据
     *
     * @return 数据
     */
    byte[] getData();
}
