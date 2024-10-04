package org.jyafoo.mydb.backend.dm;

import org.jyafoo.mydb.backend.dm.dataItem.DataItem;
import org.jyafoo.mydb.backend.dm.logger.Logger;
import org.jyafoo.mydb.backend.dm.page.PageOne;
import org.jyafoo.mydb.backend.dm.pageCache.PageCache;
import org.jyafoo.mydb.backend.tm.TransactionManager;

/**
 * 数据管理器接口
 * <p>
 * DM 层提供了三个功能供上层使用，分别是读、插入和修改。
 * 修改是通过读出的 DataItem 实现的，于是 DataManager 只需要提供 read() 和 insert() 方法
 *
 * @author jyafoo
 * @since 2024/10/2
 */
public interface DataManager {
    /**
     * 根据uid获取数据项
     *
     * @param uid 数据项的唯一标识符
     * @return 如果数据项有效，则返回数据项；否则返回null
     * @throws Exception 如果读取过程中发生错误，抛出异常
     */
    DataItem read(long uid) throws Exception;

    /**
     * 插入数据到数据库中
     *
     * @param xid 事务ID
     * @param data 要插入的数据
     * @return 返回插入数据的唯一标识
     * @throws Exception 如果数据太大或数据库太忙而无法插入，则抛出异常
     */
    long insert(long xid, byte[] data) throws Exception;

    /**
     * 关闭资源
     */
    void close();

    static DataManager create(String path, long memory, TransactionManager tm) {
        PageCache pageCache = PageCache.create(path, memory);
        Logger logger = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pageCache, logger, tm);
        // 从空文件创建首先需要对第一页进行初始化
        dm.initPageOne();  //
        return dm;
    }

    /**
     * 打开或初始化数据管理器
     *
     * @param path   数据库文件路径
     * @param memory 内存大小
     * @param tm     事务管理器
     * @return 数据管理对象
     */
    static DataManager open(String path, long memory, TransactionManager tm) {
        // 初始化页面缓存，打开或创建数据库文件，并分配内存空间
        PageCache pageCache = PageCache.open(path, memory);
        // 初始化日志系统，用于记录数据库操作的日志
        Logger logger = Logger.open(path);

        DataManagerImpl dm = new DataManagerImpl(pageCache, logger, tm);

        // 从已有文件创建，则是需要对第一页进行校验，来判断是否需要执行恢复流程
        if (!dm.loadCheckPageOne()) {
            Recover.recover(tm, logger, pageCache);
        }

        // 填充页面索引，以便快速定位数据库文件中的页面
        dm.fillPageIndex();
        // 设置版本控制信息，表示数据库管理器已打开
        PageOne.setVcOpen(dm.pageOne);
        // 将更新后的版本控制信息刷新到数据库文件中
        dm.pageCache.flushPage(dm.pageOne);

        return dm;
    }

}
