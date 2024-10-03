package org.jyafoo.mydb.backend.dm;

import org.jyafoo.mydb.backend.common.AbstractCache;
import org.jyafoo.mydb.backend.dm.dataItem.DataItem;
import org.jyafoo.mydb.backend.dm.dataItem.DataItemImpl;
import org.jyafoo.mydb.backend.dm.logger.Logger;
import org.jyafoo.mydb.backend.dm.page.Page;
import org.jyafoo.mydb.backend.dm.page.PageOne;
import org.jyafoo.mydb.backend.dm.page.PageX;
import org.jyafoo.mydb.backend.dm.pageCache.PageCache;
import org.jyafoo.mydb.backend.dm.pageIndex.PageIndex;
import org.jyafoo.mydb.backend.dm.pageIndex.PageInfo;
import org.jyafoo.mydb.backend.tm.TransactionManager;
import org.jyafoo.mydb.backend.utils.Panic;
import org.jyafoo.mydb.backend.utils.Types;
import org.jyafoo.mydb.common.Error;

/**
 * 数据管理器实现
 *
 * @author jyafoo
 * @since 2024/10/2
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {


    TransactionManager tm;
    PageCache pageCache;
    Logger logger;
    PageIndex pageIndex;
    /**
     * 数据库文件第一页
     */
    Page pageOne;

    public DataManagerImpl(PageCache pageCache, Logger logger, TransactionManager tm) {
        super(0);
        this.pageCache = pageCache;
        this.logger = logger;
        this.tm = tm;
        this.pageIndex = new PageIndex();
    }

    /**
     * 在创建文件时初始化PageOne
     */
    public void initPageOne() {
        int pgno = pageCache.newPage(PageOne.initRaw());
        assert pgno == 1;
        try {
            pageOne = pageCache.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pageCache.flushPage(pageOne);
    }

    /**
     * 填充页面索引
     * <p>
     * 在 DataManager 被创建时，需要获取所有页面并填充 PageIndex
     */
    public void fillPageIndex() {
        int pageNumber = pageCache.getPageNumbers();
        // 从2开始是因为没有第0页，第1页是特殊页，2及之后才是普通页
        for (int i = 2; i <= pageNumber; i++) {
            Page page = null;
            try {
                page = pageCache.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pageIndex.add(page.getPageNumber(), PageX.getFreeSpace(page));
            page.release(); // 在使用完 Page 后需要及时 release，否则可能会撑爆缓存
        }
    }

    /**
     * 释放数据项
     * <p>
     * 当数据项不再需要时，通过此方法将其释放，以进行垃圾回收或资源释放
     *
     * @param dataItem 要释放的数据项对象
     */
    public void releaseDataItem(DataItem dataItem) {
        super.release(dataItem.getUid());
    }

    /**
     * 为事务xid生成更新日志
     *
     * @param xid      事务xid
     * @param dataItem 要更新的数据项
     */
    public void logDataItem(long xid, DataItem dataItem) {
        byte[] updateLog = Recover.updateLog(xid, dataItem);
        logger.log(updateLog);
    }

    /**
     * 在打开已有文件时时读入PageOne，并验证正确性
     *
     * @return PageOne验证通过返回true，否则返回false
     */
    public boolean loadCheckPageOne() {
        try {
            pageOne = pageCache.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl dataItemImpl = (DataItemImpl) super.get(uid);

        if (!dataItemImpl.isValid()) {
            dataItemImpl.release();
            return null;
        }

        return dataItemImpl;
    }

    /**
     * 插入数据到数据库中
     *
     * @param xid 事务ID
     * @param data 要插入的数据
     * @return 返回插入数据的唯一标识
     * @throws Exception 如果数据太大或数据库太忙而无法插入，则抛出异常
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if (raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        PageInfo pageInfo = null;
        // 尝试找到合适的页面插入数据，最多尝试5次
        for (int i = 0; i < 5; i++) {
            pageInfo = pageIndex.select(raw.length);
            if (pageInfo != null) {
                break;
            } else {
                // 如果没有找到合适的页面，则创建一个新的页面
                int newPgno = pageCache.newPage(PageX.initRaw());
                // 将新页面添加到页面索引中
                pageIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        // 如果没有找到合适的页面，抛出异常
        if (pageInfo == null) {
            throw Error.DatabaseBusyException;
        }

        Page page = null;
        int freeSpace = 0;
        try {
            // 获取页面
            page = pageCache.getPage(pageInfo.pgno);
            // 生成插入操作的日志记录
            byte[] log = Recover.insertLog(xid, page, raw);
            // 将日志记录写入日志文件
            logger.log(log);

            // 在页面中插入数据
            short offset = PageX.insert(page, raw);

            // 释放页面
            page.release();
            // 返回插入数据的唯一标识
            return Types.addressToUid(pageInfo.pgno, offset);
        } finally {
            // 无论是否成功插入数据，都需要更新页面索引
            // 如果成功获取了页面，则根据实际使用的空间更新页面索引
            // TODO (jyafoo,2024/10/2,20:37) 对于页面索引的更新不理解
            if (page != null) {
                pageIndex.add(pageInfo.pgno, PageX.getFreeSpace(page));
            } else {
                pageIndex.add(pageInfo.pgno, freeSpace);
            }
        }
    }

    /**
     * 从 key 中解析出页号，从 pageCache 中获取到页面，再根据偏移，解析出 DataItem 即可
     */
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int) (uid & ((1L << 32) - 1));
        Page page = pageCache.getPage(pgno);
        return DataItem.parseDataItem(page, offset, this);
    }

    /**
     * DataItem 缓存释放，需要将 DataItem 写回数据源，由于对文件的读写是以页为单位进行的，只需要将 DataItem 所在的页 release 即可
     */
    @Override
    protected void releaseForCache(DataItem dataItem) {
        dataItem.page().release();
    }

    /**
     * DataManager正常关闭时，需要执行缓存和日志的关闭流程，同时设置第一页的字节校验
     */
    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pageCache.close();
    }

}
