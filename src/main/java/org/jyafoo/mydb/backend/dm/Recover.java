package org.jyafoo.mydb.backend.dm;

import com.google.common.primitives.Bytes;
import org.jyafoo.mydb.backend.common.SubArray;
import org.jyafoo.mydb.backend.dm.dataItem.DataItem;
import org.jyafoo.mydb.backend.dm.logger.Logger;
import org.jyafoo.mydb.backend.dm.page.Page;
import org.jyafoo.mydb.backend.dm.page.PageX;
import org.jyafoo.mydb.backend.dm.pageCache.PageCache;
import org.jyafoo.mydb.backend.tm.TransactionManager;
import org.jyafoo.mydb.backend.utils.Panic;
import org.jyafoo.mydb.backend.utils.Parser;
import org.jyafoo.mydb.common.Error;

import java.util.*;

/**
 * @author jyafoo
 * @since 2024/9/30
 */
public class Recover {
    /**
     * 日志记录的操作类型：插入
     */
    private static final byte LOG_TYPE_INSERT = 0;
    /**
     * 日志记录的操作类型：修改
     */
    private static final byte LOG_TYPE_UPDATE = 1;

    /**
     * 重做日志操作
     */
    private static final int REDO = 0;
    /**
     * 回滚日志操作
     */
    private static final int UNDO = 1;

    // 通用偏移量
    /**
     * 偏移量：日志类型，占1字节
     */
    private static final int OF_TYPE = 0;
    /**
     * 偏移量：事务xid，占8字节
     */
    private static final int OF_XID = OF_TYPE + 1;

    /**
     * 更新的日志格式
     * updateLog：[LogType] [XID] [UID] [OldRaw] [NewRaw]
     */
    static class UpdateLogInfo {
        long xid;       // 事务ID
        int pgno;       // 页码
        short offset;   // 偏移量，表示数据在页面中的具体位置
        byte[] oldRaw;  // 更新前数据的原始字节码，用于记录旧的数据内容
        byte[] newRaw;  // 更新后数据的新字节码，用于记录新的数据内容
    }

    // update偏移量
    /**
     * 偏移量：更新类型uid，占8字节
     */
    private static final int OF_UPDATE_UID = OF_XID + 8;
    /**
     * 偏移量：更新数据：新数据和旧数据
     */
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    /**
     * 插入的日志格式
     * insertLog：[LogType] [XID] [Pgno] [Offset] [Raw]
     */
    static class InsertLogInfo {
        long xid;       // 事务ID
        int pgno;       // 页码
        short offset;   // 偏移量，表示数据在页面中的具体位置
        byte[] raw;     // 插入数据的原始字节码，用于记录插入的数据内容
    }

    // insert偏移量
    /**
     * 偏移量：页号，占4字节
     */
    private static final int OF_INSERT_PGNO = OF_XID + 8;
    /**
     * 偏移量：数据在页面中的具体位置，占2字节
     */
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;
    /**
     * 偏移量：插入数据的起始位置
     */
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;


    /**
     * 重做事务
     * <p>
     * 在系统崩溃后，通过日志重做未完成的事务，以保证数据的一致性
     *
     * @param tm        事务管理器
     * @param logger    日志管理器
     * @param pageCache 页面缓存
     */
    // TODO (jyafoo,2024/10/1,20:23) redoTransactions这块不熟，需要再看，尤其是doInsertLog和doUpdateLog
    private static void redoTransactions(TransactionManager tm, Logger logger, PageCache pageCache) {
        logger.rewind();

        while (true) {
            byte[] log = logger.next();
            if (log == null) {
                break;
            }

            // 判断日志类型
            if (isInsertLog(log)) {
                InsertLogInfo insertLogInfo = parseInsertLog(log);

                long xid = insertLogInfo.xid;
                if (!tm.isActive(xid)) {
                    doInsertLog(pageCache, log, REDO);
                }
            } else {
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);

                long xid = updateLogInfo.xid;
                if (!tm.isActive(xid)) {
                    doUpdateLog(pageCache, log, REDO);
                }
            }
        }
    }

    /**
     * 回滚事务
     * <p>
     * 通过分析日志并执行逆操作来撤销未提交的事务，以确保事务的原子性和数据库的一致性
     *
     * @param tm        事务管理器
     * @param logger    日志管理器
     * @param pageCache 页面缓存
     */
    private static void undoTransactions(TransactionManager tm, Logger logger, PageCache pageCache) {
        HashMap<Long, List<byte[]>> logCache = new HashMap<>(); // 缓存日志，键为事务ID，值为该事务的所有日志
        logger.rewind();

        while (true) {
            byte[] log = logger.next();
            if (log == null) {
                break;
            }

            if (isInsertLog(log)) {
                InsertLogInfo insertLogInfo = parseInsertLog(log);

                long xid = insertLogInfo.xid;
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                long xid = updateLogInfo.xid;
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        // 对所有activelog进行倒序undo
        for (Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue(); // 可能一个事务涉及到多条日志
            for (int i = logs.size() - 1; i >= 0; i--) {
                byte[] log = logs.get(i);
                if (isInsertLog(log)) {
                    doInsertLog(pageCache, log, UNDO);
                } else {
                    doUpdateLog(pageCache, log, UNDO);
                }
            }
            // 撤销事务
            tm.abort(entry.getKey());
        }
    }

    /**
     * 判断是否为插入操作日志
     *
     * @param log 日志字节数组
     * @return 如果日志类型为插入，则返回true；否则返回false
     */
    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    /**
     * 判断是否为更新操作日志
     *
     * @param log 日志字节数组
     * @return 如果日志类型为更新，则返回true；否则返回false
     */
    private static boolean isUpdateLog(byte[] log) {
        return log[0] == LOG_TYPE_UPDATE;
    }

    /**
     * 解析log封装插入日志对象
     *
     * @param log 插入日志的字节数组
     * @return 插入日志信息的InsertLogInfo对象
     */
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo insertLogInfo = new InsertLogInfo();
        insertLogInfo.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        insertLogInfo.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        insertLogInfo.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        insertLogInfo.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return insertLogInfo;
    }

    /**
     * 执行插入日志操作
     *
     * @param pageCache 页面缓存对象，用于获取页面
     * @param log       要操作的日志
     * @param flag      操作标志，表示是重做插入还是撤销插入操作
     */
    private static void doInsertLog(PageCache pageCache, byte[] log, int flag) {
        InsertLogInfo insertLogInfo = parseInsertLog(log);

        Page page = null;
        try {
            page = pageCache.getPage(insertLogInfo.pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            if (flag == UNDO) {
                // TODO (jyafoo,2024/10/1,20:20) 写完dataitem回来释放
                 DataItem.setDataItemRawInvalid(insertLogInfo.raw);
            }
            PageX.recoverInsert(page, insertLogInfo.raw, insertLogInfo.offset);
        } finally {
            page.release();
        }
    }


    /**
     * 解析log封装更新日志对象
     *
     * @param log 更新日志字节数组
     * @return 更新日志信息的UpdateLogInfo对象
     */
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo updateLogInfo = new UpdateLogInfo();

        // 解析并存储事务标识（xid）信息
        updateLogInfo.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));

        // TODO (jyafoo,2024/10/1,20:32) Q7：这看懵逼了，uid & ((1L << 16) - 1)是什么操作？A：因为offset和pgno并没有在日志格式中体现出来，即uid包含了offset和pgno，所以根据这两个字段的字节数从uid中推导出来。猜的。
        // 提取并存储偏移量信息
        updateLogInfo.offset = (short) (uid & ((1L << 16) - 1));
        // 将UID右移以提取页面编号信息
        uid >>>= 32;
        updateLogInfo.pgno = (int) (uid & ((1L << 32) - 1));

        // 计算旧数据片段和新数据片段的长度，并根据此长度提取相应的数据
        int length = (log.length - OF_UPDATE_RAW) / 2;
        updateLogInfo.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        updateLogInfo.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + length * 2);

        return updateLogInfo;
    }

    /**
     * 执行更新日志操作
     *
     * @param pageCache 页面缓存对象，用于获取页面
     * @param log       要操作的日志
     * @param flag      操作标志，表示是重做更新还是回滚更新操作
     */
    private static void doUpdateLog(PageCache pageCache, byte[] log, int flag) {

        UpdateLogInfo updateLogInfo = parseUpdateLog(log);
        int pgno = updateLogInfo.pgno;
        short offset = updateLogInfo.offset;
        byte[] raw = null;
        if (flag == REDO) {
            raw = updateLogInfo.newRaw;     // 重做日志用后相
        } else if (flag == UNDO) {
            raw = updateLogInfo.oldRaw;    // 回滚日志用前相
        } else {
            Panic.panic(Error.InvalidTypeException);
        }

        Page page = null;

        try {
            page = pageCache.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            PageX.recoverUpdate(page, raw, offset);
        } finally {
            page.release();
        }
    }


    /**
     * 构造更新日志条目
     *
     * @param xid      事务ID
     * @param dataItem 数据项对象，包含需要更新的数据项以及其相关元信息
     * @return 构造好的日志字节数组
     */
    public static byte[] updateLog(long xid, DataItem dataItem) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(dataItem.getUid());
        byte[] oldRaw = dataItem.getOldRaw();
        SubArray raw = dataItem.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }


    /**
     * 构造插入日志条目
     *
     * @param xid  事务ID
     * @param page 页对象
     * @param raw  原始数据
     * @return 构造好的日志字节数组
     */
    public static byte[] insertLog(long xid, Page page, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(page.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(page));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    /**
     * 系统恢复，通过redo和undo来恢复数据库的一致性
     *
     * @param tm        事务管理器
     * @param logger    日志管理器
     * @param pageCache 页缓存管理
     */
    public static void recover(TransactionManager tm, Logger logger, PageCache pageCache) {
        System.out.println("Recovering...");

        logger.rewind();

        // 寻找最大页号
        // TODO (jyafoo,2024/10/2,10:18) Q8：为什么要找最大页号？
        int maxPgno = 0;
        while (true) {
            byte[] log = logger.next();
            if (log == null) {
                break;
            }

            int pgno;
            if (isInsertLog(log)) {
                InsertLogInfo insertLogInfo = parseInsertLog(log);
                pgno = insertLogInfo.pgno;
            } else {
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                pgno = updateLogInfo.pgno;
            }

            maxPgno = Math.max(maxPgno, pgno);
        }

        maxPgno = maxPgno == 0 ? 1 : maxPgno;

        pageCache.truncateByPgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        redoTransactions(tm, logger, pageCache);
        System.out.println("Redo Transactions Over.");

        undoTransactions(tm, logger, pageCache);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }
}

