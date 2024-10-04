package org.jyafoo.mydb.backend.vm;

import com.google.common.primitives.Bytes;
import lombok.Data;
import lombok.Getter;
import org.jyafoo.mydb.backend.common.SubArray;
import org.jyafoo.mydb.backend.dm.dataItem.DataItem;
import org.jyafoo.mydb.backend.utils.Parser;

import java.util.Arrays;

/**
 * VM 向上层抽象出 Entry，Entry 中保存一个 DataItem 的引用
 *
 * @author jyafoo
 * @since 2024/10/3
 */
//@Data
public class Entry {

    // entry结构：[XMIN] [XMAX] [data]
    /**
     * 偏移量：XMIN 是创建该条记录（版本）的事务编号，从第0个字节开始，占8个字节
     */
    private static final int OF_XMIN = 0;
    /**
     * 偏移量：XMAX 则是删除该条记录（版本）的事务编号，从第8个字节开始，占8个字节
     */
    private static final int OF_XMAX = OF_XMIN + 8;
    /**
     * 偏移量：数据的起始位置，从第15个字节开始
     */
    private static final int OF_DATA = OF_XMAX + 8;

    /**
     * 数据项唯一标识
     * -- GETTER --
     *  获取数据项唯一标识uid
     *
     * @return

     */
    @Getter
    private long uid;
    /**
     * 数据项
     */
    private DataItem dataItem;
    /**
     * 版本管理器
     */
    private VersionManager vm;

    /**
     * 创建一个新的entry对象
     *
     * @param vm       版本管理器
     * @param dataItem 数据项
     * @param uid      条目的唯一标识符
     * @return entry对象，如果数据项为null，则返回null
     */
    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        if (dataItem == null) {
            return null;
        }
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }

    /**
     * 根据版本管理和UID加载条目
     *
     * @param vm  版本管理器
     * @param uid 要加载的条目的唯一标识符
     * @return 返回一个新的条目对象，代表所加载的条目
     * @throws Exception 如果加载过程中出现问题或UID无效，则抛出异常
     */
    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        DataItem dataItem = ((VersionManagerImpl) vm).dm.read(uid);
        return newEntry(vm, dataItem, uid);
    }

    // TODO (jyafoo,2024/10/3,17:05) 觉得wrapEntryRaw的注释不准确
    /**
     * 对入口数据进行封装，生成原始的事务记录
     *
     * @param xid  事务标识，用于生成xmin
     * @param data 事务数据的字节数组
     */
    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }

    /**
     * 获取数据项中的
     * @return
     */
    public byte[] data() {
        dataItem.rLock();
        byte[] data;
        try {
            SubArray subArray = dataItem.data();
            // TODO (jyafoo,2024/10/3,17:10) Q：不理解为什么要减去OF_DATA？
            data = new byte[subArray.end - subArray.start - OF_DATA];
            System.arraycopy(subArray.raw, subArray.start + OF_DATA, data, 0, data.length);
            // 以拷贝的形式返回内容
            return data;
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 设置删除该条记录（版本）的事务编号
     * @param xid 事务xid
     */
    public void setXmax(long xid) {
        dataItem.before();
        try {
            SubArray subArray = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, subArray.raw, subArray.start + OF_XMAX, 8);
        } finally {
            dataItem.after(xid);
        }
    }

    /**
     * 获取创建该条记录（版本）的事务编号
     * @return 事务xid
     */
    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray subArray = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(subArray.raw, subArray.start + OF_XMIN, subArray.start + OF_XMAX));
        } finally {
            dataItem.rUnLock();
        }
    }


    /**
     * 获取删除该条记录（版本）的事务编号
     * @return 事务xid
     */
    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start + OF_XMAX, sa.start + OF_DATA));
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 释放entry对象的资源
     */
    public void release() {
        ((VersionManagerImpl) vm).releaseEntry(this);
    }

    public void remove() {
        dataItem.release();
    }
}
