package org.jyafoo.mydb.backend.dm.dataItem;

import com.google.common.primitives.Bytes;
import org.jyafoo.mydb.backend.common.SubArray;
import org.jyafoo.mydb.backend.dm.DataManagerImpl;
import org.jyafoo.mydb.backend.dm.page.Page;
import org.jyafoo.mydb.backend.utils.Parser;
import org.jyafoo.mydb.backend.utils.Types;

import java.util.Arrays;

/**
 * DataItem 是 DM 层向上层提供的数据抽象
 *
 * @author jyafoo
 * @since 2024/10/1
 */
public interface DataItem {

    /**
     * 获取原始数据的共享内存对象
     *
     * @return 原始数据的共享内存对象
     */
    SubArray data();

    /*
        在上层模块试图对 DataItem 进行修改时，需要遵循一定的流程：
            在修改之前需要调用 before() 方法，想要撤销修改时，调用 unBefore() 方法。
            在修改完成后，调用 after() 方法。
        整个流程，主要是为了保存前相数据，并及时落日志。DM 会保证对 DataItem 的修改是原子性的。
     */

    /**
     * 在数据变更前执行的操作
     */
    void before();

    /**
     * 撤销变更，恢复到before状态
     */
    void unBefore();

    /**
     * 事务完成，生成更新日志
     *
     * @param xid 事务xid
     */
    void after(long xid);

    /**
     * 释放数据项占有资源
     */
    void release();

    /**
     * 上读锁
     */
    void wLock();

    /**
     * 释放读锁
     */
    void wUnLock();

    /**
     * 上写锁
     */
    void rLock();

    /**
     * 释放写锁
     */
    void rUnLock();

    /**
     * 获取数据项所属页面对象
     *
     * @return 页面对象
     */
    Page page();

    /**
     * 获取数据项唯一标识
     *
     * @return 数据项唯一标识
     */
    long getUid();

    /**
     * 获取旧的原始数据
     *
     * @return 旧的原始数据
     */
    byte[] getOldRaw();

    /**
     * 获取原始数据的共享内存对象
     *
     * @return 原始数据的共享内存对象
     */
    SubArray getRaw();


    /**
     * 封装数据项
     *
     * @param raw 原始数据字节数组
     * @return 封装后的字节数组，包括有效性标志、数据长度和原始数据
     */
    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, size, raw);
    }

    /**
     * 解析数据项
     *
     * @param page   页面对象
     * @param offset 数据项在页面中的起始偏移量
     * @param dm     数据管理器
     * @return 数据项对象
     */
    static DataItem parseDataItem(Page page, short offset, DataManagerImpl dm) {
        byte[] raw = page.getData();
        // 解析数据项大小
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
        // 计算数据项的长度，包括元数据
        short length = (short) (size + DataItemImpl.OF_DATA);
        // 据页面编号和偏移量计算出数据项的唯一标识
        long uid = Types.addressToUid(page.getPageNumber(), offset);

        return new DataItemImpl(new SubArray(raw, offset, offset + length), new byte[length], page, uid, dm);
    }

    /**
     * 将数据项设为无效，用于回滚操作
     *
     * @param raw 原始数据
     */
    static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte) 1;
    }

}
