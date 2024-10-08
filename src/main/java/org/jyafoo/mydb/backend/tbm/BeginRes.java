package org.jyafoo.mydb.backend.tbm;

/**
 * 事务的开始资源对象
 *
 * @author jyafoo
 * @since 2024/10/5
 */
public class BeginRes {
    /**
     * 事务xid
     */
    public long xid;

    /**
     * 操作结果或数据的存储
     */
    public byte[] result;
}
