package org.jyafoo.mydb.backend.vm;

import org.jyafoo.mydb.backend.dm.DataManager;
import org.jyafoo.mydb.backend.tm.TransactionManager;

/**
 * 版本管理器，实现了调度序列的可串行化，向上层提供功能
 *
 * @author jyafoo
 * @since 2024/10/3
 */
public interface VersionManager {

    /**
     * 读取一个entry
     *
     * @param xid 事务ID，标识当前操作的事务
     * @param uid 对象ID，标识要读取的对象
     * @return 如果对象可见，则返回对象的数据；否则返回null
     * @throws Exception 如果在读取过程中发生错误，或事务中存在错误，则抛出异常
     */
    byte[] read(long xid, long uid) throws Exception;

    /**
     * 将数据包裹成 Entry，交给 DM 插入
     *
     * @param xid 事务标识符
     * @param data 要插入的数据
     * @return 插入操作的结果，返回值类型为long，表示插入数据的ID
     * @throws Exception 如果插入操作失败，或者指定的事务不存在或已出错，将抛出异常
     */
    long insert(long xid, byte[] data) throws Exception;

    /**
     * 删除版本管理器中的数据
     * @param xid
     * @param uid
     * @return
     * @throws Exception
     */
    boolean delete(long xid, long uid) throws Exception;

    /**
     * 开启一个新事务
     *
     * @param level 隔离级别
     * @return 事务xid
     */
    long begin(int level);

    /**
     *
     * @param xid
     * @throws Exception
     */
    void commit(long xid) throws Exception;

    /**
     *
     * @param xid
     */
    void abort(long xid);

    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }

}
