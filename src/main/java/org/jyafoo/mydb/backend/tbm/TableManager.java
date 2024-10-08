package org.jyafoo.mydb.backend.tbm;

import org.jyafoo.mydb.backend.dm.DataManager;
import org.jyafoo.mydb.backend.parse.statement.*;
import org.jyafoo.mydb.backend.utils.Parser;
import org.jyafoo.mydb.backend.vm.VersionManager;

/**
 * 数据库表管理操作接口
 *
 * @author jyafoo
 * @since 2024/10/5
 */
public interface TableManager {
    /**
     * 开始一个新的事务资源
     *
     * @param begin 事务的开始对象，包含事务初始化所需的必要信息
     * @return 返回事务的开始资源对象，用于后续的事务操作
     */
    BeginRes begin(Begin begin);

    /**
     * 提交事务
     *
     * @param xid 事务的唯一标识符，用于标识特定的事务
     * @return 返回事务提交的结果，通常是一个表示成功或失败的状态码
     * @throws Exception 如果事务提交过程中发生任何异常，抛出一个异常
     */
    byte[] commit(long xid) throws Exception;

    /**
     * 回滚事务
     *
     * @param xid 事务的唯一标识符，用于标识特定的事务
     * @return 返回事务回滚的结果，通常是一个表示成功或失败的状态码
     */
    byte[] abort(long xid);

    /**
     * 显示事务状态
     *
     * @param xid 事务的唯一标识符，用于标识特定的事务
     * @return 返回事务的当前状态信息
     */
    byte[] show(long xid);

    /**
     * 创建数据库对象
     *
     * @param xid 事务的唯一标识符，用于标识特定的事务
     * @param create 包含创建数据库所需信息的对象
     * @return 返回数据库创建的结果
     * @throws Exception 如果数据库创建过程中发生任何异常，抛出一个异常
     */
    byte[] create(long xid, Create create) throws Exception;

    /**
     * 向数据库中插入数据
     *
     * @param xid 事务的唯一标识符，用于标识特定的事务
     * @param insert 包含插入操作所需信息的对象
     * @return 返回数据插入的结果
     * @throws Exception 如果数据插入过程中发生任何异常，抛出一个异常
     */
    byte[] insert(long xid, Insert insert) throws Exception;

    /**
     * 从数据库中读取数据
     *
     * @param xid 事务的唯一标识符，用于标识特定的事务
     * @param select 包含查询条件的对象
     * @return 返回查询到的数据结果
     * @throws Exception 如果数据读取过程中发生任何异常，抛出一个异常
     */
    byte[] read(long xid, Select select) throws Exception;

    /**
     * 更新数据库中的数据
     *
     * @param xid 事务的唯一标识符，用于标识特定的事务
     * @param update 包含更新操作所需信息的对象
     * @return 返回数据更新的结果
     * @throws Exception 如果数据更新过程中发生任何异常，抛出一个异常
     */
    byte[] update(long xid, Update update) throws Exception;

    /**
     * 从数据库中删除数据
     *
     * @param xid 事务的唯一标识符，用于标识特定的事务
     * @param delete 包含删除操作所需信息的对象
     * @return 返回数据删除的结果
     * @throws Exception 如果数据删除过程中发生任何异常，抛出一个异常
     */
    byte[] delete(long xid, Delete delete) throws Exception;


    public static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    public static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }
}
