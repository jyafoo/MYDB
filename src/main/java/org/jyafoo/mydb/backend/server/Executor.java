package org.jyafoo.mydb.backend.server;

import org.jyafoo.mydb.backend.parse.Parser;
import org.jyafoo.mydb.backend.parse.statement.*;
import org.jyafoo.mydb.backend.tbm.BeginRes;
import org.jyafoo.mydb.backend.tbm.TableManager;
import org.jyafoo.mydb.common.Error;

/**
 * 执行器类，用于处理数据库事务和查询
 *
 * @author jyafoo
 * @since 2024/10/8
 */
public class Executor {
    // 事务ID
    private long xid;
    // 表管理器实例
    TableManager tbm;

    /**
     * 构造函数
     *
     * @param tbm 表管理器实例
     */
    public Executor(TableManager tbm) {
        this.tbm = tbm;
        this.xid = 0;
    }

    /**
     * 关闭执行器时，检查并处理挂起的事务
     */
    public void close() {
        if (xid != 0) {
            System.out.println("Abnormal Abort: " + xid);
            tbm.abort(xid);
        }
    }

    /**
     * 执行SQL命令
     *
     * @param sql SQL命令的字节数组表示
     * @return 执行结果的字节数组
     * @throws Exception 如果执行过程中发生错误
     */
    public byte[] execute(byte[] sql) throws Exception {
        // 打印执行的SQL命令，以便于调试和日志记录
        System.out.println("Execute: " + new String(sql));

        // 解析SQL命令，并根据解析结果确定操作类型
        Object stat = Parser.Parse(sql);

        // 根据解析结果的类型，执行相应的数据库操作
        if (Begin.class.isInstance(stat)) {
            // 检查是否存在嵌套事务，如果存在则抛出异常
            if (xid != 0) {
                throw Error.NestedTransactionException;
            }
            // 执行开始事务操作，并返回结果
            BeginRes r = tbm.begin((Begin) stat);
            xid = r.xid;
            return r.result;
        } else if (Commit.class.isInstance(stat)) {
            // 检查是否存在未提交的事务，如果不存在则抛出异常
            if (xid == 0) {
                throw Error.NoTransactionException;
            }
            // 执行提交事务操作，并重置事务标识符，然后返回结果
            byte[] res = tbm.commit(xid);
            xid = 0;
            return res;
        } else if (Abort.class.isInstance(stat)) {
            // 检查是否存在未提交的事务，如果不存在则抛出异常
            if (xid == 0) {
                throw Error.NoTransactionException;
            }
            // 执行终止事务操作，并重置事务标识符，然后返回结果
            byte[] res = tbm.abort(xid);
            xid = 0;
            return res;
        } else {
            // 对于其他类型的SQL命令，调用execute2方法进行执行
            return execute2(stat);
        }
    }


    /**
     * 执行除事务控制命令以外的SQL语句
     *
     * @param stat 解析后的SQL语句对象
     * @return 执行结果的字节数组
     * @throws Exception 如果执行过程中发生错误
     */
    private byte[] execute2(Object stat) throws Exception {
        boolean tmpTransaction = false;
        Exception e = null;
        if (xid == 0) {
            tmpTransaction = true;
            BeginRes r = tbm.begin(new Begin());
            xid = r.xid;
        }
        try {
            byte[] res = null;
            if (Show.class.isInstance(stat)) {
                res = tbm.show(xid);
            } else if (Create.class.isInstance(stat)) {
                res = tbm.create(xid, (Create) stat);
            } else if (Select.class.isInstance(stat)) {
                res = tbm.read(xid, (Select) stat);
            } else if (Insert.class.isInstance(stat)) {
                res = tbm.insert(xid, (Insert) stat);
            } else if (Delete.class.isInstance(stat)) {
                res = tbm.delete(xid, (Delete) stat);
            } else if (Update.class.isInstance(stat)) {
                res = tbm.update(xid, (Update) stat);
            }
            return res;
        } catch (Exception e1) {
            e = e1;
            throw e;
        } finally {
            if (tmpTransaction) {
                if (e != null) {
                    tbm.abort(xid);
                } else {
                    tbm.commit(xid);
                }
                xid = 0;
            }
        }
    }
}


