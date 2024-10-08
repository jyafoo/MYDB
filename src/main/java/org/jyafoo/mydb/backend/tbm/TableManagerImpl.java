package org.jyafoo.mydb.backend.tbm;


import org.jyafoo.mydb.backend.dm.DataManager;
import org.jyafoo.mydb.backend.parse.statement.*;
import org.jyafoo.mydb.backend.utils.Parser;
import org.jyafoo.mydb.backend.vm.VersionManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jyafoo.mydb.common.Error;

/**
 * @author jyafoo
 * @since 2024/10/6
 */
public class TableManagerImpl implements TableManager {
    /// 版本管理器，用于处理数据的版本控制
    VersionManager vm;
    /// 数据管理器，用于处理数据的增删改查操作
    DataManager dm;
    /// 启动器对象，用于系统的启动和初始化
    private Booter booter;
    /// 表缓存，用于缓存表对象，提高表访问效率
    private Map<String, Table> tableCache;
    /// 事务ID与表缓存映射，用于缓存事务相关的表信息，提高事务处理效率
    private Map<Long, List<Table>> xidTableCache;
    /// 锁对象，用于同步访问共享资源，保证线程安全
    private Lock lock;

    TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        this.xidTableCache = new HashMap<>();
        lock = new ReentrantLock();
        loadTables();
    }

    /**
     * 加载表格数据到缓存
     * <p>
     * 此方法通过遍历从第一个表格UID开始的所有表格，并将它们加载到缓存中
     * 它确保常用表格在内存中快速可取，提高数据访问效率
     */
    private void loadTables() {
        // 获取第一个表格的UID
        long uid = firstTableUid();
        // 当还有表格UID未处理时，继续加载表格
        while (uid != 0) {
            // 根据UID加载表格对象
            Table tb = Table.loadTable(this, uid);
            // 更新下一个表格的UID
            uid = tb.nextUid;
            // 将加载的表格放入缓存中，使用表格名称作为键
            tableCache.put(tb.name, tb);
        }
    }


    /**
     * 获取第一个表的UID
     * <p>
     * 本方法通过加载存储在特定位置的原始字节数据，然后解析这些字节数据来获取第一个表的UID
     * 这对于初始化或引用存储在类似数据库的系统中的第一个表时尤为重要
     *
     * @return long 返回解析后的UID，该UID唯一标识第一个表
     */
    private long firstTableUid() {
        // 加载原始字节数据，这些数据包含了表的UID信息
        byte[] raw = booter.load();
        // 解析加载的原始字节数据，转换为长整型的UID
        return Parser.parseLong(raw);
    }


    /**
     * 更新第一张表的UID信息
     *
     * 本方法的主要作用是将给定的UID转换为字节序列，并将该字节序列更新到第一张表中
     * 这是为了保持数据的同步性和一致性，确保UID在系统中是唯一且准确的
     *
     * @param uid 需要更新到第一张表中的UID，作为一个长整型数值传入
     */
    private void updateFirstTableUid(long uid) {
        // 将长整型的UID转换为字节序列，以便进行存储或传输
        byte[] raw = Parser.long2Byte(uid);
        // 更新第一张表中的UID信息
        booter.update(raw);
    }


    @Override
    public BeginRes begin(Begin begin) {
        BeginRes res = new BeginRes();
        int level = begin.isRepeatableRead ? 1 : 0;
        res.xid = vm.begin(level);
        res.result = "begin".getBytes();
        return res;
    }

    @Override
    public byte[] commit(long xid) throws Exception {
        vm.commit(xid);
        return "commit".getBytes();
    }

    @Override
    public byte[] abort(long xid) {
        vm.abort(xid);
        return "abort".getBytes();
    }

    @Override
    public byte[] show(long xid) {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            for (Table tb : tableCache.values()) {
                sb.append(tb.toString()).append("\n");
            }
            List<Table> t = xidTableCache.get(xid);
            if (t == null) {
                return "\n".getBytes();
            }
            for (Table tb : t) {
                sb.append(tb.toString()).append("\n");
            }
            return sb.toString().getBytes();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] create(long xid, Create create) throws Exception {
        lock.lock();
        try {
            if (tableCache.containsKey(create.tableName)) {
                throw Error.DuplicatedTableException;
            }
            Table table = Table.createTable(this, firstTableUid(), xid, create);
            updateFirstTableUid(table.uid);
            tableCache.put(create.tableName, table);
            if (!xidTableCache.containsKey(xid)) {
                xidTableCache.put(xid, new ArrayList<>());
            }
            xidTableCache.get(xid).add(table);
            return ("create " + create.tableName).getBytes();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        lock.lock();
        Table table = tableCache.get(insert.tableName);
        lock.unlock();
        if (table == null) {
            throw Error.TableNotFoundException;
        }
        table.insert(xid, insert);
        return "insert".getBytes();
    }

    @Override
    public byte[] read(long xid, Select read) throws Exception {
        lock.lock();
        Table table = tableCache.get(read.tableName);
        lock.unlock();
        if (table == null) {
            throw Error.TableNotFoundException;
        }
        return table.read(xid, read).getBytes();
    }

    @Override
    public byte[] update(long xid, Update update) throws Exception {
        lock.lock();
        Table table = tableCache.get(update.tableName);
        lock.unlock();
        if (table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.update(xid, update);
        return ("update " + count).getBytes();
    }

    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        lock.lock();
        Table table = tableCache.get(delete.tableName);
        lock.unlock();
        if (table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.delete(xid, delete);
        return ("delete " + count).getBytes();
    }
}
