package org.jyafoo.mydb.backend.tbm;

import com.google.common.primitives.Bytes;
import org.jyafoo.mydb.backend.parse.statement.*;
import org.jyafoo.mydb.backend.tm.TransactionManagerImpl;
import org.jyafoo.mydb.backend.utils.Panic;
import org.jyafoo.mydb.backend.utils.ParseStringRes;
import org.jyafoo.mydb.backend.utils.Parser;

import org.jyafoo.mydb.common.Error;

import java.util.*;

/**
 * Table 维护了表结构
 * <p>
 * 二进制结构如下：
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 *
 * @author jyafoo
 * @since 2024/10/7
 */
public class Table {
    /// 表管理器，用于管理表操作
    TableManager tbm;
    /// 表的唯一id
    long uid;
    /// 表名
    String name;
    /// 表的状态
    byte status;
    /// 用于存储下一个将要分配给新记录或新对象的唯一标识符
    long nextUid;
    /// 表中包含的字段
    List<Field> fields = new ArrayList<>();

    /**
     * 加载数据表
     * <p>
     * 该方法通过表管理器和唯一标识符加载一个数据表对象
     *
     * @param tbm 表管理器，用于管理数据表的操作
     * @param uid 数据表的唯一标识符
     * @return 返回加载的数据表对象
     */
    public static Table loadTable(TableManager tbm, long uid) {
        // 从表管理器中读取数据表的原始数据
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl) tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        // 使用原始数据创建一个表对象
        Table tb = new Table(tbm, uid);
        // 解析并返回表对象
        return tb.parseSelf(raw);
    }

    /**
     * 创建一个新的表对象，并将其持久化
     * <p>
     * 该方法包括初始化表对象、添加字段以及持久化表的过程
     *
     * @param tbm     表管理器，用于管理和操作表对象
     * @param nextUid 下一个UID，用于为新表分配唯一的标识符
     * @param xid     事务ID，标识当前操作的事务
     * @param create  包含表结构信息的对象，包括表名、字段名、字段类型和索引信息
     * @return 返回新创建并持久化的表对象
     * @throws Exception 如果表创建或持久化过程中发生错误，则抛出异常
     */
    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        // 初始化表对象，包括表管理器、表名和下一个UID
        Table tb = new Table(tbm, create.tableName, nextUid);

        // 遍历创建表所需的字段信息，为每个字段创建Field对象并添加到表中
        for (int i = 0; i < create.fieldName.length; i++) {
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            boolean indexed = false;

            // 检查当前字段是否需要建立索引
            for (int j = 0; j < create.index.length; j++) {
                if (fieldName.equals(create.index[j])) {
                    indexed = true;
                    break;
                }
            }

            // 创建字段对象并将其添加到表中
            tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed));
        }

        // 持久化表对象，并返回持久化后的表对象
        return tb.persistSelf(xid);
    }


    public Table(TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }

    public Table(TableManager tbm, String tableName, long nextUid) {
        this.tbm = tbm;
        this.name = tableName;
        this.nextUid = nextUid;
    }

    /**
     * 解析自身的Table对象
     * <p>
     * 本方法从原始字节数组中解析出Table对象的信息，包括表名、下一个UID和字段信息
     *
     * @param raw 原始字节数组，包含Table对象的所有信息
     * @return 返回解析后的Table对象
     */
    private Table parseSelf(byte[] raw) {
        // 初始化位置变量，用于跟踪字节数组中的当前位置
        int position = 0;

        // 解析表名，并更新位置变量
        ParseStringRes res = Parser.parseString(raw);
        name = res.str;
        position += res.next;

        // 解析下一个UID，并更新位置变量
        nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
        position += 8;

        // 循环解析字段信息，直到字节数组结束
        while (position < raw.length) {
            // 解析字段的UID，并更新位置变量
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
            position += 8;
            // 根据字段的UID加载Field对象，并添加到字段列表中
            fields.add(Field.loadField(this, uid));
        }

        // 返回解析后的Table对象
        return this;
    }


    /**
     * 持久化当前表对象
     * <p>
     * 该方法负责将当前表对象的状态持久化到存储介质中，包括表名、下一个UID和所有字段的UID
     *
     * @param xid 事务ID，用于在持久化过程中跟踪事务
     * @return 返回当前表对象，以便进行链式调用
     * @throws Exception 如果在持久化过程中发生错误，则抛出异常
     */
    private Table persistSelf(long xid) throws Exception {
        // 将表名转换为字节数组
        byte[] nameRaw = Parser.string2Byte(name);
        // 将下一个UID转换为字节数组
        byte[] nextRaw = Parser.long2Byte(nextUid);
        // 初始化字段RAW数组，用于存储所有字段的UID
        byte[] fieldRaw = new byte[0];
        // 遍历所有字段，将每个字段的UID转换为字节数组并拼接到fieldRaw中
        for (Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(field.uid));
        }
        // 插入表数据到存储介质，并获取生成的UID
        uid = ((TableManagerImpl) tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        // 返回当前表对象，以便进行链式调用
        return this;
    }


    /**
     * 执行删除操作
     *
     * @param xid    删除操作的事务ID
     * @param delete 删除操作的对象，包含删除条件
     * @return 删除的行数
     * @throws Exception 删除过程中可能抛出的异常
     */
    public int delete(long xid, Delete delete) throws Exception {
        // 解析删除条件，获取需要删除的记录的UID列表
        List<Long> uids = parseWhere(delete.where);

        int count = 0;
        // 遍历UID列表，对每个UID执行删除操作
        for (Long uid : uids) {
            // 如果删除成功，则增加计数器
            if (((TableManagerImpl) tbm).vm.delete(xid, uid)) {
                count++;
            }
        }
        // 返回删除的总行数
        return count;
    }

    /**
     * 根据更新条件更新数据
     *
     * @param xid    事务ID
     * @param update 更新对象，包含更新的字段名称、值和条件
     * @return 更新的数据行数
     * @throws Exception 当字段未找到或更新过程中出现错误时抛出异常
     */
    public int update(long xid, Update update) throws Exception {
        // 解析更新条件中的UIDs
        List<Long> uids = parseWhere(update.where);

        // 初始化字段变量，用于后续查找
        Field fd = null;

        // 遍历所有字段，查找需要更新的字段
        for (Field f : fields) {
            if (f.fieldName.equals(update.fieldName)) {
                fd = f;
                break;
            }
        }

        // 如果未找到指定字段，抛出异常
        if (fd == null) {
            throw Error.FieldNotFoundException;
        }

        // 将更新的字符串值转换为字段对应的值类型
        Object value = fd.string2Value(update.value);

        // 初始化更新计数器
        int count = 0;

        // 遍历所有符合条件的UID
        for (Long uid : uids) {
            // 读取原始数据
            byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid);
            // 如果数据不存在，跳过当前UID
            if (raw == null) {
                continue;
            }

            // 删除旧的数据条目
            ((TableManagerImpl) tbm).vm.delete(xid, uid);

            // 解析原始数据为Map对象
            Map<String, Object> entry = parseEntry(raw);

            // 更新字段值
            entry.put(fd.fieldName, value);

            // 将更新后的Map对象转换回字节数组
            raw = entry2Raw(entry);

            // 插入新的数据条目，并获取新的UUID
            long uuid = ((TableManagerImpl) tbm).vm.insert(xid, raw);

            // 增加更新计数器
            count++;

            // 遍历所有字段，如果字段被索引，则更新索引
            for (Field field : fields) {
                if (field.isIndexed()) {
                    field.insert(entry.get(field.fieldName), uuid);
                }
            }
        }

        // 返回更新的数据行数
        return count;
    }


    /**
     * 根据给定的事务ID和查询条件，读取并返回匹配的数据
     * <p>
     * 此方法首先解析查询条件以获取需要读取的数据项UID，然后从表管理器中读取对应的数据项，
     * 解析这些数据项并将它们格式化为字符串返回
     *
     * @param xid  事务ID，用于标识事务
     * @param read 查询对象，包含查询条件
     * @return 包含所有匹配数据项的字符串
     * @throws Exception 如果读取过程中发生错误，抛出异常
     */
    public String read(long xid, Select read) throws Exception {
        // 解析查询条件中的WHERE子句，获取需要读取的数据项UID列表
        List<Long> uids = parseWhere(read.where);

        // 使用StringBuilder来高效地构建最终的返回字符串
        StringBuilder sb = new StringBuilder();

        // 遍历UID列表，对每个UID进行数据读取和处理
        for (Long uid : uids) {
            // 从表管理器中读取指定UID的数据项
            byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid);

            // 如果读取的数据项为空，则跳过当前UID，继续处理下一个
            if (raw == null) {
                continue;
            }

            // 解析数据项，将其转换为Map结构，以便于后续处理
            Map<String, Object> entry = parseEntry(raw);

            // 将解析后的数据项格式化为字符串，并追加到StringBuilder中
            sb.append(printEntry(entry)).append("\n");
        }

        // 返回包含所有数据项的字符串
        return sb.toString();
    }

    /**
     * 根据提供的事务ID和插入对象来插入新记录
     *
     * @param xid    事务ID，用于标识事务
     * @param insert 插入对象，包含要插入的数据
     * @throws Exception 如果插入过程中发生错误，则抛出异常
     */
    public void insert(long xid, Insert insert) throws Exception {
        // 将插入对象中的字符串表示的数据转换为Entry对象，以便后续处理
        Map<String, Object> entry = string2Entry(insert.values);
        // 将Entry对象转换为字节数组，以便存储
        byte[] raw = entry2Raw(entry);
        // 通过表管理器的版本管理器插入记录，并返回唯一标识符
        long uid = ((TableManagerImpl) tbm).vm.insert(xid, raw);
        // 遍历所有字段，对于被索引的字段，进行索引插入操作
        for (Field field : fields) {
            if (field.isIndexed()) {
                field.insert(entry.get(field.fieldName), uid);
            }
        }
    }


    /**
     * 将字符串数组转换为Map条目
     * <p>
     * 此方法用于将一组字符串值映射到一个Map对象中，其中Map的键是字段名，值是字段的解析后的内容
     *
     * @param values 字符串数组，表示待转换的字段值
     * @return Map<String, Object>，包含字段名和对应解析后的字段值的Map对象
     * @throws Exception 当输入的字段值数量与预期的字段数量不匹配时，抛出异常
     */
    private Map<String, Object> string2Entry(String[] values) throws Exception {
        // 检查字符串数组的长度是否与预期的字段数量匹配，如果不匹配则抛出异常
        if (values.length != fields.size()) {
            throw Error.InvalidValuesException;
        }
        // 创建一个Map对象用于存储字段名和对应字段值
        Map<String, Object> entry = new HashMap<>();
        // 遍历所有字段，将每个字段的字符串值转换为对应的对象，并存入Map中
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            Object v = f.string2Value(values[i]);
            entry.put(f.fieldName, v);
        }
        // 返回包含所有字段名和对应字段值的Map对象
        return entry;
    }

    /**
     * 解析Where对象，根据条件筛选UID列表
     *
     * @param where 查询条件对象，如果为null则使用默认条件
     * @return 包含筛选后的UID的列表
     * @throws Exception 如果字段未找到或字段未索引，则抛出异常
     */
    private List<Long> parseWhere(Where where) throws Exception {
        // 初始化范围变量和单条件标志
        long l0 = 0, r0 = 0, l1 = 0, r1 = 0;
        boolean single = false;
        Field fd = null;

        // 处理where为null的情况，使用默认条件
        if (where == null) {
            // 查找第一个索引字段作为筛选条件
            for (Field field : fields) {
                if (field.isIndexed()) {
                    fd = field;
                    break;
                }
            }
            // 设置默认筛选范围
            l0 = 0;
            r0 = Long.MAX_VALUE;
            single = true;
        } else {
            // 根据where条件查找对应字段
            for (Field field : fields) {
                if (field.fieldName.equals(where.singleExp1.field)) {
                    if (!field.isIndexed()) {
                        throw Error.FieldNotIndexedException;
                    }
                    fd = field;
                    break;
                }
            }
            // 如果未找到对应字段，抛出异常
            if (fd == null) {
                throw Error.FieldNotFoundException;
            }
            // 计算where条件范围
            CalWhereRes res = calWhere(fd, where);
            l0 = res.l0;
            r0 = res.r0;
            l1 = res.l1;
            r1 = res.r1;
            single = res.single;
        }

        // 根据计算的范围获取UID列表
        List<Long> uids = fd.search(l0, r0);
        // 如果不是单条件查询，还需要获取第二范围的UID列表并合并
        if (!single) {
            List<Long> tmp = fd.search(l1, r1);
            uids.addAll(tmp);
        }
        return uids;
    }


    /**
     * 计算结果范围类
     * <p>
     * 用于表示一个计算操作的结果范围，包括两个闭区间和一个标志位
     * 该类主要在计算过程中用于表示和操作结果的范围
     */
    class CalWhereRes {
        /// 左边区间的左边界
        long l0;
        /// 左边区间的右边界
        long r0;
        /// 右边区间的左边界
        long l1;
        /// 右边区间的右边界
        long r1;
        /// 是否为单个区间标志位，true表示结果为单个区间，false表示结果为两个区间
        boolean single;
    }

    /**
     * 根据字段和条件计算查询范围
     * 此方法用于解析给定字段和条件对象，以确定查询的范围
     * 它根据不同的逻辑运算符处理单个条件或多个条件的计算
     *
     * @param fd    字段对象，用于计算查询条件
     * @param where 条件对象，包含查询的逻辑运算符和表达式
     * @return 返回一个CalWhereRes对象，包含计算后的查询范围信息
     * @throws Exception 如果逻辑运算符无效，则抛出异常
     */
    private CalWhereRes calWhere(Field fd, Where where) throws Exception {
        CalWhereRes res = new CalWhereRes();
        switch (where.logicOp) {
            case "":
                // 当逻辑运算符为空时，表示只有一个条件
                res.single = true;
                FieldCalRes r = fd.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                break;
            case "or":
                // 当逻辑运算符为"or"时，表示两个条件之间的选择关系
                res.single = false;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left;
                res.r1 = r.right;
                break;
            case "and":
                // 当逻辑运算符为"and"时，表示两个条件之间的与关系，计算它们的交集
                res.single = true;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left;
                res.r1 = r.right;
                // 计算两个条件范围的交集
                if (res.l1 > res.l0) {
                    res.l0 = res.l1;
                }
                if (res.r1 < res.r0) {
                    res.r0 = res.r1;
                }
                break;
            default:
                // 如果逻辑运算符既不是空，也不是"or"或"and"，则抛出异常
                throw Error.InvalidLogOpException;
        }
        return res;
    }


    /**
     * 根据指定的字段信息来格式化并打印条目
     * <p>
     * 该方法遍历所有字段，并使用每个字段的printValue方法格式化条目中的相应字段值
     * 最终返回一个包含所有字段名和对应值的字符串
     *
     * @param entry 包含字段名和值的Map对象，代表一个条目
     * @return 格式化后的条目字符串
     */
    private String printEntry(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            sb.append(field.printValue(entry.get(field.fieldName)));
            if (i == fields.size() - 1) {
                sb.append("]");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }


    /**
     * 解析给定的原始字节数组数据
     * <p>
     * 该方法根据fields数组中的字段定义，将原始数据分割并解析成一个Map对象
     * 每个字段的解析由其对应的parserValue方法完成，该方法返回解析后的值以及消耗的字节数
     *
     * @param raw 原始字节数组，包含待解析的数据
     * @return 返回一个Map对象，其中包含解析出的各个字段的名称和对应的解析值
     */
    private Map<String, Object> parseEntry(byte[] raw) {
        // 初始化位置指针，用于追踪当前解析的位置
        int pos = 0;
        // 创建一个Map对象，用于存储解析后的键值对
        Map<String, Object> entry = new HashMap<>();
        // 遍历fields数组中的每个字段定义，对原始数据进行解析
        for (Field field : fields) {
            // 调用当前字段的parserValue方法，解析从当前位置开始的一段原始数据
            Field.ParseValueRes r = field.parserValue(Arrays.copyOfRange(raw, pos, raw.length));
            // 将解析得到的字段名称和值添加到entry Map中
            entry.put(field.fieldName, r.v);
            // 更新位置指针，移动到下一个字段的起始位置
            pos += r.shift;
        }
        // 返回包含所有解析结果的Map对象
        return entry;
    }

    /**
     * 将数据库条目转换为字节数组
     * <p>
     * 该方法主要用于将数据库条目中的数据转换为字节数组，以便于存储或传输
     *
     * @param entry 数据库条目，包含多个字段和对应的值
     * @return 返回转换后的字节数组
     */
    private byte[] entry2Raw(Map<String, Object> entry) {
        // 初始化一个空的字节数组
        byte[] raw = new byte[0];
        // 遍历所有字段，将每个字段的值转换为字节数组并拼接起来
        for (Field field : fields) {
            // 将当前字段的值转换为字节数组，并与之前的字节数组拼接
            raw = Bytes.concat(raw, field.value2Raw(entry.get(field.fieldName)));
        }
        // 返回拼接后的字节数组
        return raw;
    }

    /**
     * 输出表信息
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append(name).append(": ");
        for (Field field : fields) {
            sb.append(field.toString());
            if (field == fields.get(fields.size() - 1)) {
                sb.append("}");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}

