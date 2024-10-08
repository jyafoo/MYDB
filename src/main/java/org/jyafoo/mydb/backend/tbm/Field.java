package org.jyafoo.mydb.backend.tbm;


import com.google.common.primitives.Bytes;
import org.jyafoo.mydb.backend.im.BPlusTree;
import org.jyafoo.mydb.backend.parse.statement.SingleExpression;
import org.jyafoo.mydb.backend.tm.TransactionManagerImpl;
import org.jyafoo.mydb.backend.utils.Panic;
import org.jyafoo.mydb.backend.utils.ParseStringRes;
import org.jyafoo.mydb.backend.utils.Parser;
import org.jyafoo.mydb.common.Error;

import java.util.Arrays;
import java.util.List;

/**
 * 字段
 *
 * @author jyafoo
 * @since 2024/10/6
 */
public class Field {
    /// 资源唯一标识符
    long uid;

    /// 关联的表对象，表示该字段属于哪个表
    private Table tb;

    /// 字段名称
    String fieldName;

    /// 字段类型，只支持int32，int64，string三种类型
    String fieldType;

    /// 字段在表中的索引位置，用于快速定位字段
    private long index;

    /// B+树索引，用于字段的高效查找和排序
    private BPlusTree bt;

    /**
     * 加载指定字段信息
     * <p>
     * 该方法从给定的表中加载一个字段，通过UID读取数据，并解析为Field对象
     *
     * @param tb  表对象，用于定位字段所在的表
     * @param uid 字段的唯一标识符
     * @return 返回解析后的Field对象
     */
    public static Field loadField(Table tb, long uid) {
        // 初始化原始数据数组为null
        byte[] raw = null;
        try {
            // 通过虚拟机读取指定UID的数据
            raw = ((TableManagerImpl) tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            // 如果发生异常，抛出并引发恐慌
            Panic.panic(e);
        }
        // 确保读取到的原始数据不为null
        assert raw != null;
        // 解析并返回新的Field对象
        return new Field(uid, tb).parseSelf(raw);
    }


    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }

    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }

    /**
     * 解析给定的字节数组以初始化当前字段对象
     * <p>
     * 此方法通过解析字节数组来提取字段名称、字段类型和索引，并根据索引加载B+树（如果存在）
     *
     * @param raw 包含字段信息的原始字节数组
     * @return 返回解析并初始化后的Field对象
     */
    private Field parseSelf(byte[] raw) {
        // 初始化解析位置
        int position = 0;

        // 解析字段名称
        ParseStringRes res = Parser.parseString(raw);
        fieldName = res.str;
        position += res.next;

        // 解析字段类型
        res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
        fieldType = res.str;
        position += res.next;

        // 解析索引
        this.index = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));

        // 如果索引非零，则尝试加载B+树
        if (index != 0) {
            try {
                bt = BPlusTree.load(index, ((TableManagerImpl) tb.tbm).dm);
            } catch (Exception e) {
                // 如果加载失败，抛出恐慌
                Panic.panic(e);
            }
        }

        // 返回解析后的Field对象
        return this;
    }


    /**
     * 创建一个字段并将其持久化
     * <p>
     * 此方法负责在给定的表中创建一个新的字段，包括设置字段名、字段类型和是否索引
     * 如果字段需要索引，它将创建一个B+树索引并将其关联到字段
     *
     * @param tb        表对象，表示字段所属的表
     * @param xid       事务ID，用于持久化操作
     * @param fieldName 字段名，用于标识字段
     * @param fieldType 字段类型，用于定义字段的数据类型
     * @param indexed   是否索引，指示是否需要为字段创建索引
     * @return 返回创建的字段对象
     * @throws Exception 如果操作过程中发生错误，将抛出异常
     */
    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        // 检查字段类型是否有效
        typeCheck(fieldType);
        // 创建一个新字段对象，初始版本号设为0
        Field f = new Field(tb, fieldName, fieldType, 0);
        if (indexed) {
            // 创建B+树索引，并获取索引ID
            long index = BPlusTree.create(((TableManagerImpl) tb.tbm).dm);
            // 加载B+树索引
            BPlusTree bt = BPlusTree.load(index, ((TableManagerImpl) tb.tbm).dm);
            // 将索引ID和B+树对象关联到字段
            f.index = index;
            f.bt = bt;
        }
        // 持久化字段对象
        f.persistSelf(xid);
        return f;
    }

    /**
     * 在数据库中持久化当前字段对象
     * <p>
     * 该方法主要负责在给定的事务执行上下文中，将字段的名称、类型和索引信息组合并插入到数据库中
     *
     * @param xid 事务执行的上下文标识，用于数据库操作的事务控制
     * @throws Exception 如果数据库插入操作失败，或者解析操作出现问题，可能会抛出异常
     */
    private void persistSelf(long xid) throws Exception {
        // 将字段名称转换为字节数组，以便于数据库存储
        byte[] nameRaw = Parser.string2Byte(fieldName);
        // 将字段类型转换为字节数组，目的是相同的，为了能够有效地在数据库中存储
        byte[] typeRaw = Parser.string2Byte(fieldType);
        // 将字段索引（这里可能是字段在表中的位置标识）转换为字节数组
        byte[] indexRaw = Parser.long2Byte(index);
        // 将字段的相关信息组合起来，并通过虚拟机插入到数据库中，返回的uid可能是该插入操作的唯一标识
        this.uid = ((TableManagerImpl) tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }


    /**
     * 检查字段类型是否有效
     * <p>
     * 该方法用于验证给定的字段类型是否为预定义的有效类型列表中的一员
     * 如果字段类型无效，则抛出异常
     *
     * @param fieldType 字段类型，作为字符串传递，例如"int32", "int64", "string"
     * @throws Exception 如果字段类型不是"int32", "int64"或"string"之一，则抛出Error.InvalidFieldException异常
     */
    private static void typeCheck(String fieldType) throws Exception {
        // 检查字段类型是否为"int32", "int64"或"string"之一
        if (!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            // 如果字段类型无效，抛出异常
            throw Error.InvalidFieldException;
        }
    }


    /**
     * 检查索引状态是否被设置
     * <p>
     * 此方法用于判断对象的索引状态是否非零，即是否已经索引或初始化
     * 它在内部通过比较index变量与零来实现这一逻辑
     *
     * @return 如果索引状态被设置（index非零），则返回true；否则返回false
     */
    public boolean isIndexed() {
        return index != 0;
    }


    /**
     * 插入键和用户ID到二叉树中
     *
     * @param key 插入的键，用于查找和操作二叉树
     * @param uid 关联的用户ID，与键一起被插入
     * @throws Exception 当插入操作因任何原因失败时抛出异常
     */
    public void insert(Object key, long uid) throws Exception {
        // 将键转换为用户ID，以便在二叉树中进行操作
        long uKey = value2Uid(key);
        // 在二叉树中插入键和用户ID
        bt.insert(uKey, uid);
    }


    /**
     * 在指定的数值范围内搜索相关的元素
     * <p>
     * 此方法将会调用bt树的搜索范围方法，以找到在指定范围内所有的元素
     *
     * @param left  搜索范围的左边界
     * @param right 搜索范围的右边界
     * @return 包含在指定范围内所有元素的列表
     * @throws Exception 如果搜索操作过程中发生问题，将会抛出异常
     */
    public List<Long> search(long left, long right) throws Exception {
        return bt.searchRange(left, right);
    }


    /**
     * 将字符串转换为指定类型的价值
     * <p>
     * 此方法根据字段类型将字符串转换为相应的Java数据类型支持的值
     * 它处理三种类型的转换：int32，int64和string如果字段类型不匹配任何支持的类型，则返回null
     *
     * @param str 待转换的字符串
     * @return 转换后的值如果字段类型不受支持，则为null
     */
    public Object string2Value(String str) {
        switch (fieldType) {
            case "int32":
                // 将字符串解析为Integer，适用于32位整数
                return Integer.parseInt(str);
            case "int64":
                // 将字符串解析为Long，适用于64位整数
                return Long.parseLong(str);
            case "string":
                // 当字段类型为字符串时，直接返回原始字符串
                return str;
        }
        // 如果字段类型不匹配任何支持的类型，返回null
        return null;
    }


    /**
     * 根据键的类型将其转换为唯一的用户标识符（uid）
     *
     * @param key 要转换的键，可以是字符串、32位整数或64位整数类型
     * @return 返回转换后的uid
     */
    public long value2Uid(Object key) {
        long uid = 0;
        // 根据字段类型执行不同的转换逻辑
        switch (fieldType) {
            case "string":
                // 如果键是字符串类型，调用str2Uid方法将其转换为uid
                uid = Parser.str2Uid((String) key);
                break;
            case "int32":
                // 如果键是32位整数类型，直接将其转换为long类型并返回
                int uint = (int) key;
                return (long) uint;
            case "int64":
                // 如果键是64位整数类型，直接将其转换为uid
                uid = (long) key;
                break;
        }
        return uid;
    }


    /**
     * 将给定的值转换为原始字节数组
     *
     * 此方法根据字段类型，将不同类型的值转换为对应的字节数组
     * 支持的字段类型包括int32、int64和string
     *
     * @param v 待转换的值，可以是int、long或String类型
     * @return 转换后的字节数组如果输入值的类型不被支持，则返回null
     */
    public byte[] value2Raw(Object v) {
        byte[] raw = null;
        switch (fieldType) {
            case "int32":
                // 将整数转换为字节数组
                raw = Parser.int2Byte((int) v);
                break;
            case "int64":
                // 将长整数转换为字节数组
                raw = Parser.long2Byte((long) v);
                break;
            case "string":
                // 将字符串转换为字节数组
                raw = Parser.string2Byte((String) v);
                break;
        }
        return raw;
    }


    /**
     * 解析值的结果类，用于存储解析后的值及其相关的偏移量
     */
    class ParseValueRes {
        /// 解析后的值，可以是任意类型
        Object v;
        /// 值的偏移量，用于表示解析后当前位置的调整量
        int shift;
    }

    /**
     * 解析给定的原始字节数组并将其转换为相应的值类型
     *
     * 该方法根据字段类型解析原始数据，并返回一个包含解析值和位移的对象
     *
     * @param raw 原始字节数组，包含待解析的数据
     * @return ParseValueRes 返回一个包含解析后的值和位移量的对象
     */
    public ParseValueRes parserValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes(); // 创建一个ParseValueRes对象，用于存储解析结果
        switch (fieldType) { // 根据字段类型选择相应的解析方式
            case "int32":
                // 如果字段类型为int32，则解析前4个字节为整数，并设置位移量为4
                res.v = Parser.parseInt(Arrays.copyOf(raw, 4));
                res.shift = 4;
                break;
            case "int64":
                // 如果字段类型为int64，则解析前8个字节为长整数，并设置位移量为8
                res.v = Parser.parseLong(Arrays.copyOf(raw, 8));
                res.shift = 8;
                break;
            case "string":
                // 如果字段类型为字符串，则使用parseString方法解析，获取字符串值和下一个位置
                ParseStringRes r = Parser.parseString(raw);
                res.v = r.str;
                res.shift = r.next;
                break;
        }
        return res; // 返回解析结果对象
    }

    /**
     * 根据字段类型转换并返回字符串表示
     *
     * 此方法演示了如何根据不同的字段类型（如“int32”，“int64”，“string”），
     * 将给定的值转换为字符串形式它展示了Java中类型强制转换的用法，
     * 以及如何通过switch语句根据字段类型选择适当的转换方法
     *
     * @param v 要转换的值，其类型取决于字段类型
     * @return 转换后的字符串表示如果转换无法执行或字段类型不匹配，则返回null
     */
    public String printValue(Object v) {
        String str = null;
        switch (fieldType) {
            case "int32":
                // 将对象转换为int，然后转换为字符串
                str = String.valueOf((int) v);
                break;
            case "int64":
                // 将对象转换为long，然后转换为字符串
                str = String.valueOf((long) v);
                break;
            case "string":
                // 直接将对象作为字符串，无需转换
                str = (String) v;
                break;
        }
        return str;
    }


    @Override
    public String toString() {
        return new StringBuilder("(")
                .append(fieldName)
                .append(", ")
                .append(fieldType)
                .append(index != 0 ? ", Index" : ", NoIndex")
                .append(")")
                .toString();
    }

    /**
     * 计算单个表达式的值
     * 根据表达式中的比较运算符，计算并返回表达式对应的数据范围
     *
     * @param exp 单个表达式对象，包含比较运算符、数值等信息
     * @return FieldCalRes对象，包含计算出的数据范围
     * @throws Exception 当解析或计算过程中发生错误时抛出异常
     */
    public FieldCalRes calExp(SingleExpression exp) throws Exception {
        // 初始化用于存储计算结果的变量
        Object v = null;
        // 初始化FieldCalRes对象，用于返回计算结果
        FieldCalRes res = new FieldCalRes();
        // 根据表达式的比较运算符，执行相应的计算逻辑
        switch (exp.compareOp) {
            // 处理小于运算
            case "<":
                // 设置左边界为0
                res.left = 0;
                // 将字符串形式的值转换为对应的数据类型
                v = string2Value(exp.value);
                // 将转换后的值转换为唯一的标识符（UID），作为右边界
                res.right = value2Uid(v);
                // 如果右边界大于0，则将其减1，以确保左边界小于右边界
                if (res.right > 0) {
                    res.right--;
                }
                break;
            // 处理等于运算
            case "=":
                // 将字符串形式的值转换为对应的数据类型，并将其转换为UID，作为左边界和右边界
                v = string2Value(exp.value);
                res.left = value2Uid(v);
                res.right = res.left;
                break;
            // 处理大于运算
            case ">":
                // 设置右边界为Long的最大值
                res.right = Long.MAX_VALUE;
                // 将字符串形式的值转换为对应的数据类型，并将其转换为UID，加上1，作为左边界
                v = string2Value(exp.value);
                res.left = value2Uid(v) + 1;
                break;
        }
        // 返回计算出的数据范围
        return res;
    }

}
