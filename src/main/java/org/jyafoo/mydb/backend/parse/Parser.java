package org.jyafoo.mydb.backend.parse;

import java.util.ArrayList;
import java.util.List;

import org.jyafoo.mydb.backend.parse.statement.*;
import org.jyafoo.mydb.common.Error;

/**
 * SQL语句的结构化解析
 * <p>
 * Parser 类则直接对外提供了 Parse(byte[] statement) 方法，
 * 核心就是一个调用 Tokenizer 类分割 Token，并根据词法规则包装成具体的 Statement 类并返回。
 *
 * @author jyafoo
 * @since 2024/10/5
 */
public class Parser {

    /**
     * 解析给定的字节码指令并返回相应的对象
     * <p>
     * 该方法支持多种数据库操作指令的解析，包括begin、commit、abort、create、drop、select、insert、delete、update和show
     * 如果指令无效或解析过程中发生错误，将抛出异常
     *
     * @param statement 包含数据库操作指令的字节数组
     * @return 解析后的对象，类型取决于具体的数据库操作
     * @throws Exception 如果解析失败或指令无效，将抛出异常
     */
    public static Object Parse(byte[] statement) throws Exception {
        // 创建tokenizer来分割字节码指令
        Tokenizer tokenizer = new Tokenizer(statement);
        // 获取下一个令牌，但不立即移除
        String token = tokenizer.peek();
        tokenizer.pop();

        // 用于存储解析后的对象
        Object stat = null;
        // 用于存储解析过程中可能发生的异常
        Exception statErr = null;
        try {
            // 根据首令牌判断指令类型，并调用相应的解析方法
            switch (token) {
                case "begin":
                    stat = parseBegin(tokenizer);
                    break;
                case "commit":
                    stat = parseCommit(tokenizer);
                    break;
                case "abort":
                    stat = parseAbort(tokenizer);
                    break;
                case "create":
                    stat = parseCreate(tokenizer);
                    break;
                case "drop":
                    stat = parseDrop(tokenizer);
                    break;
                case "select":
                    stat = parseSelect(tokenizer);
                    break;
                case "insert":
                    stat = parseInsert(tokenizer);
                    break;
                case "delete":
                    stat = parseDelete(tokenizer);
                    break;
                case "update":
                    stat = parseUpdate(tokenizer);
                    break;
                case "show":
                    stat = parseShow(tokenizer);
                    break;
                default:
                    // 如果指令不匹配任何已知类型，抛出异常
                    throw Error.InvalidCommandException;
            }
        } catch (Exception e) {
            // 捕获解析过程中的异常
            statErr = e;
        }
        try {
            // 检查是否还有剩余的令牌，如果有，可能是指令格式错误
            String next = tokenizer.peek();
            if (!"".equals(next)) {
                // 获取错误的指令内容
                byte[] errStat = tokenizer.errStat();
                // 抛出包含错误信息的异常
                statErr = new RuntimeException("Invalid statement: " + new String(errStat));
            }
        } catch (Exception e) {
            // 捕获检查过程中产生的异常
            e.printStackTrace();
            byte[] errStat = tokenizer.errStat();
            statErr = new RuntimeException("Invalid statement: " + new String(errStat));
        }
        // 如果有错误发生，抛出异常
        if (statErr != null) {
            throw statErr;
        }
        // 返回解析后的对象
        return stat;
    }


    /**
     * 解析show命令
     * <p>
     * 该方法用于解析一个显示命令，如果输入为空，则返回一个新的Show对象；如果输入不为空，则抛出InvalidCommandException异常
     *
     * @param tokenizer 用于分割命令字符串的分隔符
     * @return 返回一个新的Show对象，如果输入为空
     * @throws Exception 如果输入不为空，抛出异常
     */
    private static Show parseShow(Tokenizer tokenizer) throws Exception {
        // 预览下一个token
        String tmp = tokenizer.peek();
        // 如果预览的token为空字符串，表示命令输入结束
        if ("".equals(tmp)) {
            // 返回一个新的Show对象
            return new Show();
        }
        // 如果命令不为空，抛出异常，表示命令无效
        throw Error.InvalidCommandException;
    }


    /**
     * 解析更新语句
     * <p>
     * 此方法通过一个词法分析器来解析更新语句，生成一个Update对象
     * 它会检查并处理"set"关键字、字段名、等于号、更新值以及可选的where条件
     *
     * @param tokenizer 词法分析器，用于解析更新语句的各个部分
     * @return Update 解析后的Update对象，包含更新操作所需的信息
     * @throws Exception 如果更新语句的格式不正确，抛出异常
     */
    private static Update parseUpdate(Tokenizer tokenizer) throws Exception {
        // 创建一个新的Update对象来存储解析后的更新信息
        Update update = new Update();

        // 期望下一个词法单元是表名，并将其赋给update的tableName属性
        update.tableName = tokenizer.peek();
        tokenizer.pop();

        // 检查下一个词法单元是否是"set"关键字，如果不是，抛出无效命令异常
        if (!"set".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        // 期望下一个词法单元是字段名，并将其赋给update的fieldName属性
        update.fieldName = tokenizer.peek();
        tokenizer.pop();

        // 检查下一个词法单元是否是等于号，如果不是，抛出无效命令异常
        if (!"=".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        // 期望下一个词法单元是更新值，并将其赋给update的value属性
        update.value = tokenizer.peek();
        tokenizer.pop();

        // 检查是否有where子句，如果下一个词法单元是空字符串，则没有where子句
        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            update.where = null;
            return update;
        }

        // 解析where子句，并将其结果赋给update的where属性
        update.where = parseWhere(tokenizer);
        return update;
    }

    /**
     * 解析删除命令
     * <p>
     * 本方法通过 tokenizer 解析一个 DELETE 命令，返回一个 Delete 对象，该对象包含了删除操作所需的信息
     * 如果命令语法不符合预期，可能会抛出 Exception
     *
     * @param tokenizer 用于解析命令的 tokenizer 对象
     * @return 返回一个填充了删除操作相关信息的 Delete 对象
     * @throws Exception 如果命令语法不正确，则抛出异常
     */
    private static Delete parseDelete(Tokenizer tokenizer) throws Exception {
        // 创建一个 Delete 对象来存储解析后的删除操作信息
        Delete delete = new Delete();

        // 检查下一个令牌是否为 "from"，这是 DELETE 命令中必需的
        if (!"from".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        // 从 tokenizer 中移除已检查的 "from"
        tokenizer.pop();

        // 获取要删除的表名
        String tableName = tokenizer.peek();
        // 检查表名是否有效
        if (!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        // 将表名存储到 Delete 对象中
        delete.tableName = tableName;
        // 从 tokenizer 中移除已检查的表名
        tokenizer.pop();

        // 解析并获取删除操作的 WHERE 条件
        delete.where = parseWhere(tokenizer);
        // 返回填充了删除操作信息的 Delete 对象
        return delete;
    }


    /**
     * 解析INSERT命令
     * <p>
     * 该方法负责解析由tokenizer提供的INSERT命令，并将其转换为Insert对象
     * 它会检查命令的结构，确保包含"into"和"values"关键字，并提取表名和值列表
     * 如果命令结构不正确或包含无效的表名，则抛出异常
     *
     * @param tokenizer 用于解析INSERT命令的Tokenizer对象
     * @return 返回解析后的Insert对象
     * @throws Exception 如果命令无效或结构不正确，则抛出异常
     */
    private static Insert parseInsert(Tokenizer tokenizer) throws Exception {
        // 创建一个新的Insert对象
        Insert insert = new Insert();

        // 检查下一个令牌是否为"into"，如果不是，则抛出异常
        if (!"into".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        // 移除已检查的"into"令牌
        tokenizer.pop();

        // 获取表名
        String tableName = tokenizer.peek();
        // 检查表名是否有效，如果无效，则抛出异常
        if (!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        // 将表名赋值给Insert对象，并移除已检查的表名令牌
        insert.tableName = tableName;
        tokenizer.pop();

        // 检查下一个令牌是否为"values"，如果不是，则抛出异常
        if (!"values".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        // 创建一个列表来存储值
        List<String> values = new ArrayList<>();
        // 循环读取值，直到遇到空字符串
        while (true) {
            tokenizer.pop();
            String value = tokenizer.peek();
            // 如果遇到空字符串，则结束循环
            if ("".equals(value)) {
                break;
            } else {
                // 将值添加到列表中
                values.add(value);
            }
        }
        // 将值列表转换为数组，并赋值给Insert对象
        insert.values = values.toArray(new String[values.size()]);

        // 返回解析后的Insert对象
        return insert;
    }


    /**
     * 解析SQL查询语句的SELECT部分
     * <p>
     * 该方法通过tokenizer解析出查询的字段、表名和条件，并将其封装为Select对象
     *
     * @param tokenizer 用于解析SQL语句的Tokenizer对象
     * @return 返回封装了查询字段、表名和条件的Select对象
     * @throws Exception 当SQL语句格式不符合预期时抛出异常
     */
    private static Select parseSelect(Tokenizer tokenizer) throws Exception {
        // 创建Select对象来存储解析后的查询信息
        Select read = new Select();

        // 解析查询字段
        List<String> fields = new ArrayList<>();
        String asterisk = tokenizer.peek();
        if ("*".equals(asterisk)) {
            // 如果下一个词元是"*"，表示选择所有字段
            fields.add(asterisk);
            tokenizer.pop();
        } else {
            // 否则，解析具体的字段列表
            while (true) {
                String field = tokenizer.peek();
                // 确保下一个词元是有效的字段名
                if (!isName(field)) {
                    throw Error.InvalidCommandException;
                }
                fields.add(field);
                tokenizer.pop();
                // 检查是否还有更多的字段
                if (",".equals(tokenizer.peek())) {
                    tokenizer.pop();
                } else {
                    break;
                }
            }
        }
        // 将解析后的字段列表存储到Select对象中
        read.fields = fields.toArray(new String[fields.size()]);

        // 确认下一个词元是"from"
        if (!"from".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        // 解析表名
        String tableName = tokenizer.peek();
        // 确保下一个词元是有效的表名
        if (!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        read.tableName = tableName;
        tokenizer.pop();

        // 检查是否还有查询条件
        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            // 如果没有查询条件，返回Select对象
            read.where = null;
            return read;
        }

        // 解析查询条件
        read.where = parseWhere(tokenizer);
        return read;
    }


    /**
     * 解析WHERE子句
     * <p>
     * 该方法根据tokenizer中的令牌构建一个Where对象，包括两个单表达式和一个逻辑操作符
     *
     * @param tokenizer 用于解析SQL命令的tokenizer对象
     * @return 返回构建的Where对象
     * @throws Exception 如果SQL命令格式无效，则抛出异常
     */
    private static Where parseWhere(Tokenizer tokenizer) throws Exception {
        Where where = new Where();

        // 确保下一个令牌是'where'，以验证命令的正确性
        if (!"where".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        // 解析第一个单表达式
        SingleExpression exp1 = parseSingleExp(tokenizer);
        where.singleExp1 = exp1;

        // 获取下一个令牌，用于判断是否还有第二个单表达式以及逻辑操作符
        String logicOp = tokenizer.peek();
        // 如果下一个令牌为空，则表示只有一个单表达式，直接返回
        if ("".equals(logicOp)) {
            where.logicOp = logicOp;
            return where;
        }
        // 确保下一个令牌是合法的逻辑操作符
        if (!isLogicOp(logicOp)) {
            throw Error.InvalidCommandException;
        }
        where.logicOp = logicOp;
        tokenizer.pop();

        // 解析第二个单表达式
        SingleExpression exp2 = parseSingleExp(tokenizer);
        where.singleExp2 = exp2;

        // 确保tokenizer中没有剩余的令牌，以验证命令的完整性
        if (!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return where;
    }


    /**
     * 解析单一表达式
     * <p>
     * 该方法通过 tokenizer 解析一个单一表达式，包括字段名、比较运算符和值
     *
     * @param tokenizer 用于解析表达式各部分的 tokenizer 对象
     * @return 返回解析后的 SingleExpression 对象
     * @throws Exception 如果字段名或比较运算符不合法，则抛出异常
     */
    private static SingleExpression parseSingleExp(Tokenizer tokenizer) throws Exception {
        // 创建一个新的 SingleExpression 对象
        SingleExpression exp = new SingleExpression();

        // 获取下一个字段名
        String field = tokenizer.peek();
        // 如果字段名不合法，抛出异常
        if (!isName(field)) {
            throw Error.InvalidCommandException;
        }
        // 将字段名赋值给表达式的 field 属性，并从 tokenizer 中移除该字段名
        exp.field = field;
        tokenizer.pop();

        // 获取下一个比较运算符
        String op = tokenizer.peek();
        // 如果比较运算符不合法，抛出异常
        if (!isCmpOp(op)) {
            throw Error.InvalidCommandException;
        }
        // 将比较运算符赋值给表达式的 compareOp 属性，并从 tokenizer 中移除该比较运算符
        exp.compareOp = op;
        tokenizer.pop();

        // 获取并移除下一个值，赋值给表达式的 value 属性
        exp.value = tokenizer.peek();
        tokenizer.pop();
        return exp;
    }


    /**
     * 判断给定的操作符是否为比较操作符
     * <p>
     * 该方法用于确定传入的操作符是否属于赋值（=）、大于（>）或小于（<）操作符
     * 这些操作符在编程语言中常用于比较操作或赋值操作
     *
     * @param op 待检查的操作符
     * @return 如果操作符是"="、">"或"<"之一，则返回true；否则返回false
     */
    private static boolean isCmpOp(String op) {
        return ("=".equals(op) || ">".equals(op) || "<".equals(op));
    }


    /**
     * 判断给定的字符串是否为逻辑运算符
     * <p>
     * 此方法用于确定传入的字符串参数是否为"and"或"or"，这是两个常见的逻辑运算符
     * 在解析或处理逻辑表达式时，此方法可以帮助识别出逻辑操作，从而正确地解析和执行逻辑表达式
     *
     * @param op 待检查的字符串，预期为逻辑运算符"and"或"or"
     * @return 如果字符串为"and"或"or"，则返回true；否则返回false
     */
    private static boolean isLogicOp(String op) {
        return ("and".equals(op) || "or".equals(op));
    }

    /**
     * 解析 DROP 语句
     * <p>
     * 此方法通过 tokenizer 解析输入命令，以创建一个 Drop 对象
     * 它检查命令是否以 "table" 开始，后跟表名，然后是可选的空字符串
     *
     * @param tokenizer 用于解析命令的 tokenizer 对象
     * @return 返回包含解析后的表名的 Drop 对象
     * @throws Exception 如果命令格式不正确，抛出异常
     */
    private static Drop parseDrop(Tokenizer tokenizer) throws Exception {
        // 检查命令是否以 "table" 开始
        if (!"table".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        // 获取并检查表名的有效性
        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        // 确保命令以空字符串结束
        if (!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        // 创建 Drop 对象并设置表名
        Drop drop = new Drop();
        drop.tableName = tableName;
        return drop;
    }


    /**
     * 解析"create"类型的SQL命令
     * <p>
     * 该方法主要负责解析创建表的SQL语句，提取表名、字段名、字段类型以及索引信息
     *
     * @param tokenizer 一个Tokenizer对象，用于逐个访问SQL命令中的单词
     * @return 返回一个Create对象，包含解析后的表名、字段名、字段类型以及索引信息
     * @throws Exception 如果SQL命令格式不符合预期，则抛出异常
     */
    private static Create parseCreate(Tokenizer tokenizer) throws Exception {
        // 检查下一个单词是否为"table"，如果不是，则抛出异常
        if (!"table".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        Create create = new Create();
        String name = tokenizer.peek();
        // 检查表名是否有效，如果无效，则抛出异常
        if (!isName(name)) {
            throw Error.InvalidCommandException;
        }
        create.tableName = name;

        List<String> fNames = new ArrayList<>();
        List<String> fTypes = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            // 如果遇到"("，则停止收集字段信息
            if ("(".equals(field)) {
                break;
            }

            // 检查字段名是否有效，如果无效，则抛出异常
            if (!isName(field)) {
                throw Error.InvalidCommandException;
            }

            tokenizer.pop();
            String fieldType = tokenizer.peek();
            // 检查字段类型是否有效，如果无效，则抛出异常
            if (!isType(fieldType)) {
                throw Error.InvalidCommandException;
            }
            fNames.add(field);
            fTypes.add(fieldType);
            tokenizer.pop();

            String next = tokenizer.peek();
            // 如果遇到","，则继续下一轮循环
            if (",".equals(next)) {
                continue;
            } else if ("".equals(next)) {
                // 如果遇到空字符串，则抛出异常
                throw Error.TableNoIndexException;
            } else if ("(".equals(next)) {
                // 如果遇到"("，则停止收集字段信息
                break;
            } else {
                // 如果遇到其他非法字符，则抛出异常
                throw Error.InvalidCommandException;
            }
        }
        create.fieldName = fNames.toArray(new String[fNames.size()]);
        create.fieldType = fTypes.toArray(new String[fTypes.size()]);

        tokenizer.pop();
        // 检查下一个单词是否为"index"，如果不是，则抛出异常
        if (!"index".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        List<String> indexes = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            // 如果遇到")"，则停止收集索引字段信息
            if (")".equals(field)) {
                break;
            }
            // 检查索引字段名是否有效，如果无效，则抛出异常
            if (!isName(field)) {
                throw Error.InvalidCommandException;
            } else {
                indexes.add(field);
            }
        }
        create.index = indexes.toArray(new String[indexes.size()]);
        tokenizer.pop();

        // 检查是否已解析完所有SQL命令，如果还有剩余单词，则抛出异常
        if (!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return create;
    }


    /**
     * 判断给定的类型字符串是否为预定义的有效类型
     *
     * @param tp 待检查的类型字符串
     * @return 如果类型字符串是"int32"、"int64"或"string"之一，则返回true；否则返回false
     */
    private static boolean isType(String tp) {
        return ("int32".equals(tp) || "int64".equals(tp) ||
                "string".equals(tp));
    }


    /**
     * 解析中断命令
     * <p>
     * 此方法旨在处理"abort"命令的解析，确保当前令牌器中没有未消费的文本
     * 如果令牌器中剩余任何内容，则抛出无效命令异常，表示命令格式错误
     *
     * @param tokenizer 用于命令解析的令牌器
     * @return 返回一个新建的Abort实例，表示中断操作
     * @throws Exception 如果令牌器中仍有剩余内容，则抛出异常，表明命令无效
     */
    private static Abort parseAbort(Tokenizer tokenizer) throws Exception {
        // 检查令牌器中是否还有剩余的内容未被解析
        if (!"".equals(tokenizer.peek())) {
            // 如果存在剩余内容，抛出无效命令异常
            throw Error.InvalidCommandException;
        }
        // 返回一个新建的Abort实例，用于表示中断操作
        return new Abort();
    }

    /**
     * 解析提交信息
     * <p>
     * 该方法用于从 tokenizer 中解析出提交信息它通过检查 tokenizer 中是否还有剩余内容来判断是否可以进行提交解析
     * 如果tokenizer中没有剩余内容，则抛出InvalidCommandException异常，表示无法解析提交
     * 解析成功后，返回一个新的Commit对象
     *
     * @param tokenizer Tokenizer对象，用于解析提交信息
     * @return 返回一个新的Commit对象，表示解析出的提交信息
     * @throws Exception 如果tokenizer中没有剩余内容，则抛出异常，表示无法解析提交
     */
    private static Commit parseCommit(Tokenizer tokenizer) throws Exception {
        // 检查tokenizer中是否有剩余内容，如果没有则抛出异常，表示无法解析提交
        if (!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        // 解析成功，返回一个新的Commit对象，表示解析出的提交信息
        return new Commit();
    }


    /**
     * 解析并创建Begin对象
     * 该方法根据tokenizer中的令牌信息，构建一个表示事务开始的Begin对象
     * 支持解析两种隔离级别：READ COMMITTED 和 REPEATABLE READ
     *
     * @param tokenizer 用于解析命令的Tokenizer对象
     * @return 返回解析后的Begin对象
     * @throws Exception 如果命令格式无效，抛出异常
     */
    private static Begin parseBegin(Tokenizer tokenizer) throws Exception {
        // 查看下一个令牌，判断是否为空或指定的隔离级别关键字
        String isolation = tokenizer.peek();
        Begin begin = new Begin();
        if ("".equals(isolation)) {
            return begin;
        }
        if (!"isolation".equals(isolation)) {
            throw Error.InvalidCommandException;
        }

        // 消耗当前令牌，查看下一个令牌
        tokenizer.pop();
        String level = tokenizer.peek();
        if (!"level".equals(level)) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        // 根据令牌内容，确定隔离级别
        String tmp1 = tokenizer.peek();
        if ("read".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if ("committed".equals(tmp2)) {
                tokenizer.pop();
                if (!"".equals(tokenizer.peek())) {
                    throw Error.InvalidCommandException;
                }
                return begin;
            } else {
                throw Error.InvalidCommandException;
            }
        } else if ("repeatable".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if ("read".equals(tmp2)) {
                begin.isRepeatableRead = true;
                tokenizer.pop();
                if (!"".equals(tokenizer.peek())) {
                    throw Error.InvalidCommandException;
                }
                return begin;
            } else {
                throw Error.InvalidCommandException;
            }
        } else {
            throw Error.InvalidCommandException;
        }
    }


    /**
     * 判断给定的字符串是否为有效名称
     * <p>
     * 有效名称定义为：非空，且长度大于1的字符串，或者单个字母字符
     * 此方法主要通过检查字符串长度和首字符是否为字母来判断
     *
     * @param name 待判断的字符串
     * @return 如果字符串是有效名称，则返回true；否则返回false
     */
    private static boolean isName(String name) {
        // 判断条件：字符串长度为1且首个字符不是字母，则返回false
        return !(name.length() == 1 && !Tokenizer.isAlphaBeta(name.getBytes()[0]));
    }

}