package org.jyafoo.mydb.backend.parse.statement;

/**
 * 条件表达式
 *
 * @author jyafoo
 * @since 2024/10/6
 */
public class SingleExpression {
    /**
     * 字段名
     */
    public String field;

    /**
     * 比较操作符
     */
    public String compareOp;

    /**
     * 比较值
     */
    public String value;
}
