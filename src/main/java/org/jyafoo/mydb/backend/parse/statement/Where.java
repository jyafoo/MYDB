package org.jyafoo.mydb.backend.parse.statement;

/**
 * 条件
 * <p>
 * 格式：【表达式】【and | or】【表达式】
 *
 * @author jyafoo
 * @since 2024/10/6
 */
public class Where {
    public SingleExpression singleExp1;
    public String logicOp;
    public SingleExpression singleExp2;
}
