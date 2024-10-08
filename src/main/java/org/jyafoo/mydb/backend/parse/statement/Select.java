package org.jyafoo.mydb.backend.parse.statement;

/**
 * DQL：查找
 *
 * @author jyafoo
 * @since 2024/10/6
 */
public class Select {
    public String tableName;
    public String[] fields;
    public Where where;
}
