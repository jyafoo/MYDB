package org.jyafoo.mydb.backend.parse.statement;

/**
 * DML：更新
 *
 * @author jyafoo
 * @since 2024/10/6
 */
public class Update {
    public String tableName;
    public String fieldName;
    public String value;
    public Where where;
}
