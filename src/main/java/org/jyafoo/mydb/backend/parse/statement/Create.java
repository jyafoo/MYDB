package org.jyafoo.mydb.backend.parse.statement;

/**
 * DDL：创建
 *
 * @author jyafoo
 * @since 2024/10/6
 */
public class Create {
    public String tableName;
    public String[] fieldName;
    public String[] fieldType;
    public String[] index;
}
