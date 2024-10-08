package org.jyafoo.mydb.backend.utils;

/**
 * @author jyafoo
 * @since 2024/9/28
 */
public class Types {

    /**
     * 将页号和偏移量转换为统一的UID
     * 该方法用于将两个部分（页号和偏移量）合并成一个唯一的标识符（UID）
     *
     * @param pgno   页号，表示数据所在的页数
     * @param offset 偏移量，表示在页中的相对位置
     * @return 返回组合后的唯一标识符（UID）
     */
    public static long addressToUid(int pgno, short offset) {
        long u0 = pgno;
        long u1 = offset;
        return u0 << 32 | u1;
    }
}
