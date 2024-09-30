package org.jyafoo.mydb.backend.common;

import lombok.AllArgsConstructor;

/**
 * 共享内存数组
 * @author jyafoo
 * @since 2024/9/29
 */
@AllArgsConstructor
public class SubArray {
    /**
     * 存储原始数组的引用
     */
    public byte[] raw;

    /**
     * 标记子数组的起始位置
     */
    public int start;

    /**
     * 标记子数组的结束位置
     */
    public int end;

}
