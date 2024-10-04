package org.jyafoo.mydb.backend.dm.pageIndex;

import lombok.AllArgsConstructor;

/**
 * 内存页剩余空间信息
 *
 * @author jyafoo
 * @since 2024/10/2
 */
@AllArgsConstructor
public class PageInfo {
    /**
     * 页号
     */
    public int pgno;

    /**
     * 页面剩余空间
     */
    public int freeSpace;
}
