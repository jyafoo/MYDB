package org.jyafoo.mydb.backend.dm.page;

import org.jyafoo.mydb.backend.dm.pageCache.PageCache;
import org.jyafoo.mydb.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * 数据库文件的第一页，通常用作一些特殊用途，比如存储一些元数据，用来启动检查等
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 用于判断上一次数据库是否正常关闭
 *
 * @author jyafoo
 * @since 2024/9/30
 */
public class PageOne {
    /**
     * ValidCheck 偏移量：第100字节
     */
    private static final int OF_VC = 100;

    /**
     * ValidCheck 长度：8字节
     */
    private static final int LEN_VC = 8;

    /**
     * 初始化原始字节数组，用于模拟页面缓存中的页面初始化过程.
     *
     * @return 初始化后的字节数组，其大小为页面大小.
     */
    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    /**
     * 对页对象设置 ValidCheck 开启标志
     *
     * @param page 页面
     */
    public static void setVcOpen(Page page) {
        page.setDirty(true);
        setVcOpen(page.getData());
    }

    /**
     * 对页数据设置 ValidCheck 开启标志
     *
     * @param raw 原始字节数组
     */
    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    /**
     * 对页对象设置 ValidCheck 关闭标志
     *
     * @param page 页面
     */
    public static void setVcClose(Page page) {
        page.setDirty(true);
        setVcClose(page.getData());
    }

    /**
     * 对页数据设置 ValidCheck 关闭标志
     *
     * @param raw 原始字节数组
     */
    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC + LEN_VC, LEN_VC);
    }

    /**
     * 校验首页两处的字节是否相同，以此来判断上一次是否正常关闭。
     *
     * @param page 首页
     * @return true：正常；false：异常
     */
    public static boolean checkVc(Page page) {
        return checkVc(page.getData());
    }

    /**
     * 校验首页两处的字节是否相同，以此来判断上一次是否正常关闭。
     *
     * @param raw 首页数据
     * @return true：正常；false：异常
     */
    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(
                Arrays.copyOfRange(raw, OF_VC, OF_VC + LEN_VC),
                Arrays.copyOfRange(raw, OF_VC + LEN_VC, OF_VC + 2 * LEN_VC)
        );
    }

}
