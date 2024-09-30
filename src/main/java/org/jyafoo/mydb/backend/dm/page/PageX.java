package org.jyafoo.mydb.backend.dm.page;

import org.jyafoo.mydb.backend.dm.pageCache.PageCache;
import org.jyafoo.mydb.backend.utils.Parser;

import java.util.Arrays;

/**
 * 普通数据页
 * MYDB 对于普通数据页的管理比较简单。
 * 一个普通页面以一个 2 字节无符号数起始，表示这一页的空闲位置的偏移。剩下的部分都是实际存储的数据。
 * 所以对普通页的管理，基本都是围绕着对 FSO（Free Space Offset）进行的。
 * </p>
 * 普通页结构：[FreeSpaceOffset] [Data]。FreeSpaceOffset: 2字节 空闲位置开始偏移
 *
 * @author jyafoo
 * @since 2024/9/30
 */
public class PageX {

    /**
     * 空闲空间偏移量的起始位置，0字节处
     */
    private static final short OF_FREE = 0;

    /**
     * 数据区域的起始位置，从第2字节开始
     */
    private static final short OF_DATA = 2;

    /**
     * 最大空闲空间大小，为页面大小减去数据偏移量
     */
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    /**
     * 初始化原始字节数组，设置空闲位置的偏移
     *
     * @return 初始化后的字节数组
     */
    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OF_DATA);
        return raw;
    }


    /**
     * 将原始字节数组插入到页面中
     *
     * @param page 要插入数据的页面
     * @param raw  要插入的原始字节数组
     * @return 返回插入原始数据的偏移量
     */
    public static short insert(Page page, byte[] raw) {
        page.setDirty(true); // 插入操作后页面处于脏状态
        short offset = getFSO(page.getData());
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
        setFSO(page.getData(), (short) (offset + raw.length));
        return offset;
    }

    /**
     * 设置页的空闲位置的偏移FSO字段
     *
     * @param raw    原始数据数组
     * @param ofData 要设置的FSO字段值
     *               <p>
     *               此方法将给定的FSO字段值转换为字节数组，并将其复制到原始数据数组中的特定位置
     *               这是为了确保数据包的FSO字段被正确更新，以便进行进一步的处理或传输
     */
    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    /**
     * 获取页面的FSO字段
     *
     * @param page 内存页面
     * @return FSO字段
     */
    public static short getFSO(Page page) {
        return getFSO(page.getData());
    }

    /**
     * 获取数据区域的FSO字段
     *
     * @param raw 数据区
     * @return FSO字段
     */
    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    /**
     * 获取页面的空闲空间，单位字节
     *
     * @param page 内存页面
     * @return 页面的空闲空间
     */
    public static int getFreeSpace(Page page) {
        return PageCache.PAGE_SIZE - getFSO(page.getData());
    }

    // TODO (jyafoo,2024/9/30,16:36) 两个函数 recoverInsert() 和 recoverUpdate() 用于在数据库崩溃后重新打开时，恢复例程直接插入数据以及修改数据使用。不太理解这个场景的使用

    /**
     * 在数据库崩溃后重新打开时恢复例程直接插入数据
     *
     * @param page   要插入数据的页面对象
     * @param raw    要插入页面的数据
     * @param offset 插入数据的起始偏移量，指示了在页面数据数组中开始插入数据的位置
     */
    public static void recoverInsert(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);

        short rawFSO = getFSO(page);
        // TODO (jyafoo,2024/9/30,16:51) 问1：不理解为什么大于插入数据后的末尾偏移量就不用更新，不会造成空间浪费吗？
        //  答：插入数据只会比原来大或跟原来相等，不会比原来小。
        // 判断插入数据后是否需要更新FSO
        if (rawFSO < offset + raw.length) {
            // 如果当前FSO值小于插入数据后的末尾偏移量，则更新FSO值为新的末尾偏移量
            setFSO(page.getData(), (short) (offset + raw.length));
        }
    }

    /**
     * 在数据库崩溃后重新打开时恢复例程直接修改数据
     *
     * @param page   要修改数据的页面对象
     * @param raw    要修改页面的数据
     * @param offset 数据的起始偏移量，指示了在页面数据数组中开始修改数据的位置
     */
    public static void recoverUpdate(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
    }
}
