package org.jyafoo.mydb.backend.dm.pageIndex;

import org.jyafoo.mydb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 页面索引，缓存了每一页的空闲空间
 * <p>
 * 用于在上层模块进行插入操作时，能够快速找到一个合适空间的页面，而无需从磁盘或者缓存中检查每一个页面的信息。
 *
 * @author jyafoo
 * @since 2024/10/2
 */
public class PageIndex {
    /**
     * 分区间隔，将一页划成40个区间
     */
    private static final int INTERVALS_NO = 40;

    /**
     * 页面缓存的每个区间阈值大小
     */
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    /**
     * 存储PageInfo列表的数组，用于按区间管理页面信息
     */
    private List<PageInfo>[] lists;

    /**
     * 锁
     */
    private Lock lock;

    @SuppressWarnings("unchecked")  // 抑制编译器对泛型类型检查警告
    public PageIndex() {
        lock = new ReentrantLock();
        // TODO (jyafoo,2024/10/2,11:07) Q9：为什么是创建INTERVALS_NO + 1个？
        lists = new List[INTERVALS_NO + 1];
        for (int i = 0; i < INTERVALS_NO + 1; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    /**
     * 根据指定的spaceSize选择并返回一个PageInfo对象。
     *
     * 被选择的页，会直接从 PageIndex 中移除
     * @param spaceSize 请求的页大小，用于确定查找的区间
     * @return 可用的PageInfo对象，如果找不到则返回null
     */
    public PageInfo select(int spaceSize) {
        // TODO (jyafoo,2024/10/2,11:18) Q9：这一段select没看懂什么意思？找到一个合适的页面来插入数据
        lock.lock();
        try {
            // 计算请求的页大小对应于哪个区间
            int number = spaceSize / THRESHOLD;
            // 确保计算结果不会小于区间数量
            if (number < INTERVALS_NO) {
                number++;
            }

            // 遍历区间，尝试找到并返回一个可用的PageInfo对象
            while (number <= INTERVALS_NO) {
                if (lists[number].size() == 0) {
                    number++;
                    continue;
                }
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 向列表中添加页面信息。
     *
     * 上层模块使用完这个页面后，需要将其重新插入 PageIndex
     * @param pgno 页号
     * @param freeSpace 空闲空间
     */
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            // 计算空闲空间段的数量，用于确定页面信息应添加到哪个列表中
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pgno,freeSpace));
        } finally {
            lock.unlock();
        }
    }


}
