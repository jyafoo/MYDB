package org.jyafoo.mydb.backend.common;

import org.jyafoo.mydb.common.Error;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AbstractCache 实现了一个引用计数策略的缓存
 *
 * @author jyafoo
 * @since 2024/9/29
 */
public abstract class AbstractCache<T> {

    /**
     * 实际缓存数据
     */
    private HashMap<Long, T> cache;
    /**
     * 元素的引用个数
     */
    private HashMap<Long, Integer> references;
    /**
     * 正在获取某资源的线程
     */
    private HashMap<Long, Boolean> getting;

    /**
     * 缓存的最大缓存资源数量，用于控制缓存的大小
     */
    private int maxResource;

    /**
     * 缓存中元素的个数
     */
    private int count = 0;

    /**
     * 锁，用于同步操作
     */
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 根据键获取资源对象
     * 该方法首先尝试从缓存中获取资源，如果缓存中不存在且资源获取未被其他线程锁定，则尝试将资源加入缓存
     * 如果缓存已满，则抛出CacheFullException异常
     *
     * @param key 资源的键
     * @return 资源对象
     * @throws Exception 如果在获取资源过程中发生错误
     */
    protected T get(long key) throws Exception {
        // TODO (jyafoo,2024/9/30,10:18) Q：获取资源不是很理解？A：这是缓存的抽象类，下游模块可通过cache对磁盘进行读写
        // 循环上锁是为了拿到获取资源的权利
        while (true) {
            // 上锁，防止多线程并发访问资源缓存时出现竞争条件
            lock.lock();
            // 1、检查当前请求的资源是否正在被其他线程获取
            if (getting.containsKey(key)) {
                // 请求的资源正在被其他线程获取，释放锁并短暂休眠，然后重新尝试
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    // 休眠被中断时处理异常，打印堆栈跟踪后继续下一次循环尝试
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            // 2、检查资源是否已经在缓存中
            if (cache.containsKey(key)) {
                // 资源在缓存中，直接返回，并增加引用计数
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }

            // 3、不在缓存中，准备尝试获取资源，检查是否达到最大资源限制
            if (maxResource > 0 && count == maxResource) {
                // 缓存已满，无法获取更多资源，释放锁并抛出异常
                lock.unlock();
                throw Error.CacheFullException;
            }
            // 4、未达资源限制，准备获取资源
            count++;
            getting.put(key, true);
            lock.unlock();
            break; // 退出循环，尝试获取资源
        }

        T obj = null;
        try {
            // 实际尝试获取资源，并准备放入缓存
            obj = getForCache(key);
        } catch (Exception e) {
            // 获取资源过程中发生异常，减少计数，移除获取标记，并重新抛出异常
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        // 获取资源成功，更新缓存和引用计数
        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();

        return obj;
    }

    /**
     * 释放与指定key关联的缓存对象
     * 当一个缓存对象不再需要时，通过此方法来释放它
     * 如果该对象正在被其他地方使用（引用计数大于1），则不释放，仅减少引用计数
     *
     * @param key 缓存对象的键
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            if (ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count--;
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    // TODO (jyafoo,2024/9/30,10:49) 这里感觉只有释放，为什么说写回？
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * 当资源不在缓存时的获取行为
     *
     * @param key 资源id
     * @return
     * @throws Exception
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时的写回行为
     *
     * @param obj 资源对象
     */
    protected abstract void releaseForCache(T obj);

}
