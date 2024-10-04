package org.jyafoo.mydb.backend.vm;

import org.jyafoo.mydb.common.Error;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 维护了一个依赖等待图，以进行死锁检测
 *
 * @author jyafoo
 * @since 2024/10/3
 */
public class LockTable {

    /**
     * 某个XID已经获得的资源的UID列表
     */
    private Map<Long, List<Long>> x2u;

    /**
     * UID被某个XID持有
     */
    private Map<Long, Long> u2x;

    /**
     * 正在等待UID的XID列表
     */
    private Map<Long, List<Long>> wait;

    /**
     * 正在等待资源的XID的锁
     */
    private Map<Long, Lock> waitLock;

    /**
     * XID正在等待的UID
     */
    private Map<Long, Long> waitU;

    /**
     * 锁
     */
    private Lock lock;


    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 每次出现等待的情况时，就尝试向图中增加一条边，并进行死锁检测。
     * 如果检测到死锁，就撤销这条边，不允许添加，并撤销该事务。
     *
     * @param xid 事务xid
     * @param uid 资源uid
     * @return 新的Lock对象，供等待的用户线程使用
     */
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            // 检查当前事务是否已经持有请求的锁
            if (isInList(x2u, xid, uid)) {
                return null;
            }

            // 如果当前用户未持有任何锁，则直接分配锁
            if (!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }

            waitU.put(xid, uid);
            putIntoList(wait, uid, xid);

            // 检查是否形成死锁
            if (hasDeadLock()) {
                waitU.remove(xid);
                removeFromList(wait, uid, xid);
                throw Error.DeadlockException;
            }

            // 创建并锁定一个新的重入锁，供等待的用户使用
            Lock lock0 = new ReentrantLock();
            lock0.lock();
            waitLock.put(xid, lock0);
            return lock0;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 在一个事务 commit 或者 abort 时，就可以释放所有它持有的锁，并将自身从等待图中删除
     *
     * @param xid 事务xid
     */
    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> list = x2u.get(xid);
            if (list != null) {
                while (!list.isEmpty()) {
                    Long uid = list.remove(0);
                    selectNewXid(uid);
                }
            }

            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);

        } finally {
            lock.unlock();
        }
    }

    /**
     * 从等待队列中选择一个xid来占用uid
     *
     * @param uid 资源唯一标识
     */
    private void selectNewXid(Long uid) {
        // 移除uid和xid的映射
        u2x.remove(uid);
        // 获取等待队列中所有的xid
        List<Long> list = wait.get(uid);
        // 如果等待队列为空，直接返回
        if (list == null) {
            return;
        }
        assert !list.isEmpty();

        // while 循环释放掉了这个线程所有持有的资源的锁，这些资源可以被等待的线程所获取
        while (!list.isEmpty()) {
            // 从 List 开头开始尝试解锁，还是个公平锁。
            // 解锁时，将该 Lock 对象 unlock 即可，这样业务线程就获取到了锁，就可以继续执行了。
            long xid = list.remove(0);

            // 如果xid不在等待锁的映射中，继续下一次循环
            if (waitLock.containsKey(xid)) {
                // 将xid映射到uid
                u2x.put(uid, xid);
                // 移除等待锁，并释放锁
                Lock lock0 = waitLock.remove(xid);
                waitU.remove(xid);
                lock0.unlock();
                // 找到可用的xid后，结束循环
                break;
            }
        }

        // 如果等待队列为空，移除uid的等待队列
        if (list.isEmpty()) {
            wait.remove(uid);
        }
    }

    /**
     * 从指定的Map中移除关联到uid0的列表中的uid1元素
     * <p>
     * 如果uid0作为键不存在于Map中，或者与uid0关联的列表为空，则不进行任何操作
     * 如果uid1成功从列表中移除，并且移除后列表为空，则同时从Map中移除uid0作为键的条目
     *
     * @param listMap 一个Map对象
     * @param uid0    长整型键值，用于标识Map中的条目
     * @param uid1    要从与uid0关联的列表中移除的长整型元素
     */
    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> list = listMap.get(uid0);
        if (list == null) {
            return;
        }

        Iterator<Long> iterator = list.iterator();
        while (iterator.hasNext()) {
            Long next = iterator.next();
            // 遍历列表，寻找与uid1匹配的元素
            if (next == uid1) {
                iterator.remove();
                break;
            }
        }

        // 如果与uid0关联的列表为空，则从Map中移除uid0作为键的条目
        if (list.isEmpty()) {
            listMap.remove(uid0);
        }
    }

    /**
     * 事务ID与对应的版本号
     */
    private Map<Long, Integer> xidStamp;
    /**
     * 全局版本号
     */
    private int stamp;

    /**
     * 检查系统中是否存在死锁
     * 死锁检测是通过图的遍历算法实现的，具体来说，是通过深度优先搜索（DFS）来检测循环依赖关系
     * 如果存在循环依赖，那么就表明系统中存在死锁
     *
     * @return 如果检测到死锁，则返回true；否则返回false
     */
    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        for (Long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            if (s != null && s > 0) {
                continue;
            }
            stamp++;
            if (dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 深搜查找图中是否有环
     * 该方法通过检查事务的 stamps 和等待关系来确定是否存在循环等待
     *
     * @param xid 当前检查的事务的 ID
     * @return 如果存在循环等待，则返回 true；否则返回 false
     */
    private boolean dfs(long xid) {
        // 获取当前事务的 stamp
        Integer stp = xidStamp.get(xid);
        // 如果当前事务的 stamp 存在且与当前检查的 stamp 相同，说明存在循环等待
        if (stp != null && stp == stamp) {
            return true;
        }
        // 如果当前事务的 stamp 存在且小于当前检查的 stamp，说明没有循环等待
        if (stp != null && stp < stamp) {
            return false;
        }
        // 将当前事务的 stamp 设置为当前检查的 stamp，表示已经访问过该事务
        xidStamp.put(xid, stamp);

        // 获取当前事务等待的用户 ID
        Long uid = waitU.get(xid);
        // 如果当前事务没有等待的用户，说明没有循环等待
        if (uid == null) {
            return false;
        }
        // 获取用户对应的事务 ID
        Long x = u2x.get(uid);
        // 断言用户对应的事务 ID 不应为空
        assert x != null;
        // 递归检查用户对应的事务，如果存在循环等待，则返回 true
        return dfs(x);
    }


    /**
     * 将指定的UID1放入与UID0关联的列表中，如果该列表不存在，则创建一个新的列表
     *
     * @param listMap 一个Map对象，用于存储UID0作为键和UID1的列表作为值的映射关系
     * @param uid0    要作为键的UID，标识列表的拥有者
     * @param uid1    要添加到与UID0关联的列表中的UID
     */
    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if (!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>());
        }
        listMap.get(uid0).add(0, uid1);
    }

    /**
     * 检查一个元素是否在指定的列表映射中
     *
     * @param listMap 包含长整型键和值为长整型列表的映射表示一对多的关系
     * @param uid0    第一个长整型ID作为映射中的键
     * @param uid1    第二个长整型ID用于检查是否在第一个ID对应的列表中
     * @return 如果uid1在uid0对应的列表中则返回true否则返回false
     */
    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> list = listMap.get(uid0);
        if (list == null) {
            return false;
        }

        Iterator<Long> iterator = list.iterator();
        while (iterator.hasNext()) {
            Long e = iterator.next();
            if (e == uid1) {
                return true;
            }
        }

        return false;
    }


}
