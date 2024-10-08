package org.jyafoo.mydb.backend.im;

import org.jyafoo.mydb.backend.common.SubArray;
import org.jyafoo.mydb.backend.dm.DataManager;
import org.jyafoo.mydb.backend.dm.dataItem.DataItem;
import org.jyafoo.mydb.backend.tm.TransactionManagerImpl;
import org.jyafoo.mydb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * B+树
 * <p>
 *
 * @author jyafoo
 * @since 2024/10/4
 */
public class BPlusTree {
    /**
     * IM 在操作 DM 时，使用的事务都是 SUPER_XID
     */
    DataManager dm;
    /**
     *
     */
    long bootUid;
    /**
     * B+ 树在插入删除时，会动态调整，根节点不是固定节点，于是设置一个 bootDataItem，该 DataItem 中存储了根节点的 UID
     */
    DataItem bootDataItem;
    /**
     * 锁
     */
    Lock bootLock;

    /**
     * 创建一个初始数据结构
     * <p>
     * 该方法首先创建一个空的根节点，然后将其插入数据管理器中两次
     * 第一次插入是为了创建根节点，第二次插入是为了将根节点的UID作为数据创建一个新的节点
     *
     * @param dm 数据管理器对象，用于操作数据存储
     * @return 返回创建的根节点的UID
     * @throws Exception 如果插入操作失败，抛出异常
     */
    public static long create(DataManager dm) throws Exception {
        byte[] rawRoot = Node.newNilRootRaw();
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }

    /**
     * 根据指定的启动UID加载B+树
     *
     * @param bootUid 启动B+树的唯一标识
     * @param dm      数据管理器，用于读取和管理数据
     * @return 返回加载的BPlusTree实例
     * @throws Exception 如果读取失败或数据项为空，则抛出异常
     */
    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;

        BPlusTree bPlusTree = new BPlusTree();
        bPlusTree.bootUid = bootUid;
        bPlusTree.dm = dm;
        bPlusTree.bootDataItem = bootDataItem;
        bPlusTree.bootLock = new ReentrantLock();
        return bPlusTree;
    }

    /**
     * 获取根节点的uid
     *
     * @return 资源唯一标识uid
     */
    private long rootUid() {
        bootLock.lock();
        try {
            SubArray subArray = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(subArray.raw, subArray.start, subArray.start + 8));
        } finally {
            bootLock.unlock();
        }
    }

    /**
     * 更新系统启动时的根节点UID
     * <p>
     * 该方法用于在系统启动期间，更新根节点的UID信息它通过锁定bootLock来确保线程安全，
     * 计算新的根节点RAW数据，并通过数据库管理器插入新的根节点UID，最后更新启动数据项中的根节点UID信息
     *
     * @param left     左子树的根节点UID
     * @param right    右子树的根节点UID
     * @param rightKey 右子树根节点的密钥
     * @throws Exception 如果在操作过程中发生错误，将抛出异常
     */
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            bootDataItem.before();
            SubArray dataItemRaw = bootDataItem.data();
            // 复制新的根节点UID到启动数据项的相应位置
            System.arraycopy(Parser.long2Byte(newRootUid), 0, dataItemRaw.raw, dataItemRaw.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }

    /**
     * 在当前节点中查找叶子节点
     * <p>
     * 如果当前节点是叶子节点，直接返回其UID，否则，继续在子节点中查找
     *
     * @param nodeUid 当前节点的UID
     * @param key     需要查找的键
     * @return 找到的叶子节点的UID
     * @throws Exception 如果查找过程中发生错误，则抛出异常
     */
    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if (!isLeaf) {
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }

        return nodeUid;
    }

    /**
     * 搜索给定键的下一个节点的UID
     * <p>
     * 此方法通过遍历节点树来查找下一个具有给定键的节点，并返回其UID
     * 如果找不到具有给定键的节点，则继续遍历兄弟节点
     *
     * @param nodeUid 当前节点的UID，用于开始搜索
     * @param key     要搜索的键值
     * @return 返回包含给定键的下一个节点的UID，如果找不到则返回0
     * @throws Exception 如果在搜索过程中遇到错误，则抛出异常
     */
    private long searchNext(long nodeUid, long key) throws Exception {
        while (true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.SearchNextRes searchNextRes = node.searchNext(key);
            node.release();
            if (searchNextRes.uid != 0) {
                return searchNextRes.uid;
            }
            nodeUid = searchNextRes.siblingUid;
        }
    }

    /**
     * 根据键值搜索对应的值列表
     *
     * @param key 搜索的键值
     * @return 包含对应值的列表
     * @throws Exception 如果搜索过程中发生错误，抛出异常
     */
    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    /**
     * 根据给定的左、右键值范围，搜索并返回所有在此范围内的键的UID列表
     * <p>
     * 此方法首先定位到包含给定左键值的叶子节点，然后遍历所有包含在键值范围内的叶子节点，
     * 收集它们的UID，最后返回这些UID的列表
     *
     * @param leftKey  左键值，用于定位起始的叶子节点
     * @param rightKey 右键值，与左键值一起定义搜索的键值范围
     * @return 返回一个包含所有在指定键值范围内节点的UID的列表
     * @throws Exception 当搜索过程中出现任何错误时抛出
     */
    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        // 获取根节点的UID
        long rootUid = rootUid();
        // 搜索并获取包含左键值的叶子节点的UID
        long leafUid = searchLeaf(rootUid, leftKey);

        List<Long> uids = new ArrayList<>();
        while (true) {
            // 加载当前叶子节点
            Node leaf = Node.loadNode(this, leafUid);
            // 在当前叶子节点中搜索指定键值范围内的所有键，并获取搜索结果
            Node.LeafSearchRangeRes leafSearchRange = leaf.leafSearchRange(leftKey, rightKey);
            // 释放叶子节点资源
            leaf.release();
            // 将当前叶子节点中符合条件的所有键的UID添加到结果列表中
            uids.addAll(leafSearchRange.uids);
            // 如果没有下一个兄弟节点，则结束循环
            if (leafSearchRange.siblingUid == 0) {
                break;
            } else {
                // 否则，将下一个兄弟节点的UID设置为当前叶子节点的UID，以便在下一次循环中进行搜索
                leafUid = leafSearchRange.siblingUid;
            }
        }
        return uids;
    }

    /**
     * 插入结果类，用于存储插入操作后的相关信息
     * <p>
     * 包括新插入的节点编号和新分配的键值
     */
    class InsertRes {
        long newNode;
        long newKey;
    }

    /**
     * 插入一个键值对到树中
     * <p>
     * 此方法首先获取根用户的UID，然后尝试在树中插入一个新节点
     * 如果插入成功，可能需要更新根用户的UID和键值
     *
     * @param key 要插入的键值
     * @param uid 用户的唯一标识符，用于确定插入的位置
     * @throws Exception 如果插入过程中出现错误，抛出异常
     */
    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        if (res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    /**
     * 插入操作主要方法
     * <p>
     * 尝试在指定的节点下插入一个键，并返回插入结果
     * 如果节点是叶节点，则直接在该节点中插入键
     * 如果节点是非叶节点，则先查找合适的子节点，然后递归插入
     * 在插入过程中，如果出现节点分裂，则处理分裂后的新节点和新键
     *
     * @param nodeUid 要插入的节点的唯一标识符
     * @param uid     插入的键的唯一标识符
     * @param key     要插入的键
     * @return 返回插入操作的结果，包括可能的新节点和新键的标识符
     * @throws Exception 如果插入操作过程中发生错误，则抛出异常
     */
    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        if (isLeaf) {
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            long next = searchNext(nodeUid, key);
            InsertRes ir = insert(next, uid, key);
            if (ir.newNode != 0) {
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }

    /**
     * 插入键值对，并在节点分裂时进行处理
     * <p>
     * 当节点中的键值对数量超过一定阈值时，节点会分裂为两个节点
     * 如果插入操作导致节点分裂，则返回新节点的信息和分裂键
     *
     * @param nodeUid 当前节点的唯一标识符
     * @param uid     键值对中的值，用于标识插入的键值对
     * @param key     键值对中的键，用于排序和查找
     * @return InsertRes 包含新节点信息和分裂键的对象
     * @throws Exception 如果插入操作失败，则抛出异常
     */
    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while (true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.InsertAndSplitRes insertAndSplit = node.insertAndSplit(uid, key);

            node.release();

            // 检查插入操作是否导致节点分裂
            if (insertAndSplit.siblingUid != 0) {
                nodeUid = insertAndSplit.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newNode = insertAndSplit.newSon;
                res.newKey = insertAndSplit.newKey;
                return res;
            }
        }
    }

    /**
     * 关闭资源
     */
    public void close() {
        bootDataItem.release();
    }
}
