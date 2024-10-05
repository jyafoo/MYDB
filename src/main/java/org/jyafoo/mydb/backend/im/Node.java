package org.jyafoo.mydb.backend.im;

import org.jyafoo.mydb.backend.common.SubArray;
import org.jyafoo.mydb.backend.dm.dataItem.DataItem;
import org.jyafoo.mydb.backend.tm.TransactionManagerImpl;
import org.jyafoo.mydb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * B+树节点
 * Node结构如下：
 * [LeafFlag][KeyNumber][SiblingUid]
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 *
 * @author jyafoo
 * @since 2024/10/4
 */
// TODO (jyafoo,2024/10/5,11:15) Node这一块好多不懂
public class Node {

    /**
     * 偏移量：定义节点是否为叶子节点，第0字节开始，占1个字节
     */
    public static final int IS_LEAF_OFFSET = 0;
    /**
     * 偏移量：定义节点中关键字数量，第1字节开始，占2个字节
     */
    public static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET + 1;
    /**
     * 偏移量：定义兄弟节点指针，第3字节开始，占8个字节
     */
    public static final int SIBLING_OFFSET = NO_KEYS_OFFSET + 2;
    /**
     * 偏移量：定义节点头部的大小，包括所有元数据的存储空间，第11字节开始，占8个字节
     */
    public static final int NODE_HEADER_SIZE = SIBLING_OFFSET + 8;
    /**
     * 平衡因子的数量，用于确定B+树的阶数和平衡性
     */
    public static final int BALANCE_NUMBER = 32;
    /**
     * 根据平衡因子计算节点的总大小，包括头部和关键字与子节点指针的存储空间
     */
    // TODO (jyafoo,2024/10/4,19:59) 不是很理解NODE_SIZE的计算
    public static final int NODE_SIZE = NODE_HEADER_SIZE + (2 * 8) * (BALANCE_NUMBER * 2 + 2);

    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;


    /**
     * 从B+树中加载节点
     * <p>
     * 根据节点的唯一标识符从B+树中加载节点信息它首先通过树的
     * 数据管理器读取特定标识符的数据项然后，它断言读取的数据项非空，确保有有效的
     * 数据用于节点初始化如果数据项非空，它将创建一个新的Node对象，并用从数据项中
     * 获取的信息进行初始化这包括B+树对象、数据项本身、原始数据和节点的唯一标识符
     *
     * @param bTree B+树对象，用于上下文信息
     * @param uid   节点的唯一标识符，用于定位特定节点
     * @return 返回一个初始化的Node对象，包含从B+树读取的数据
     * @throws Exception 如果在加载过程中发生错误，例如读取的数据项为null，则抛出异常
     */
    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem dataItem0 = bTree.dm.read(uid);
        assert dataItem0 != null;

        Node node = new Node();
        node.tree = bTree;
        node.dataItem = dataItem0;
        node.raw = dataItem0.data();
        node.uid = uid;
        return node;
    }

    /**
     * 生成一个根节点的数据
     *
     * @param left  左子树的根节点指针
     * @param right 右子树的根节点指针
     * @param key   分隔左右子树的键值
     * @return 返回初始化后的新根节点字节数组
     */
    public static byte[] newRootRaw(long left, long right, long key) {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(raw, false);
        setRawNoKeys(raw, 2);
        setRawSibling(raw, 0);
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);
        return raw.raw;
    }

    /**
     * 生成一个空的根节点数据
     *
     * @return 初始化后的根节点原始字节数组
     */
    static byte[] newNilRootRaw() {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);
        return raw.raw;
    }

    private static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        raw.raw[raw.start + IS_LEAF_OFFSET] = isLeaf ? (byte) 1 : (byte) 0;
    }

    private static boolean getRawIsLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte) 1;
    }

    private static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short) noKeys), 0, raw.raw, raw.start + NO_KEYS_OFFSET, 2);
    }

    private static int getRawNoKeys(SubArray raw) {
        return (int) Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start + NO_KEYS_OFFSET, raw.start + NO_KEYS_OFFSET + 2));
    }

    /**
     * 设置给定子数组的兄弟节点值
     *
     * @param raw     子数组对象，其兄弟节点信息将被更新
     * @param sibling 要设置的兄弟节点的长整型数值
     */
    private static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start + SIBLING_OFFSET, 8);
    }

    private static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start + SIBLING_OFFSET, raw.start + SIBLING_OFFSET + 8));
    }

    /**
     * 设置第k个子节点的UID
     * <p>
     * 此方法在给定的子数组中，根据节点的顺序编号kth，设置其UID为指定的值
     * 它通过计算偏移量来直接操作原始字节数组，然后使用系统级别的方法复制字节
     *
     * @param raw 子数组对象，包含原始字节数组及其起始位置
     * @param uid 要设置的UID值
     * @param kth 节点的顺序编号，用于计算在数组中的位置
     */
    private static void setRawKthSon(SubArray raw, long uid, int kth) {
        // TODO (jyafoo,2024/10/4,18:53) setRawKthSon的偏移量计算没看懂
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    /**
     * 在原始子数组中设置第k个键的原始字节表示
     * <p>
     * 此方法主要用于在解析或序列化过程中，将长整型键值以字节的形式写入到原始子数组的特定位置
     *
     * @param raw 子数组对象，包含原始数据和起始位置等信息
     * @param key 需要设置的长整型键值
     * @param kth 表示第k个键的位置，用于计算字节偏移量
     */
    private static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    private static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    /**
     * 搜索下一个资源类
     * <p>
     * 包含资源的唯一标识符（uid）以及其兄弟资源的唯一标识符（siblingUid）
     */
    class SearchNextRes {
        /**
         * 资源的唯一标识符
         */
        long uid;
        /**
         * 资源的兄弟资源的唯一标识符
         */
        long siblingUid;
    }

    /**
     * 寻找对应 key 的 UID, 如果找不到, 则返回兄弟节点的 UID
     */
    public SearchNextRes searchNext(long key) {
        dataItem.rLock();
        try {
            SearchNextRes searchNextRes = new SearchNextRes();
            // 获取数据项的总键数
            int noKeys = getRawNoKeys(this.raw);
            for (int i = 0; i < noKeys; i++) {
                long iKey = getRawKthKey(raw, i);
                if (key < iKey) {
                    searchNextRes.uid = getRawKthSon(raw, i);
                    searchNextRes.siblingUid = getRawSibling(raw);
                    return searchNextRes;
                }
            }

            // 如果遍历完所有键值后仍未找到下一个数据项，设置查找结果中的UID和兄弟节点UID
            searchNextRes.uid = 0;
            searchNextRes.siblingUid = getRawSibling(raw);
            return searchNextRes;
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 封装了在叶子节点搜索过程中找到的UID范围的结果类
     * <p>
     * 主要用于保存搜索到的UIDs列表以及相关的兄弟UID
     */
    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }

    /**
     * 在当前节点进行范围查找，范围是 [leftKey, rightKey]
     * <p>
     * 这里约定如果 rightKey 大于等于该节点的最大的 key, 则还同时返回兄弟节点的 UID，方便继续搜索下一个节点
     *
     * @param leftKey  左边界
     * @param rightKey 右边界
     * @return
     */
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();

        try {
            int noKeys = getRawNoKeys(raw);
            int kth = 0;
            // 先找到0-leftKey
            while (kth < noKeys) {
                long kthKey = getRawKthKey(raw, kth);
                if (kthKey >= leftKey) {
                    break;
                }
                kth++;
            }

            // 再遍历left-right
            ArrayList<Long> uids = new ArrayList<>();
            while (kth < noKeys) {
                long kthKey = getRawKthKey(raw, kth);
                if (kthKey > rightKey) {
                    break;
                }
                uids.add(kthKey);
                kth++;
            }

            // TODO (jyafoo,2024/10/5,10:03) Q：为什么查找总要找兄弟节点
            long siblingUid = 0;
            if (kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }

            LeafSearchRangeRes leafSearchRangeRes = new LeafSearchRangeRes();
            leafSearchRangeRes.uids = uids;
            leafSearchRangeRes.siblingUid = siblingUid;

            return leafSearchRangeRes;
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 插入并拆分结果类
     * <p>
     * 用于存储插入数据后，父节点的兄弟节点UID，新子节点UID，以及新键值
     */
    class InsertAndSplitRes {
        /**
         * 父节点的兄弟节点UID
         */
        long siblingUid;
        /**
         * 新子节点UID
         */
        long newSon;
        /**
         * 新键值
         */
        long newKey;
    }

    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes insertAndSplitRes = new InsertAndSplitRes();

        dataItem.before();

        try {
            success = insert(uid, key);
            if (!success) {
                insertAndSplitRes.siblingUid = getRawSibling(raw);
                return insertAndSplitRes;
            }

            if (needSplit()) {
                try {
                    SplitRes splitRes = split();
                    insertAndSplitRes.newSon = splitRes.newSon;
                    insertAndSplitRes.newKey = splitRes.newKey;
                    return insertAndSplitRes;
                } catch (Exception e) {
                    err = e;
                    throw e;
                }
            } else {
                return insertAndSplitRes;
            }
        } finally {
            // 清理资源，根据成功与否或异常情况回滚或提交事务
            if (err == null && success) {
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                dataItem.unBefore();
            }
        }
    }

    /**
     * 判断当前是否需要进行拆分操作
     * <p>
     * 本方法用于确定在特定条件下是否应该拆分，依据是比较BALANCE_NUMBER的两倍与特定条目的原始数量是否相等
     *
     * @return 如果满足拆分条件，则返回true；否则返回false
     */
    private boolean needSplit() {
        return BALANCE_NUMBER * 2 == getRawNoKeys(raw);
    }


    /**
     * 分裂结果类
     * <p>
     * 用于在分裂算法中传递新生成的子节点和键信息
     */
    class SplitRes {
        /**
         * 新生成的子节点的标识符
         */
        long newSon;
        /**
         * 新生成的键的值，用于在数据结构中插入或引用新节点
         */
        long newKey;
    }

    /**
     * 分裂节点方法
     * 当节点的键数量超过平衡因子的两倍减一，即达到了必须分裂的条件时，调用此方法进行节点分裂
     * 分裂过程中，会将当前节点的前半部分复制到一个新的节点中，为后续的平衡操作做准备
     *
     * @return 返回一个SplitRes对象，其中包含了新分裂出的子节点的指针以及分裂后的中间键，
     * 用于父节点的更新和插入操作
     * @throws Exception 如果在执行分裂过程中发生错误，如磁盘操作失败，则抛出异常
     */
    private SplitRes split() throws Exception {
        // 初始化一个新的SubArray对象来存储分裂后的节点数据
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        // 设置分裂后的节点的isLeaf标志，根据当前节点是否为叶子节点来决定
        setRawIsLeaf(nodeRaw, getRawIsLeaf(raw));
        // 设置分裂后的节点的键数量为平衡因子，因为分裂会均匀分割键
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        // 设置分裂后的节点的兄弟节点指针
        setRawSibling(nodeRaw, getRawSibling(raw));
        // 从当前节点中复制前BALANCE_NUMBER个键到分裂后的节点
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);

        // 在树中插入分裂后的节点，并获取其指针
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);
        // 更新当前节点的键数量为平衡因子，因为已经分裂了一半到新的节点
        setRawNoKeys(raw, BALANCE_NUMBER);
        // 更新当前节点的兄弟节点指针为新插入的节点指针
        setRawSibling(raw, son);

        // 创建并返回SplitRes对象，包含新分裂出的子节点的指针和分裂后的中间键
        SplitRes splitRes = new SplitRes();
        splitRes.newSon = son;
        splitRes.newKey = getRawKthKey(nodeRaw, 0);

        return splitRes;
    }

    /**
     * 从一个SubArray中的特定位置复制数据到另一个SubArray
     * <p>
     * 此方法主要用于在SubArray内部进行元素的复制，跳过前导头信息
     *
     * @param from 源SubArray，包含要复制的数据
     * @param to   目标SubArray，数据将被复制到此
     * @param kth  指定从源SubArray的第kth个元素开始复制
     */
    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        // 计算源SubArray中第kth个元素的起始位置，考虑NODE_HEADER_SIZE和元素大小
        int offset = from.start + NODE_HEADER_SIZE + kth * (8 * 2);
        // 从源SubArray的特定位置复制数据到目标SubArray，忽略前导头信息
        System.arraycopy(from.raw, offset, to.raw, to.start + NODE_HEADER_SIZE, from.end - offset);
    }


    /**
     * 在B+树中插入一个新键值对
     * <p>
     * 该方法负责在给定的节点（由raw表示）中找到合适的位置插入新的键值对
     * 它需要处理叶子节点和非叶子节点的不同情况，并确保B树的性质在插入后仍然保持
     *
     * @param uid 要插入的键的唯一标识符
     * @param key 要插入的键值
     * @return 如果插入成功则返回true，否则返回false
     */
    private boolean insert(long uid, long key) {
        // 获取当前节点中的键数量
        int noKeys = getRawNoKeys(raw);
        // 初始化遍历的索引
        int kth = 0;
        // 寻找合适的位置插入新的键
        while (kth < noKeys) {
            // 获取当前遍历到的键
            long kthKey = getRawKthKey(raw, kth);
            // 如果当前键小于新键，则向后移动遍历
            if (kthKey >= key) {
                break;
            }
            kth++;
        }

        // 如果遍历到了最后一个键，并且存在兄弟节点，则插入失败
        if (kth == noKeys && getRawSibling(raw) != 0) {
            return false;
        }

        // 如果是叶子节点，则直接在叶子中插入新键
        if (getRawIsLeaf(raw)) {
            // 为新键在叶子节点中腾出空间
            shiftRawKth(raw, kth);
            // 在找到的位置插入新键
            setRawKthKey(raw, key, kth);
            // 设置新键对应的子节点UID
            setRawKthSon(raw, uid, kth);
            // 更新节点中的键数量
            setRawNoKeys(raw, noKeys + 1);
        } else {
            // 如果是非叶子节点，则需要先调整当前节点的键
            // 获取当前遍历到的键
            long kk = getRawKthKey(raw, kth);
            // 在找到的位置插入新键
            setRawKthKey(raw, key, kth);
            // 为新键之后的键腾出空间
            shiftRawKth(raw, kth + 1);
            // 将遍历到的键后移一位
            setRawKthKey(raw, kk, kth + 1);
            // 设置新键对应的子节点UID
            setRawKthSon(raw, uid, kth + 1);
            // 更新节点中的键数量
            setRawNoKeys(raw, noKeys + 1);
        }
        return true;
    }


    /**
     * 在原始子数组中移动特定的键值对
     * <p>
     * 此方法的目的是在节点内部对特定的键值对进行右移操作，为新的键值对腾出空间
     * 它通过从最后一个字节开始，到特定键值对的开始位置，逐个字节地复制数据，实现键值对的移动
     *
     * @param raw 原始子数组对象，包含需要移动的键值对
     * @param kth 指定的键值对索引，用于定位需要移动的键值对位置
     */
    private void shiftRawKth(SubArray raw, int kth) {
        // TODO (jyafoo,2024/10/5,10:46) Node下的shiftRawKth没看懂
        int begin = raw.start + NODE_HEADER_SIZE + (kth + 1) * (8 * 2);
        int end = raw.start + NODE_SIZE - 1;
        for (int i = end; i >= begin; i--) {
            raw.raw[i] = raw.raw[i - (8 * 2)];
        }
    }

    /**
     * 判断当前节点是否为叶子节点
     *
     * @return 如果当前节点是叶子节点，返回true；否则返回false
     */
    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIsLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 释放资源
     */
    public void release() {
        dataItem.release();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIsLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for (int i = 0; i < KeyNumber; i++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }

}
