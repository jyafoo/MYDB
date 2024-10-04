package org.jyafoo.mydb.backend.vm;

import org.jyafoo.mydb.backend.tm.TransactionManager;

/**
 * 可见性判断
 *
 * @author jyafoo
 * @since 2024/10/3
 */
public class Visibility {

    /**
     * 判断当前事务是否需要跳过对给定条目的版本检查
     *
     * @param tm          事务管理器，用于检查事务状态
     * @param transaction 当前事务，其级别和标识用于判断是否跳过版本检查
     * @param entry       数据条目，其xmax（最新事务标识）用于判断是否跳过版本检查
     * @return 如果当前事务需要跳过对给定条目的版本检查，则返回true；否则返回false
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction transaction, Entry entry) {
        long xmax = entry.getXmax();

        // 如果事务级别为0，即RC，表示不需要进行版本检查，直接返回false
        if (transaction.level == 0) {
            return false;
        } else {
            // TODO (jyafoo,2024/10/3,20:48) Q：isVersionSkip的跳过逻辑怎么理解，感觉上下文有点割裂了
            // 当且仅当最新事务已经提交，并且其标识大于当前事务标识或在当前事务快照中时，才跳过版本检查
            return tm.isCommitted(xmax) && (xmax > transaction.xid || transaction.isInSnapshot(xmax));
        }
    }

    /**
     * 判断给定的事务在事务管理器的控制下是否可以读取特定的数据条目
     *
     * @param tm          事务管理器
     * @param transaction 当前事务，其级别和标识用于判断是否跳过版本检查
     * @param entry       数据条目，事务尝试读取的目标
     * @return 如果数据条目对事务可见，则返回true；否则返回false
     */
    public static boolean isVisible(TransactionManager tm, Transaction transaction, Entry entry) {
        if (transaction.level == 0) {
            return readCommitted(tm, transaction, entry);
        } else {
            return repeatableRead(tm, transaction, entry);
        }
    }


    /**
     * 判断给定的事务是否可以读取指定的条目
     * <p>
     * 根据多版本并发控制（MVCC）原则，一个事务能够读取一个条目，当且仅当
     * 条目由该事务自己在当前事务开始前提交，或者条目由其他事务提交且当前事务未对其进行更新
     *
     * @param tm          事务管理器
     * @param transaction 当前事务
     * @param entry       待读取的条目
     * @return 如果当前事务可以读取条目，则返回true；否则返回false
     */
    private static boolean readCommitted(TransactionManager tm, Transaction transaction, Entry entry) {
        long xid = transaction.xid;
        long xmin = entry.getXmin();
        long xmax = entry.getXmax();

        /*
            读提交下，版本对事务的可见性逻辑如下：
            (XMIN == Ti and                             // 由Ti创建且
                XMAX == NULL                            // 还未被删除
            )
            or                                          // 或
            (XMIN is commited and                       // 由一个已提交的事务创建且
                (XMAX == NULL or                        // 尚未删除或
                (XMAX != Ti and XMAX is not commited)   // 由一个未提交的事务删除
            ))
        */
        if (xmin == xid && xmax == 0) {
            return true;
        }
        if (tm.isCommitted(xmin)) {
            if (xmax == 0) {
                return true;
            }
            if (xid != xmax) {
                if (!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }

        return false;
    }


    /**
     * @param tm          事务管理器
     * @param transaction 当前事务
     * @param entry       待读取的条目
     * @return 如果当前事务可以读取条目，则返回true；否则返回false
     */
    private static boolean repeatableRead(TransactionManager tm, Transaction transaction, Entry entry) {
        long xid = transaction.xid;
        long xmin = entry.getXmin();
        long xmax = entry.getXmax();

        if (xid == xmin && xmax == 0) {
            return true;
        }

        /*
            (XMIN == Ti and                 // 由Ti创建且
             (XMAX == NULL                  // 尚未被删除
            ))
            or                              // 或
            (XMIN is commited and           // 由一个已提交的事务创建且
             XMIN < XID and                 // 这个事务小于Ti且
             XMIN is not in SP(Ti) and      // 这个事务在Ti开始前提交且
             (XMAX == NULL or               // 尚未被删除或
              (XMAX != Ti and               // 由其他事务删除但是
               (XMAX is not commited or     // 这个事务尚未提交或
            XMAX > Ti or                    // 这个事务在Ti开始之后才开始或
            XMAX is in SP(Ti)               // 这个事务在Ti开始前还未提交
            ))))
         */
        // TODO (jyafoo,2024/10/3,20:30) RR的处理逻辑好绕
        if (tm.isCommitted(xmin) && xmin < xid && !transaction.isInSnapshot(xmin)) {
            if (xmax == 0) {
                return true;
            }

            if (xmax != xid) {
                if (!tm.isCommitted(xmax) || xmax > xid || transaction.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }
}
