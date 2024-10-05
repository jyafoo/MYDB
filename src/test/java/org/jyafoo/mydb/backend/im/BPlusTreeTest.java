package org.jyafoo.mydb.backend.im;

import org.junit.Test;
import org.jyafoo.mydb.backend.dm.DataManager;
import org.jyafoo.mydb.backend.dm.pageCache.PageCache;
import org.jyafoo.mydb.backend.tm.MockTransactionManager;
import org.jyafoo.mydb.backend.tm.TransactionManager;

import java.io.File;
import java.util.List;

public class BPlusTreeTest {
    @Test
    public void testTreeSingle() throws Exception {
        TransactionManager tm = new MockTransactionManager();
        DataManager dm = DataManager.create("C:\\Users\\JIA\\Desktop\\HLJU_CSTI\\25020-数据库科系统概论\\25022-实验代码\\20225958 李嘉富\\实验三四\\MYDB\\src\\main\\resources\\db\\TestTreeSingle", PageCache.PAGE_SIZE * 10, tm);

        long root = BPlusTree.create(dm);
        BPlusTree tree = BPlusTree.load(root, dm);


        int lim = 10000;
        for (int i = lim - 1; i >= 0; i--) {
            tree.insert(i, i);
        }

        for (int i = 0; i < lim; i++) {
            List<Long> uids = tree.search(i);
            assert uids.size() == 1;
            assert uids.get(0) == i;
        }
        dm.close();
        tm.close();

        assert new File("C:\\Users\\JIA\\Desktop\\HLJU_CSTI\\25020-数据库科系统概论\\25022-实验代码\\20225958 李嘉富\\实验三四\\MYDB\\src\\main\\resources\\db\\TestTreeSingle.db").delete();
        assert new File("C:\\Users\\JIA\\Desktop\\HLJU_CSTI\\25020-数据库科系统概论\\25022-实验代码\\20225958 李嘉富\\实验三四\\MYDB\\src\\main\\resources\\db\\TestTreeSingle.log").delete();
    }
}