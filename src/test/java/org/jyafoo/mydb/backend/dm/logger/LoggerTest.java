package org.jyafoo.mydb.backend.dm.logger;

import org.junit.Test;
import org.jyafoo.mydb.backend.dm.logger.Logger;

import java.io.File;

public class LoggerTest {
    @Test
    public void testLogger() {
        Logger lg = Logger.create("C:\\Users\\JIA\\Desktop\\HLJU_CSTI\\25020-数据库科系统概论\\25022-实验代码\\20225958 李嘉富\\实验三四\\MYDB\\src\\main\\resources\\db\\logger_test");
        lg.log("aaa".getBytes());
        lg.log("bbb".getBytes());
        lg.log("ccc".getBytes());
        lg.log("ddd".getBytes());
        lg.log("eee".getBytes());
        lg.close();

        lg = Logger.open("C:\\Users\\JIA\\Desktop\\HLJU_CSTI\\25020-数据库科系统概论\\25022-实验代码\\20225958 李嘉富\\实验三四\\MYDB\\src\\main\\resources\\db\\logger_test");
        lg.rewind();

        byte[] log = lg.next();
        assert log != null;
        assert "aaa".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "bbb".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "ccc".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "ddd".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "eee".equals(new String(log));

        log = lg.next();
        assert log == null;

        lg.close();

        assert new File("C:\\Users\\JIA\\Desktop\\HLJU_CSTI\\25020-数据库科系统概论\\25022-实验代码\\20225958 李嘉富\\实验三四\\MYDB\\src\\main\\resources\\db\\logger_test.log").delete();
    }
}