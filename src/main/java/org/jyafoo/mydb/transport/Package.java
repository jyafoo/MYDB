package org.jyafoo.mydb.transport;

/**
 * 传输的最基本结构，是 Package
 *
 * @author jyafoo
 * @since 2024/10/8
 */
public class Package {
    /// 数据数组，用于存储传输的具体数据
    byte[] data;
    /// 错误信息，用于在传输过程中记录可能发生的异常
    Exception err;

    public Package(byte[] data, Exception err) {
        this.data = data;
        this.err = err;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getErr() {
        return err;
    }
}
