package org.jyafoo.mydb.client;

import org.jyafoo.mydb.transport.Packager;
import org.jyafoo.mydb.transport.Package;

/**
 * @author jyafoo
 * @since 2024/10/8
 */
public class Client {
    private RoundTripper roundTripper;

    public Client(Packager packager) {
        this.roundTripper = new RoundTripper(packager);
    }

    /**
     * 执行一个状态包的往返操作，并返回结果数据
     *
     * @param stat 输入的状态数据，用于创建Package对象
     * @return byte数组，包含成功执行后的数据
     * @throws Exception 如果执行过程中发生错误，将抛出异常
     */
    public byte[] execute(byte[] stat) throws Exception {
        // 创建一个新的Package对象，用于承载初始状态数据
        Package pkg = new Package(stat, null);

        // 执行往返操作，获取响应的Package对象
        Package resPkg = roundTripper.roundTrip(pkg);

        // 检查响应包中是否包含错误信息
        if(resPkg.getErr() != null) {
            // 如果有错误，抛出该错误信息
            throw resPkg.getErr();
        }

        // 返回成功执行后的数据
        return resPkg.getData();
    }


    public void close() {
        try {
            roundTripper.close();
        } catch (Exception e) {
        }
    }
}
