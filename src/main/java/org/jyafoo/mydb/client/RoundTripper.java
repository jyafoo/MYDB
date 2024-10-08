package org.jyafoo.mydb.client;

import org.jyafoo.mydb.transport.Packager;
import org.jyafoo.mydb.transport.Package;

/**
 * RoundTripper 类实现了单次收发动作
 *
 * @author jyafoo
 * @since 2024/10/8
 */
public class RoundTripper {
    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    /**
     * 执行一个包的往返传输
     * <p>
     * 此方法首先发送一个包，然后接收一个包
     * 主要用于需要通过网络等媒介进行数据交换的场景
     *
     * @param pkg 要发送的包，可以包含各种数据
     * @return 返回接收到的包，可能与发送的包不同
     * @throws Exception 如果发送或接收过程中发生错误，将抛出异常
     */
    public Package roundTrip(Package pkg) throws Exception {
        // 发送包，将数据传输到目标地址
        packager.send(pkg);
        // 接收包，从目标地址获取传输回来的数据
        return packager.receive();
    }


    public void close() throws Exception {
        packager.close();
    }
}
