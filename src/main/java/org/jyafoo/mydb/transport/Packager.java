package org.jyafoo.mydb.transport;

/**
 * Packager 是 Encoder 和 Transporter 的结合体，直接对外提供 send 和 receive 方法
 *
 * @author jyafoo
 * @since 2024/10/8
 */
public class Packager {
    private Transporter transpoter;
    private Encoder encoder;

    public Packager(Transporter transpoter, Encoder encoder) {
        this.transpoter = transpoter;
        this.encoder = encoder;
    }

    /**
     * 发送包裹
     * <p>
     * 将给定的包裹对象编码为字节数组，并通过传输器发送出去
     * 此方法展示了从数据打包到实际发送的数据传输过程
     *
     * @param pkg 待发送的包裹对象，包含待编码发送的数据
     * @throws Exception 如果编码或发送过程中发生错误，将抛出异常
     */
    public void send(Package pkg) throws Exception {
        // 将包裹对象编码为字节数组，这是数据传输的常见步骤
        byte[] data = encoder.encode(pkg);
        // 通过传输器发送字节数组，完成数据发送过程
        transpoter.send(data);
    }

    /**
     * 接收数据包
     * <p>
     * 本方法负责从传输层接收原始数据，并将其解码为可理解的数据包格式
     * 它首先通过transpoter对象接收原始数据，然后使用encoder对象将这些原始数据解码成数据包
     * 如果在接收或解码过程中发生任何问题，将抛出异常
     *
     * @return Package 解码后的数据包，如果解码成功
     * @throws Exception 如果接收或解码过程中发生错误
     */
    public Package receive() throws Exception {
        // 从传输层接收原始数据
        byte[] data = transpoter.receive();

        // 将接收到的原始数据解码为数据包
        return encoder.decode(data);
    }


    /**
     * 关闭传输器
     * <p>
     * 此方法旨在释放与传输器相关的资源通过调用transpoter的close方法来实现
     * 它确保在对象不再使用时，底层资源能够被正确释放
     *
     * @throws Exception 如果在关闭过程中发生错误，可能会抛出异常
     */
    public void close() throws Exception {
        transpoter.close();
    }

}
