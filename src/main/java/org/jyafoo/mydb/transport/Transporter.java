package org.jyafoo.mydb.transport;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.*;
import java.net.Socket;

/**
 * 传输类
 * <p>
 * 通过Encoder编码之后的信息会通过 Transporter 类，写入输出流发送出去。
 * 为了避免特殊字符造成问题，这里会将数据转成十六进制字符串（Hex String），并为信息末尾加上换行符。
 * 这样在发送和接收数据时，就可以很简单地使用 BufferedReader 和 Writer 来直接按行读写了。
 *
 * @author jyafoo
 * @since 2024/10/8
 */

public class Transporter {
    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;

    public Transporter(Socket socket) throws IOException {
        this.socket = socket;
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    /**
     * 发送数据
     * <p>
     * 将字节数组数据转换为十六进制字符串并写入输出流
     *
     * @param data 要发送的字节数组
     * @throws Exception 如果数据转换或写入过程中发生错误
     */
    public void send(byte[] data) throws Exception {
        // 将字节数组转换为十六进制字符串
        String raw = hexEncode(data);
        // 将转换后的字符串写入输出流
        writer.write(raw);
        // 刷新输出流，确保数据立即写入
        writer.flush();
    }


    /**
     * 接收数据并返回字节数组
     * <p>
     * 此方法从一个读者对象读取一行字符串，并在字符串为null时关闭连接，
     * 然后将读取的字符串进行十六进制解码，返回相应的字节数组
     *
     * @return 字节数组，如果读取到的字符串为null，则返回null
     * @throws Exception 如果读取或解码过程中发生错误，抛出异常
     */
    public byte[] receive() throws Exception {
        // 从reader对象读取一行字符串
        String line = reader.readLine();
        // 如果读取到的字符串为null，表示没有数据可读，关闭连接
        if (line == null) {
            close();
        }
        // 对读取到的字符串进行十六进制解码，返回字节数组
        return hexDecode(line);
    }


    public void close() throws IOException {
        writer.close();
        reader.close();
        socket.close();
    }

    /**
     * 将字节数组转换为十六进制字符串
     *
     * @param buf 字节数组，包含要转换的数据
     * @return 返回转换后的十六进制字符串，末尾带换行符
     */
    private String hexEncode(byte[] buf) {
        // 使用Hex类的静态方法encodeHexString将字节数组转换为十六进制字符串，并添加换行符
        return Hex.encodeHexString(buf, true) + "\n";
    }


    /**
     * 将十六进制字符串解码为字节数组
     *
     * @param buf 十六进制表示的字符串
     * @return 解码后的字节数组
     * @throws DecoderException 如果输入的字符串不是有效的十六进制字符串，则抛出此异常
     */
    private byte[] hexDecode(String buf) throws DecoderException {
        return Hex.decodeHex(buf);
    }

}

