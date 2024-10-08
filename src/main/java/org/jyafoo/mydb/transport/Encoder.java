package org.jyafoo.mydb.transport;

import com.google.common.primitives.Bytes;

import java.util.Arrays;

import org.jyafoo.mydb.common.Error;

/**
 * 编码器
 *
 * 每个 Package 在发送前，由 Encoder 编码为字节数组，在对方收到后同样会由 Encoder 解码成 Package 对象。
 * 若 flag 为 0，表示发送的是数据，那么 data 即为这份数据本身；
 * 如果 flag 为 1，表示发送的是错误，data 是 Exception.getMessage() 的错误提示信息。
 *
 * @author jyafoo
 * @since 2024/10/8
 */
public class Encoder {

    /**
     * 编码包为字节数组
     * <p>
     * 如果包中包含错误信息，则编码为包含错误信息的字节数组
     * 否则，编码为包含包数据的字节数组
     *
     * @param pkg 要编码的包
     * @return 编码后的字节数组
     */
    public byte[] encode(Package pkg) {
        // 检查包是否包含错误
        if (pkg.getErr() != null) {
            Exception err = pkg.getErr();
            String msg = "Intern server error!";
            // 如果错误信息非空，则使用错误信息
            if (err.getMessage() != null) {
                msg = err.getMessage();
            }
            // 返回包含错误标识和错误信息的字节数组
            return Bytes.concat(new byte[]{1}, msg.getBytes());
        } else {
            // 返回包含错误标识和包数据的字节数组
            return Bytes.concat(new byte[]{0}, pkg.getData());
        }
    }


    /**
     * 解码字节数组为Package对象
     * <p>
     * 该方法根据给定的字节数组数据，解析出一个Package对象如果数据格式不正确或数据量不足，
     * 则抛出异常该方法支持两种类型的Package解析：一种包含data数据，另一种包含error信息
     *
     * @param data 字节数组，包含Package的编码信息
     * @return 解码后的Package对象如果第一个字节为0，则包含data；如果第一个字节为1，则包含error
     * @throws Exception 当数据格式不正确或数据量不足时抛出异常
     */
    public Package decode(byte[] data) throws Exception {
        // 检查数据量是否小于1，如果是，则抛出异常，因为数据量过少无法解析
        if (data.length < 1) {
            throw Error.InvalidPkgDataException;
        }
        // 如果第一个字节为0，解析为包含data的Package对象
        if (data[0] == 0) {
            return new Package(Arrays.copyOfRange(data, 1, data.length), null);
        } else if (data[0] == 1) {
            // 如果第一个字节为1，解析为包含error的Package对象
            return new Package(null, new RuntimeException(new String(Arrays.copyOfRange(data, 1, data.length))));
        } else {
            // 如果第一个字节不是0或1，则抛出异常，因为数据格式不正确
            throw Error.InvalidPkgDataException;
        }
    }

}
