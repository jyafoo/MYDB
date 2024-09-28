package org.jyafoo.mydb.backend.utils;

import java.security.SecureRandom;
import java.util.Random;

/**
 * 随机工具
 * @author jyafoo
 * @since 2024/9/28
 */
public class RandomUtil {

    /**
     * 生成指定长度的随机字节数组
     *
     * @param length 需要生成的随机字节数组的长度
     * @return 生成的随机字节数组
     */
    public static byte[] randomBytes(int length) {
        Random r = new SecureRandom();
        byte[] buf = new byte[length];
        r.nextBytes(buf);
        return buf;
    }
}
