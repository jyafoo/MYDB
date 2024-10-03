package org.jyafoo.mydb.backend.dm.logger;

import com.google.common.primitives.Bytes;
import org.jyafoo.mydb.backend.utils.Panic;
import org.jyafoo.mydb.backend.utils.Parser;
import org.jyafoo.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 日志文件读写
 * <p>
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 为后续所有日志计算的Checksum，int类型
 *
 * @author jyafoo
 * @since 2024/10/1
 */
public class LoggerImpl implements Logger {

    /**
     * 种子值：用于计算校验和
     */
    private static final int SEED = 13331;

    // 每条正确日志的格式为：[Size] [Checksum] [Data]
    /**
     * 单条日志data段的长度，4字节表示，从第0个字节开始算
     */
    private static final int OF_SIZE = 0;
    /**
     * 单条日志的校验和，通过指定的种子实现，4字节表示，从第4个字节开始算
     */
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    /**
     * 单条日志的数据段，从第8个字节开始算
     */
    private static final int OF_DATA = OF_CHECKSUM + 4;

    /**
     * 日志文件的后缀
     */
    public static final String LOG_SUFFIX = ".log";

    // 文件读写相关对象
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private Lock lock;  // 用于并发控制的锁

    /**
     * 当前日志指针的位置
     */
    private long position;
    /**
     * 日志文件的大小，初始化时记录，后续日志操作不更新此值
     */
    private long fileSize;
    /**
     * 整个日志文件的Checksum之和，用于检测文件的完整性，4字节表示
     */
    private int xChecksum;

    LoggerImpl(RandomAccessFile randomAccessFile, FileChannel fileChannel) {
        this.randomAccessFile = randomAccessFile;
        this.fileChannel = fileChannel;
        lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile randomAccessFile, FileChannel fileChannel, int xChecksum) {
        this.randomAccessFile = randomAccessFile;
        this.fileChannel = fileChannel;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    /**
     * 初始化日志文件，验证其完整性和校验和
     */
    void init() {
        long size = 0;
        try {
            size = randomAccessFile.length();
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 检查文件大小是否小于4字节，如果是，则认为日志文件不完整，因为xCheckSum就占有4字节
        if (size < 4) {
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fileChannel.position(0);
            fileChannel.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }

        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;

        checkAndRemoveBadTail();
    }

    /**
     * 检查并移除 BadTail。
     * <p>
     * BadTail 是在数据库崩溃时，没有来得及写完的日志数据，这个 BadTail 不一定存在
     */
    private void checkAndRemoveBadTail() {
        // 1、重置指针位置到第一条日志
        rewind();

        // 2、计算并比对总校验和
        int xChecksumTemp = 0;
        while (true) {
            byte[] log = internNext();
            if (log == null) {
                break;
            }
            xChecksumTemp = calChecksum(xChecksumTemp, log);
        }
        if (xChecksumTemp != this.xChecksum) {
            // 如果不匹配，说明日志文件损坏，抛出异常
            Panic.panic(Error.BadLogFileException);
        }

        // 3、尝试修剪日志文件，使其长度变为当前position，以删除可能的无效尾部
        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }

        // 4、将文件指针移动到position位置，为下一次读取或写入操作做准备
        try {
            randomAccessFile.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 5、重置指针位置到第一条日志
        rewind();
    }

    /**
     * 读取下一条日志信息
     *
     * @return 日志信息
     */
    private byte[] internNext() {
        // 1、检查文件末尾，如果达到文件末尾则返回null
        if (position + OF_DATA >= fileSize) {
            return null;
        }

        // 2、读取下一条日志
        // 2.1、先读size
        ByteBuffer bufSize = ByteBuffer.allocate(4);
        try {
            fileChannel.position(position);
            fileChannel.read(bufSize);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(bufSize.array());
        if (position + size + OF_DATA > fileSize) {
            return null;
        }

        // 2.2、读取整条日志
        ByteBuffer bufData = ByteBuffer.allocate(OF_DATA + size);
        try {
            fileChannel.position(position);
            fileChannel.read(bufData);
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 3、解析单条日志，计算校验和，并与数据条目中的校验和进行比较
        byte[] log = bufData.array();
        int checksum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if (checksum1 != checkSum2) {
            return null;
        }

        // 4、更新日志指针的位置
        position += log.length;
        return log;
    }

    /**
     * 计算单条日志的校验和
     *
     * @param xCheck 初始校验和值，为0说明计算一条日志的校验和
     * @param log    需要计算校验和的某条日志数据
     * @return 加入该条日志校验和的总校验和
     */
    private int calChecksum(int xCheck, byte[] log) {
        // TODO (jyafoo,2024/10/1,17:29) Q4：为什么校验和是这么算的，有什么说法？
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }


    @Override
    public void log(byte[] data) {
        // 1、将数据包裹成日志格式
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);

        // 2、写入文件
        lock.lock();
        try {
            fileChannel.position(fileChannel.size());
            fileChannel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }

        // 3、更新文件的校验和
        updateXChecksum(log);
    }

    /**
     * 将数据包裹成日志格式
     *
     * @param data 原始日志数据
     * @return 封装后的日志数据，包括数据长度、校验和和原始数据
     */
    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    /**
     * 更新日志文件的总校验和
     *
     * @param log 新增的已封装好的日志条目
     */
    private void updateXChecksum(byte[] log) {
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fileChannel.position(0);
            fileChannel.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fileChannel.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null) {
                return null;
            }
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        // TODO (jyafoo,2024/10/1,11:48) Q3：这里为什么把position设置为4？A：跳过4字节的xCheckSum开始读取日志
        position = 4;
    }

    @Override
    public void close() {
        try {
            fileChannel.close();
            randomAccessFile.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
