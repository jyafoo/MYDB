package org.jyafoo.mydb.backend.dm.logger;

import org.jyafoo.mydb.backend.utils.Panic;
import org.jyafoo.mydb.backend.utils.Parser;
import org.jyafoo.mydb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 日志文件管理接口
 *
 * @author jyafoo
 * @since 2024/9/30
 */
public interface Logger {

    /**
     * 将日志写入文件
     *
     * @param data 要记录的原始日志数据
     */
    void log(byte[] data);

    /**
     * 截断文件，使其长度为指定的字节数
     *
     * @param x 指定文件的新长度，单位为字节
     * @throws Exception 截断操作过程中发生I/O错误，或者文件不存在等异常情况
     */
    void truncate(long x) throws Exception;

    /**
     * 迭代器模式获取下一个日志条目
     *
     * @return 排除前导信息后的日志数据副本
     */
    byte[] next();

    /**
     * 重置文件指针到起始位置，即xCheckSum后的第一条日志
     */
    void rewind();

    /**
     * 关闭读写资源
     */
    void close();

    /**
     * 创建日志对象
     * @param path 日志文件的路径，用于创建日志文件
     * @return LoggerImpl对象，用于操作日志
     */
    static Logger create(String path) {
        File logFile = new File(path + LoggerImpl.LOG_SUFFIX);

        try {
            if (!logFile.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!logFile.canRead() || !logFile.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fileChannel = null;
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(logFile, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));
        try {
            fileChannel.position(0);
            fileChannel.write(buf);
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new LoggerImpl(randomAccessFile,fileChannel);
    }

    /**
     * 打开日志文件并返回对应的Logger对象
     * @param path 日志文件的路径，用于打开日志文件
     * @return LoggerImpl对象，用于操作日志
     */
    static Logger open(String path) {
        File logFile = new File(path + LoggerImpl.LOG_SUFFIX);
        if (!logFile.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }

        if (!logFile.canRead() || !logFile.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fileChannel = null;
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(logFile, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        LoggerImpl logger = new LoggerImpl(randomAccessFile, fileChannel);
        logger.init();
        return logger;
    }

}
