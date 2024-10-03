package org.jyafoo.mydb.backend.tm;

import org.jyafoo.mydb.backend.utils.Panic;
import org.jyafoo.mydb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 事务管理器接口，用于控制和管理数据库事务
 * 提供了一些接口供其他模块调用，用来创建事务和查询事务状态
 *
 * @author jyafoo
 * @since 2024/9/28
 */
public interface TransactionManager {

    /**
     * 开启一个新事务
     *
     * @return 事务id
     */
    long begin();

    /**
     * 提交一个事务
     *
     * @param xid 事务id
     */
    void commit(long xid);

    /**
     * 取消一个事务
     *
     * @param xid 事务id
     */
    void abort(long xid);

    /**
     * 查询一个事务的状态是否正在进行的状态
     *
     * @param xid 事务id
     * @return 如果事务正在进行中，返回true；否则返回false
     */
    boolean isActive(long xid);

    /**
     * 查询一个事务的状态是否是已提交
     *
     * @param xid 事务id
     * @return 如果事务已经提交，返回true；否则返回false
     */
    boolean isCommitted(long xid);

    /**
     * 查询一个事务的状态是否是已取消
     *
     * @param xid 事务id
     * @return 如果事务已经被取消，返回true；否则返回false
     */
    boolean isAborted(long xid);

    /**
     * 关闭TM (Transaction Manager)
     * 释放事务管理器使用的资源，事务管理器不再接收调用
     */
    void close();


    static TransactionManagerImpl create(String path) {
        File file = new File(path + TransactionManagerImpl.XID_SUFFIX);

        // 1、创建事务文件
        try {
            if (!file.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 2、判断文件是否能读写
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        // 3、打开文件，建立文件通道
        FileChannel fileChannel = null;
        RandomAccessFile randomAccessFile = null;

        try {
            randomAccessFile = new RandomAccessFile(file, "rw");  // 读写(rw)模式打开文件file
            fileChannel = randomAccessFile.getChannel();  // 从RandomAccessFile获取一个文件的通道对象，用于执行更高级的文件读写操作
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 4、读取文件头，获取xidCounter
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            // TODO (jyafoo,2024/9/28,21:51) Q5：不是很懂为什么这里获取一次文件头，在checkXIDCounter还要获取一次？因为第一次创建文件的时候要对空的xid文件写写入一个初始值
            fileChannel.position(0);
            fileChannel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 5、创建事务管理器实现类
        return new TransactionManagerImpl(randomAccessFile, fileChannel);
    }

    static TransactionManagerImpl open(String path) {
        File file = new File(path + TransactionManagerImpl.XID_SUFFIX);

        // 1、检查文件是否存在
        if (!file.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }

        // 2、判断文件是否能读写
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        // 3、打开文件，建立文件通道
        FileChannel fileChannel = null;
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");  // 读写(rw)模式打开文件file
            fileChannel = randomAccessFile.getChannel();  // 从RandomAccessFile获取一个文件的通道对象，用于执行更高级的文件读写操作
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 4、创建事务管理器实现类
        return new TransactionManagerImpl(randomAccessFile, fileChannel);

    }

}

