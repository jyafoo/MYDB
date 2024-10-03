package org.jyafoo.mydb.backend.tm;

import org.jyafoo.mydb.backend.utils.Panic;
import org.jyafoo.mydb.backend.utils.Parser;
import org.jyafoo.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 事务管理器实现类
 *
 * @author jyafoo
 * @since 2024/9/28
 */
public class TransactionManagerImpl implements TransactionManager {

    /**
     * XID文件头长度为8个字节
     */
    static final int LEN_XID_HEADER_LENGTH = 8;

    /**
     * 每个xid的占用长度为1个字节
     */
    private static final int XID_FIELD_SIZE = 1;

    // 事务的三种状态
    /**
     * 执行
     */
    private static final byte FIELD_TRAN_ACTIVE = 0;
    /**
     * 提交
     */
    private static final byte FIELD_TRAN_COMMITTED = 1;
    /**
     * 取消
     */
    private static final byte FIELD_TRAN_ABORTED = 2;

    /**
     * 超级事务，永远为commited状态
     */
    public static final long SUPER_XID = 0; // TODO (JIA,2024/9/28,21:13) Q6：不太懂为什么为0而不是1，不跟执行冲突吗

    /**
     * XID 文件后缀
     */
    static final String XID_SUFFIX = ".xid";

    /**
     * 随机访问文件
     */
    private RandomAccessFile file;

    /**
     * 文件通道，用于支持高效的文件IO操作，比如文件的直接读写
     */
    private FileChannel fileChannel;

    /**
     * 生成唯一事务ID的计数器，确保每个事务都能获得唯一的ID
     */
    private long xidCounter;

    /**
     * 锁对象，用于在多线程环境下保护xidCounter的线程安全
     */
    private Lock counterLock;

    /**
     * 在构造函数创建了一个 TransactionManager 之后，首先要对 XID 文件进行校验，以保证这是一个合法的 XID 文件。
     * 校验的方式也很简单，通过文件头的 8 字节数字反推文件的理论长度，与文件的实际长度做对比。如果不同则认为 XID 文件不合法。
     */

    TransactionManagerImpl(RandomAccessFile randomAccessFile, FileChannel fileChannel) {
        this.file = randomAccessFile;
        this.fileChannel = fileChannel;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 检查XID文件中的计数器是否正确
     */
    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e) {
            Panic.panic(Error.BadXIDFileException);
        }

        // 在 XID 文件的头部，保存了一个 8 字节的数字，记录了这个 XID 文件管理的事务的个数，因此文件长度不能低于xid文件头长度
        if (fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fileChannel.position(0); // 设置文件通道的位置指针到文件的起始位置，即偏移量为0的位置
            fileChannel.read(buf); // 从当前文件通道的位置读取数据并填充到ByteBuffer对象buf中，知道buf填满
        } catch (IOException e) {
            Panic.panic(e);
        }

        xidCounter = Parser.parseLong(buf.array());

        long end = getXidPosition(xidCounter + 1);
        if (end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }
    }

    /**
     * 根据事务xid取得其在xid文件中对应的位置
     *
     * @param xid 全局事务标识符
     * @return xid在XID集合中的位置，单位为字节
     */
    private long getXidPosition(long xid) {
        // 事务 xid 在文件中的状态就存储在 (xid-1)+8 字节处，xid-1 是因为 xid 0（Super XID） 的状态不需要记录
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    @Override
    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;

            updateXIDStatus(xid, FIELD_TRAN_ACTIVE);
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    /**
     * 更新xid事务的状态为status
     *
     * @param xid    事务标识符，用于定位事务状态的位置
     * @param status 事务状态
     */
    private void updateXIDStatus(long xid, byte status) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;

        ByteBuffer buf = ByteBuffer.wrap(tmp); // 初始化了一个固定大小的缓冲区用于后续可能的数据读写操作
        try {
            fileChannel.position(offset);
            fileChannel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        try {
            // 将文件更改强制写入磁盘，false表示不阻塞其他写操作，true表示阻塞直到写操作完成
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 将 XID 加一，并更新 XID Header
     */
    private void incrXIDCounter() {
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter)); // 初始化了一个固定大小的缓冲区用于后续可能的数据读写操作

        try {
            fileChannel.position(0);
            fileChannel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 检测XID事务是否处于status状态
     *
     * @param xid    事务 XID
     * @param status 事务状态
     * @return 如果事务的当前状态与给定的状态相同，则返回true；否则返回false
     */
    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]); // 初始化了一个固定大小的缓冲区用于后续可能的数据读写操作

        try {
            fileChannel.position(offset);
            fileChannel.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return buf.array()[0] == status;
    }

    @Override
    public void commit(long xid) {
        updateXIDStatus(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        updateXIDStatus(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public boolean isActive(long xid) {
        if (xid == SUPER_XID) {
            return false;
        }
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID) {
            return false;
        }
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if (xid == SUPER_XID) {
            return false;
        }
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        try {
            fileChannel.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
