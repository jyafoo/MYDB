package org.jyafoo.mydb.backend.tbm;

import org.jyafoo.mydb.backend.utils.Panic;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.jyafoo.mydb.common.Error;

/**
 * 记录第一张表的uid
 *
 * @author jyafoo
 * @since 2024/10/6
 */
public class Booter {
    public static final String BOOTER_SUFFIX = ".bt";
    public static final String BOOTER_TMP_SUFFIX = ".bt_tmp";

    String path;
    File file;

    private Booter(String path, File file) {
        this.path = path;
        this.file = file;
    }

    /**
     * 创建一个Booter实例，用于处理给定路径下的引导文件
     * <p>
     * 此方法首先确保指定路径下不存在同名文件，然后创建该文件，并确认文件具有读写权限
     *
     * @param path 引导文件的路径，不包括文件名
     * @return 返回新创建的Booter实例
     */
    public static Booter create(String path) {
        removeBadTmp(path);

        File file = new File(path + BOOTER_SUFFIX);
        try {
            if (!file.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }

        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        return new Booter(path, file);
    }

    /**
     * 打开一个Booter
     * <p>
     * 此方法首先会检查给定路径是否存在，并确保文件具有读写权限
     * 如果路径不存在或文件不可读写，则会抛出异常
     *
     * @param path 文件路径
     * @return 返回一个新的Booter实例
     */
    public static Booter open(String path) {
        removeBadTmp(path);
        File f = new File(path + BOOTER_SUFFIX);
        if (!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }

        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        return new Booter(path, f);
    }

    /**
     * 删除指定路径下的不良临时文件
     * <p>
     * 该方法主要用于清理系统在运行过程中生成的、不再需要的临时文件，以保持系统的稳定和性能
     *
     * @param path 文件路径，表示要删除的临时文件的目录
     */
    private static void removeBadTmp(String path) {
        new File(path + BOOTER_TMP_SUFFIX).delete();
    }

    /**
     * 加载文件内容到字节数组
     * <p>
     * 此方法尝试将文件的内容读取到一个字节数组中如果文件无法读取或发生其他IO错误，
     * 则会抛出自定义的异常处理方法
     *
     * @return 文件内容的字节数组如果文件读取失败，则返回null
     */
    public byte[] load() {
        byte[] buf = null;
        try {
            buf = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf;
    }

    /**
     * 更新启动器文件
     * <p>
     * 本方法通过覆盖临时文件的方式更新启动器文件，确保更新过程的原子性和安全性
     * 它首先创建一个临时文件，检查其读写权限，然后写入新数据，并将其重命名为启动器文件
     * 如果在任何步骤出现异常，系统将抛出异常并停止执行
     *
     * @param data 包含更新后启动器数据的字节数组
     */
    public void update(byte[] data) {
        // 创建临时文件路径
        File tmp = new File(path + BOOTER_TMP_SUFFIX);

        try {
            // 创建临时文件
            tmp.createNewFile();
        } catch (Exception e) {
            // 如果创建失败，抛出异常并停止执行
            Panic.panic(e);
        }

        // 检查临时文件的读写权限
        if (!tmp.canRead() || !tmp.canWrite()) {
            // 如果文件权限不足，抛出异常并停止执行
            Panic.panic(Error.FileCannotRWException);
        }

        // 将数据写入临时文件
        try (FileOutputStream out = new FileOutputStream(tmp)) {
            out.write(data);
            out.flush();
        } catch (IOException e) {
            // 如果写入过程中发生错误，抛出异常并停止执行
            Panic.panic(e);
        }

        // 将临时文件重命名为启动器文件，如果文件已存在，则替换
        try {
            Files.move(tmp.toPath(), new File(path + BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            // 如果重命名过程中发生错误，抛出异常并停止执行
            Panic.panic(e);
        }

        // 更新启动器文件引用
        file = new File(path + BOOTER_SUFFIX);

        // 检查更新后的启动器文件读写权限
        if (!file.canRead() || !file.canWrite()) {
            // 如果文件权限不足，抛出异常并停止执行
            Panic.panic(Error.FileCannotRWException);
        }
    }

}
