package org.jyafoo.mydb.backend.utils;

/**
 * 系统级别异常处理
 * @author jyafoo
 * @since 2024/9/28
 */
public class Panic {
    /**
     * 处理系统异常情况，包括打印异常信息并退出系统
     * 使用此方法会导致程序非正常终止，适用于无法恢复的严重错误场景
     *
     * @param err 异常对象，表示触发panic的原因
     */
    public static void panic(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }
}
