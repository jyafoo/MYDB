package org.jyafoo.mydb.backend.parse;

import org.jyafoo.mydb.common.Error;

/**
 * Tokenizer 类，对语句进行逐字节解析，根据空白符或者上述词法规则，将语句切割成多个 token。
 * <p>
 * 对外提供了 peek()、pop() 方法方便取出 Token 进行解析。切割的实现不赘述。
 *
 * @author jyafoo
 * @since 2024/10/6
 */
public class Tokenizer {
    /// 存储解析过程中的状态信息
    private byte[] stat;
    /// 当前处理的位置
    private int pos;
    /// 保存当前处理的标记（token）
    private String currentToken;
    /// 标记是否需要刷新当前的token
    private boolean flushToken;
    /// 异常对象，记录处理过程中遇到的异常
    private Exception err;

    public Tokenizer(byte[] stat) {
        this.stat = stat;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
    }

    /**
     * 获取当前的token
     * <p>
     * 如果在获取下一个token的过程中抛出异常，则将异常保存并再次抛出
     * 如果不需要刷新token，则返回当前的token
     *
     * @return 当前的token
     * @throws Exception 如果在获取下一个token时发生错误，则抛出异常
     */
    public String peek() throws Exception {
        // 如果有保存的异常，直接抛出
        if (err != null) {
            throw err;
        }
        // 如果需要刷新token
        if (flushToken) {
            String token = null;
            try {
                // 尝试获取下一个token
                token = next();
            } catch (Exception e) {
                // 如果发生异常，保存异常并抛出
                err = e;
                throw e;
            }
            // 更新当前的token，并设置不需要刷新token
            currentToken = token;
            flushToken = false;
        }
        // 返回当前的token
        return currentToken;
    }

    /**
     * 弹出栈顶元素
     * <p>
     * 此方法通过设置flushToken标志为true来标记需要弹出栈顶元素的动作
     * 调用此方法后，实际的弹出操作将在后续的处理中根据此标志执行
     */
    public void pop() {
        flushToken = true;
    }

    /**
     * 获取错误状态的字节数组表示
     * <p>
     * 此方法用于生成一个表示错误状态的字节数组，它在原始状态数据的当前位置之前添加一个特殊的前缀，
     * 并在当前位置之后添加剩余的状态数据，用于日志记录或错误分析
     *
     * @return 返回包含错误状态的字节数组
     */
    public byte[] errStat() {
        // 创建一个新字节数组，用于存储错误状态
        byte[] res = new byte[stat.length + 3];
        // 复制stat数组的前pos个元素到res数组的开头
        System.arraycopy(stat, 0, res, 0, pos);
        // 在res数组的pos位置添加<<"前缀，用于标记错误状态的当前位置
        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
        // 复制stat数组从pos位置开始的剩余元素到res数组的pos+3位置，跳过标记
        System.arraycopy(stat, pos, res, pos + 3, stat.length - pos);
        // 返回包含错误状态的字节数组
        return res;
    }


    /**
     * 移除统计数组中的一个字节
     * <p>
     * 此方法用于模拟从统计数组中移除一个字节的过程，它通过增加pos变量的值来实现
     * 如果pos的值超过了数组的长度，则将其设置为数组长度，确保不会访问到数组外部
     */
    private void popByte() {
        // 增加pos的值，代表移除一个字节
        pos++;
        // 检查pos是否超过了数组的长度
        if (pos > stat.length) {
            // 如果超过了数组长度，将其设置为数组的长度
            pos = stat.length;
        }
    }


    /**
     * 查看下一个字节，但不改变当前指针位置
     * <p>
     * 此方法用于在解析状态字符串时，预先查看下一个字节的内容，
     * 以便决定下一步的处理逻辑，而不实际移动指针位置
     *
     * @return 如果存在下一个字节，则返回该字节；如果当前指针已在字符串末尾，则返回null
     */
    private Byte peekByte() {
        // 检查是否已经到达状态字符串的末尾
        if (pos == stat.length) {
            // 如果已到达末尾，则返回null，表示没有下一个字节可以预览
            return null;
        }
        // 返回当前指针位置的字节，但不改变指针位置
        return stat[pos];
    }


    /**
     * 获取下一个状态
     * <p>
     * 此方法用于获取下一个元状态（meta-state）
     * 它首先检查是否存在未处理的错误（err），如果存在，则抛出错误，
     * 否则，调用nextMetaState()方法获取下一个元状态并返回
     *
     * @return 下一个元状态
     * @throws Exception 如果存在错误，则抛出异常
     */
    private String next() throws Exception {
        // 检查是否有未处理的错误，如果有，则抛出错误
        if (err != null) {
            throw err;
        }
        // 调用nextMetaState()方法获取下一个元状态并返回
        return nextMetaState();
    }

    /**
     * 获取下一个元状态
     * <p>
     * 此方法用于解析命令字符串中的下一个元状态它会根据当前 peek 的字节来决定是继续读取、跳过或返回错误
     * 元状态可以是符号、引号状态、标记或无效命令
     *
     * @return 根据不同的元状态，返回相应的字符串表示如果遇到文件末尾，则返回空字符串
     * @throws Exception 当命令无效时，抛出异常
     */
    private String nextMetaState() throws Exception {
        // 循环直到找到非空白字符或到达文件末尾
        while (true) {
            // 查看但不移除下一个字节
            Byte b = peekByte();
            // 如果为 null，表示到达文件末尾，返回空字符串
            if (b == null) {
                return "";
            }
            // 如果字节不是空白字符，则跳出循环
            if (!isBlank(b)) {
                break;
            }
            // 移除并返回当前字节，继续查找下一个非空白字符
            popByte();
        }
        // 再次查看但不移除下一个字节
        byte b = peekByte();
        // 根据不同的字节类型处理元状态
        if (isSymbol(b)) {
            // 如果是符号，则移除字节并返回该符号
            popByte();
            return new String(new byte[]{b});
        } else if (b == '"' || b == '\'') {
            // 如果是引号，则进入引号状态
            return nextQuoteState();
        } else if (isAlphaBeta(b) || isDigit(b)) {
            // 如果是字母或数字，则进入标记状态
            return nextTokenState();
        } else {
            // 如果以上条件都不满足，则抛出无效命令异常
            err = Error.InvalidCommandException;
            throw err;
        }
    }


    /**
     * 获取下一个状态标识符
     * <p>
     * 该方法用于从输入流中提取下一个状态标识符，直到遇到非字母、数字或下划线字符为止
     * 它会忽略空白字符，并将有效字符拼接成一个字符串后返回
     *
     * @return String 下一个状态标识符
     * @throws Exception 如果在读取字节时发生错误，或者输入流为空，则抛出异常
     */
    private String nextTokenState() throws Exception {
        StringBuilder sb = new StringBuilder();
        while (true) {
            Byte b = peekByte();
            if (b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
                if (b != null && isBlank(b)) {
                    popByte();
                }
                return sb.toString();
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
    }


    /**
     * 判断给定的字节是否为数字字符
     *
     * @param b 待检查的字节
     * @return 如果字节代表的字符是数字（'0'到'9'之间），则返回true；否则返回false
     */
    static boolean isDigit(byte b) {
        // 检查字节b是否在'0'和'9'的ASCII值之间
        return (b >= '0' && b <= '9');
    }


    /**
     * 判断给定的字节是否为字母
     * <p>
     * 该方法检查字节是否在小写字母'a'到'z'之间，或者在大写字母'A'到'Z'之间
     *
     * @param b 待检查的字节
     * @return 如果字节是字母，则返回true；否则返回false
     */
    static boolean isAlphaBeta(byte b) {
        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
    }


    /**
     * 读取下一个引号状态
     * <p>
     * 此方法用于处理字符串中的引号状态，直到找到匹配的结束引号
     * 它会抛出异常，如果在找到结束引号之前遇到了文件结束或无效字节
     *
     * @return 引号内的字符串
     * @throws Exception 如果在找到结束引号之前遇到了文件结束或无效字节
     */
    private String nextQuoteState() throws Exception {
        // 获取当前的引号字节
        byte quote = peekByte();
        // 将获取的引号字节移除队列
        popByte();
        // 使用StringBuilder来构建引号内的字符串
        StringBuilder sb = new StringBuilder();
        while (true) {
            // 获取下一个字节
            Byte b = peekByte();
            // 如果字节为null，表示文件结束或错误
            if (b == null) {
                // 设置错误信息
                err = org.jyafoo.mydb.common.Error.InvalidCommandException;
                // 抛出异常
                throw err;
            }
            // 如果找到匹配的结束引号
            if (b == quote) {
                // 将结束引号移除队列
                popByte();
                // 结束循环
                break;
            }
            // 将字节转换为字符串并添加到StringBuilder中
            sb.append(new String(new byte[]{b}));
            // 将处理过的字节移除队列
            popByte();
        }
        // 返回构建好的字符串
        return sb.toString();
    }


    /**
     * 判断给定的字节是否为符号
     * <p>
     * 此方法用于确定传入的字节值是否属于一组预定义的符号这些符号包括
     * 大于号（>）、小于号（<）、等于号（=）、乘号（*）、逗号（,）、
     * 左括号（(）和右括号（)）这些符号在某些上下文中可能有特殊意义，
     * 如HTML、SQL等因此，识别这些符号有助于在处理文本或数据时采取
     * 相应的处理逻辑
     *
     * @param b 待检查的字节值
     * @return 如果字节值是上述符号之一，则返回true；否则返回false
     */
    static boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' ||
                b == ',' || b == '(' || b == ')');
    }

    /**
     * 判断给定的字节是否为空白字符
     * <p>
     * 在文本处理中，了解一个字符是否为空白（如空格、制表符或换行符）很有用处
     * 这个方法可以帮助识别和处理文本中的空白字符，包括空格、制表符和换行符
     *
     * @param b 待检查的字节
     * @return 如果字节是空白字符（空格、制表符或换行符），则返回true；否则返回false
     */
    static boolean isBlank(byte b) {
        return (b == '\n' || b == ' ' || b == '\t');
    }
}
