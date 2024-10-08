package org.jyafoo.mydb.backend.server;

import org.jyafoo.mydb.backend.tbm.TableManager;
import org.jyafoo.mydb.transport.Encoder;
import org.jyafoo.mydb.transport.Packager;
import org.jyafoo.mydb.transport.Transporter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.jyafoo.mydb.transport.Package;

/**
 *
 *
 * @author jyafoo
 * @since 2024/10/8
 */
public class Server {
    private int port;
    TableManager tbm;

    public Server(int port, TableManager tbm) {
        this.port = port;
        this.tbm = tbm;
    }

    /**
     * 启动服务器，开始监听指定端口
     *
     * 该方法创建一个服务器套接字，等待客户端连接，并使用线程池来处理每个连接请求
     */
    public void start() {
        ServerSocket ss = null;
        try {
            // 创建ServerSocket对象，绑定到指定端口
            ss = new ServerSocket(port);
        } catch (IOException e) {
            // 如果发生IO异常，打印异常信息并返回
            e.printStackTrace();
            return;
        }
        // 打印服务器开始监听的信息
        System.out.println("Server listen to port: " + port);

        // 创建一个线程池，用于处理客户端连接请求
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(10, 20, 1L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());

        try {
            // 无限循环，等待并接受客户端连接
            while(true) {
                Socket socket = ss.accept();
                // 创建一个新的可运行任务来处理Socket连接
                Runnable worker = new HandleSocket(socket, tbm);
                // 将任务提交给线程池执行
                tpe.execute(worker);
            }
        } catch(IOException e) {
            // 如果发生IO异常，打印异常信息
            e.printStackTrace();
        } finally {
            // 试图关闭ServerSocket以释放资源
            try {
                ss.close();
            } catch (IOException ignored) {
                // 如果发生IO异常，选择忽略
            }
        }
    }

}

class HandleSocket implements Runnable {
    private Socket socket;
    private TableManager tbm;

    public HandleSocket(Socket socket, TableManager tbm) {
        this.socket = socket;
        this.tbm = tbm;
    }

    // 处理socket连接后的数据交换
    @Override
    public void run() {
        // 获取客户端的地址信息
        InetSocketAddress address = (InetSocketAddress)socket.getRemoteSocketAddress();
        // 打印连接信息，包括客户端IP地址和端口号
        System.out.println("Establish connection: " + address.getAddress().getHostAddress()+":"+address.getPort());

        // 初始化打包器对象，用于数据的打包和解包
        Packager packager = null;

        try {
            // 创建数据传输对象，负责数据的发送和接收
            Transporter t = new Transporter(socket);
            // 创建数据编码对象，负责数据的编码和解码
            Encoder e = new Encoder();
            // 初始化打包器，用于数据打包和解包，结合传输和编码功能
            packager = new Packager(t, e);
        } catch(IOException e) {
            // 异常处理：打印异常信息
            e.printStackTrace();
            try {
                // 尝试关闭socket连接
                socket.close();
            } catch (IOException e1) {
                // 异常处理：再次打印异常信息
                e1.printStackTrace();
            }
            // 退出run方法
            return;
        }

        // 创建执行器对象，负责执行数据库操作
        Executor exe = new Executor(tbm);

        // 循环处理数据交换
        while(true) {
            // 初始化数据包对象
            Package pkg = null;

            try {
                // 接收数据包
                pkg = packager.receive();
            } catch(Exception e) {
                // 异常处理：终止数据交换循环
                break;
            }

            // 获取数据包中的数据内容
            byte[] sql = pkg.getData();
            // 初始化返回结果数据和异常信息
            byte[] result = null;
            Exception e = null;

            try {
                // 执行数据库操作并获取结果
                result = exe.execute(sql);
            } catch (Exception e1) {
                // 异常处理：记录异常信息
                e = e1;
                // 打印异常信息
                e.printStackTrace();
            }

            // 创建包含执行结果或异常信息的数据包
            pkg = new Package(result, e);

            try {
                // 发送数据包给客户端
                packager.send(pkg);
            } catch (Exception e1) {
                // 异常处理：打印异常信息
                e1.printStackTrace();
                // 终止数据交换循环
                break;
            }
        }

        // 关闭执行器，释放资源
        exe.close();

        try {
            // 关闭打包器，释放资源
            packager.close();
        } catch (Exception e) {
            // 异常处理：打印异常信息
            e.printStackTrace();
        }
    }


}
