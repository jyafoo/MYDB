package org.jyafoo.mydb.transport;

import org.junit.Test;
import org.jyafoo.mydb.backend.utils.Panic;

import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author jyafoo
 * @since 2024/10/8
 */
public class PackagerTest {
    @Test
    public void testPackager() throws Exception {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    ServerSocket ss = new ServerSocket(10145);
                    Socket socket = ss.accept();
                    Transporter t = new Transporter(socket);
                    Encoder e = new Encoder();
                    Packager p = new Packager(t, e);
                    Package one = p.receive();
                    assert "pkg1 test".equals(new String(one.getData()));
                    Package two = p.receive();
                    assert "pkg2 test".equals(new String(two.getData()));
                    p.send(new Package("pkg3 test".getBytes(), null));
                    ss.close();
                } catch (Exception e) {
                    Panic.panic(e);
                }
            }
        }).start();
        Thread.sleep(1000);
        Socket socket = new Socket("127.0.0.1", 10145);
        Transporter t = new Transporter(socket);
        Encoder e = new Encoder();
        Packager p = new Packager(t, e);
        p.send(new Package("pkg1 test".getBytes(), null));
        p.send(new Package("pkg2 test".getBytes(), null));
        Package three = p.receive();
        assert "pkg3 test".equals(new String(three.getData()));
    }
}
