package com.laptopkit.tunnel.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import com.laptopkit.tunnel.common.ForwardSpec;
import com.laptopkit.tunnel.common.Reverse;

/**
 * The allow list is the difference between a tunnel and an open door into whatever the
 * server can reach. Here it permits exactly one address that no test actually uses, so
 * every real destination must be refused.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "tunnel.allow[0]=^127\\.0\\.0\\.1:1$")
class TunnelAllowListTest {

    @LocalServerPort
    int serverPort;

    @Test
    @Timeout(30)
    void aForwardStreamToADeniedDestinationIsClosed() throws Exception {
        try (EchoServer echo = EchoServer.echoing()) {
            int localPort = freePort();
            ForwardSpec local = ForwardSpec.parse(localPort + ":127.0.0.1:" + echo.port());
            ClientConnection conn = new ClientConnection(url(), null, 25, List.of(local), List.of());
            Thread t = runAsync(conn);
            try (Socket sock = connect(localPort)) {
                sock.setSoTimeout(20_000);
                sock.getOutputStream().write("knock\n".getBytes());
                sock.getOutputStream().flush();
                // The listener is local, so connecting always works; the refusal shows up as
                // the tunnelled stream being closed with nothing echoed back.
                assertEquals(-1, sock.getInputStream().read(),
                        "a denied destination must end the stream, not relay data");
            } finally {
                conn.stop();
                t.join(5_000);
            }
        }
    }

    @Test
    @Timeout(30)
    void aDeniedReverseListenerIsNeverOpened() throws Exception {
        try (EchoServer echo = EchoServer.echoing()) {
            int serverListen = freePort();
            Reverse reverse = ForwardSpec.parse("R:" + serverListen + ":127.0.0.1:" + echo.port()).toReverse();
            ClientConnection conn = new ClientConnection(url(), null, 25, List.of(), List.of(reverse));
            Thread t = runAsync(conn);
            try {
                // Give the server time to have acted on the CONFIG, then confirm the port
                // stayed shut rather than being briefly open.
                Thread.sleep(2_000);
                assertThrows(ConnectException.class, () -> new Socket("127.0.0.1", serverListen).close(),
                        "the server must not bind a reverse port the allow list rejects");
            } finally {
                conn.stop();
                t.join(5_000);
            }
        }
    }

    private String url() {
        return "ws://127.0.0.1:" + serverPort + "/tunnel";
    }

    private Thread runAsync(ClientConnection conn) {
        Thread t = new Thread(() -> {
            try {
                conn.run();
            } catch (Exception ignored) {
            }
        });
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static Socket connect(int port) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while (true) {
            try {
                return new Socket("127.0.0.1", port);
            } catch (ConnectException e) {
                if (System.currentTimeMillis() > deadline) {
                    fail("tunnel entry port " + port + " never opened");
                }
                Thread.sleep(100);
            }
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
