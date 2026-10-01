package com.laptopkit.tunnel.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import com.laptopkit.tunnel.common.ForwardSpec;

/**
 * Keepalive has to work in both directions, and the only honest way to check it is to let
 * an idle tunnel sit for longer than both timeouts and then use it.
 *
 * <p>With these settings the client tears the tunnel down after 2s without a pong and the
 * server closes a session after 3s without one. A tunnel that still carries bytes after
 * several idle seconds therefore proves two things at once: the container answers the
 * client's pings, and the JDK client answers the server's. Either half missing would show
 * up here as a dropped tunnel rather than a silent regression in production.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"tunnel.keepalive=1s", "tunnel.pong-timeout=3s"})
class TunnelKeepaliveTest {

    @LocalServerPort
    int serverPort;

    @Test
    @Timeout(60)
    void anIdleTunnelSurvivesBothPongTimeouts() throws Exception {
        try (EchoServer echo = EchoServer.echoing()) {
            int localPort = freePort();
            ForwardSpec local = ForwardSpec.parse(localPort + ":127.0.0.1:" + echo.port());
            ClientConnection conn = new ClientConnection(url(), null, 1, List.of(local), List.of());
            Thread t = runAsync(conn);
            try {
                assertEquals("before", roundTrip(localPort, "before"));
                // Six idle seconds: twice the server's pong timeout, three times the client's.
                Thread.sleep(6_000);
                assertEquals("after", roundTrip(localPort, "after"),
                        "the tunnel should still be up after idling past both pong timeouts");
            } finally {
                conn.stop();
                t.join(5_000);
            }
        }
    }

    /** chisel documents {@code 0s} as "no keepalive"; it must not take the client down. */
    @Test
    @Timeout(60)
    void keepaliveOffStillCarriesTraffic() throws Exception {
        try (EchoServer echo = EchoServer.echoing()) {
            int localPort = freePort();
            ForwardSpec local = ForwardSpec.parse(localPort + ":127.0.0.1:" + echo.port());
            ClientConnection conn = new ClientConnection(url(), null, 0, List.of(local), List.of());
            Thread t = runAsync(conn);
            try {
                assertEquals("quiet", roundTrip(localPort, "quiet"));
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

    private static String roundTrip(int port, String message) throws Exception {
        try (Socket sock = connect(port)) {
            sock.setSoTimeout(15_000);
            OutputStream out = sock.getOutputStream();
            out.write((message + "\n").getBytes(StandardCharsets.UTF_8));
            out.flush();
            InputStream in = sock.getInputStream();
            StringBuilder sb = new StringBuilder();
            int c;
            while ((c = in.read()) != -1 && c != '\n') {
                sb.append((char) c);
            }
            return sb.toString();
        }
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
