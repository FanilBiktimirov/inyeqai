package com.inyeqai.tunnel.client;

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

import com.inyeqai.tunnel.common.ForwardSpec;
import com.inyeqai.tunnel.common.Frames;
import com.inyeqai.tunnel.common.Reverse;

/**
 * The reconnect case that used to leave a reverse forward dead: a client disappears without
 * closing its TCP connection, so the server still believes the session is alive and keeps
 * that session's reverse port bound. When the same client comes back, it has to be able to
 * bind the port again.
 *
 * <p>The pong timeout here is deliberately far longer than the test, so the periodic reaper
 * cannot be what frees the port. Only the check performed when a new client asks for a port
 * an unresponsive session is holding can make this pass.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"tunnel.keepalive=1s", "tunnel.pong-timeout=600s"})
class TunnelStaleSessionTest {

    @LocalServerPort
    int serverPort;

    @Test
    @Timeout(90)
    void aNewClientTakesOverAReversePortFromAnUnresponsiveSession() throws Exception {
        try (EchoServer echo = EchoServer.echoing()) {
            int reversePort = freePort();
            Reverse reverse = ForwardSpec.parse("R:" + reversePort + ":127.0.0.1:" + echo.port()).toReverse();

            try (MuteWebSocketClient zombie =
                         new MuteWebSocketClient("127.0.0.1", serverPort, "/tunnel", null)) {
                zombie.sendBinary(Frames.config(List.of(reverse)));
                awaitListening(reversePort);

                // Let the session miss a keepalive round so the server can tell it is gone.
                // It never answers a ping, which is exactly what a suspended host looks like.
                Thread.sleep(4_000);

                ClientConnection conn =
                        new ClientConnection(url(), null, 1, List.of(), List.of(reverse));
                Thread t = runAsync(conn);
                try {
                    assertEquals("after-takeover", roundTrip(reversePort, "after-takeover"),
                            "the reconnecting client should get its reverse port back");
                } finally {
                    conn.stop();
                    t.join(5_000);
                }
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

    private static void awaitListening(int port) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            try (Socket s = new Socket("127.0.0.1", port)) {
                return;
            } catch (IOException e) {
                Thread.sleep(100);
            }
        }
        fail("the server never opened reverse port " + port);
    }

    private static String roundTrip(int port, String message) throws Exception {
        // The takeover happens when the new client's CONFIG arrives, so the port may be
        // briefly closed between the old listener dying and the new one binding.
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            try (Socket sock = new Socket("127.0.0.1", port)) {
                sock.setSoTimeout(10_000);
                OutputStream out = sock.getOutputStream();
                out.write((message + "\n").getBytes(StandardCharsets.UTF_8));
                out.flush();
                InputStream in = sock.getInputStream();
                StringBuilder sb = new StringBuilder();
                int c;
                while ((c = in.read()) != -1 && c != '\n') {
                    sb.append((char) c);
                }
                if (!sb.isEmpty()) {
                    return sb.toString();
                }
            } catch (IOException e) {
                // fall through to retry
            }
            if (System.currentTimeMillis() > deadline) {
                fail("reverse port " + port + " never served the reconnecting client");
            }
            Thread.sleep(200);
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
