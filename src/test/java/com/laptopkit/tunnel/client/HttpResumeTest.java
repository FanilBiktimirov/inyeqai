package com.laptopkit.tunnel.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import com.laptopkit.tunnel.common.ForwardSpec;

/**
 * Resumption is only worth having if a cut downstream costs nothing but a round trip. These tests
 * put a proxy in the way that severs the tunnel's downstream response every so often and then ask
 * for the one thing that cannot be faked: every byte back, in order.
 *
 * <p>A transfer larger than the retransmit buffer is the interesting case, because it means frames
 * really are being confirmed and dropped as it goes rather than simply all being kept. And the
 * assertion on the echoed bytes is what makes the test meaningful: a resumption that skipped a
 * frame, or replayed one twice, would corrupt the stream in a way nothing in the tunnel reports
 * &mdash; it would simply hand back different bytes.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class HttpResumeTest {

    @LocalServerPort
    int serverPort;

    @Test
    @Timeout(180)
    void bytesSurviveADownstreamThatIsCutOverAndOver() throws Exception {
        int size = 4 * 1024 * 1024;
        byte[] payload = new byte[size];
        new Random(4321).nextBytes(payload);

        try (EchoServer echo = EchoServer.echoing();
             // Cut every 256 KB: a 4 MB echo has to survive more than a dozen of them.
             DownstreamCappingProxy proxy =
                     DownstreamCappingProxy.inFrontOf(serverPort, 256 * 1024)) {

            int localPort = freePort();
            ForwardSpec local = ForwardSpec.parse(localPort + ":127.0.0.1:" + echo.port());
            ClientConnection conn = new ClientConnection(
                    "ws://127.0.0.1:" + proxy.port() + "/tunnel", null, 25,
                    List.of(local), List.of(), Transport.HTTP);
            Thread t = runAsync(conn);
            try (Socket sock = connect(localPort)) {
                sock.setSoTimeout(120_000);
                Thread writer = new Thread(() -> {
                    try {
                        OutputStream out = sock.getOutputStream();
                        out.write(payload);
                        out.flush();
                        sock.shutdownOutput();
                    } catch (IOException e) {
                        // the read side reports the real failure
                    }
                }, "bulk-writer");
                writer.setDaemon(true);
                writer.start();

                byte[] got = readFully(sock, size);
                writer.join(10_000);
                assertArrayEquals(payload, got,
                        "every byte should come back once, in order, across the cuts");
                assertTrue(proxy.cuts() > 1,
                        "the proxy should have cut the downstream repeatedly, but cut "
                                + proxy.cuts() + " time(s)");
            } finally {
                stop(conn, t);
            }
        }
    }

    /**
     * The streams themselves must not notice. A connection opened before a cut has to still be
     * usable after it: that is the difference between resuming a session and rebuilding one,
     * since a rebuilt tunnel drops every stream it was carrying.
     */
    @Test
    @Timeout(120)
    void aStreamOpenedBeforeACutKeepsWorkingAfterIt() throws Exception {
        try (EchoServer echo = EchoServer.echoing();
             DownstreamCappingProxy proxy = DownstreamCappingProxy.inFrontOf(serverPort, 64 * 1024)) {

            int localPort = freePort();
            ForwardSpec local = ForwardSpec.parse(localPort + ":127.0.0.1:" + echo.port());
            ClientConnection conn = new ClientConnection(
                    "ws://127.0.0.1:" + proxy.port() + "/tunnel", null, 25,
                    List.of(local), List.of(), Transport.HTTP);
            Thread t = runAsync(conn);
            try (Socket sock = connect(localPort)) {
                sock.setSoTimeout(60_000);
                assertEquals("before-the-cut", exchange(sock, "before-the-cut"));

                // Push enough through this same connection to trip the cap, then keep using it.
                byte[] filler = new byte[96 * 1024];
                new Random(7).nextBytes(filler);
                sock.getOutputStream().write(filler);
                sock.getOutputStream().flush();
                readFully(sock, filler.length);
                assertTrue(proxy.cuts() > 0, "the cap should have cut the downstream by now");

                assertEquals("after-the-cut", exchange(sock, "after-the-cut"),
                        "the same stream should still carry bytes once the downstream resumed");
            } finally {
                stop(conn, t);
            }
        }
    }

    private Thread runAsync(ClientConnection conn) {
        Thread t = new Thread(() -> {
            try {
                conn.run();
            } catch (Exception ignored) {
                // run() ends when the connection is stopped
            }
        });
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static void stop(ClientConnection conn, Thread t) throws InterruptedException {
        conn.stop();
        t.join(10_000);
    }

    private static String exchange(Socket sock, String message) throws IOException {
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

    private static Socket connect(int port) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
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

    private static byte[] readFully(Socket sock, int size) throws IOException {
        byte[] got = new byte[size];
        int read = 0;
        InputStream in = sock.getInputStream();
        while (read < size) {
            int n = in.read(got, read, size - read);
            if (n < 0) {
                throw new IOException("stream ended after " + read + " of " + size + " bytes");
            }
            read += n;
        }
        return got;
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
