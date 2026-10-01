package com.laptopkit.tunnel.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
import java.util.Random;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import com.laptopkit.tunnel.common.ForwardSpec;
import com.laptopkit.tunnel.common.Reverse;

/**
 * Boots the real Spring tunnel server on a random port and drives a real
 * {@link ClientConnection} against it, verifying bytes survive a round trip through the
 * WebSocket in both the local and reverse directions.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TunnelEndToEndTest {

    @LocalServerPort
    int serverPort;

    private EchoServer echo;

    @BeforeEach
    void startEcho() throws IOException {
        echo = EchoServer.echoing();
    }

    @AfterEach
    void stopEcho() throws IOException {
        echo.close();
    }

    @Test
    void localForwardCarriesBytes() throws Exception {
        int localPort = freePort();
        ForwardSpec local = ForwardSpec.parse(localPort + ":127.0.0.1:" + echo.port());
        ClientConnection conn = new ClientConnection(url(), null, 25, List.of(local), List.of());
        Thread t = runAsync(conn);
        try {
            assertEquals("hello-local", roundTrip(localPort, "hello-local"));
        } finally {
            stop(conn, t);
        }
    }

    @Test
    void reverseForwardCarriesBytes() throws Exception {
        int serverListen = freePort();
        Reverse reverse = ForwardSpec.parse("R:" + serverListen + ":127.0.0.1:" + echo.port()).toReverse();
        ClientConnection conn = new ClientConnection(url(), null, 25, List.of(), List.of(reverse));
        Thread t = runAsync(conn);
        try {
            assertEquals("hello-reverse", roundTrip(serverListen, "hello-reverse"));
        } finally {
            stop(conn, t);
        }
    }

    /**
     * A client that stops writing but keeps reading must get its answer. Before the EOF
     * frame existed, the end of one direction tore the whole stream down and this reply
     * never arrived.
     */
    @Test
    @Timeout(30)
    void halfCloseLetsTheDestinationReplyAfterEof() throws Exception {
        try (EchoServer counting = EchoServer.countingUntilEof()) {
            int localPort = freePort();
            ForwardSpec local = ForwardSpec.parse(localPort + ":127.0.0.1:" + counting.port());
            ClientConnection conn = new ClientConnection(url(), null, 25, List.of(local), List.of());
            Thread t = runAsync(conn);
            try (Socket sock = connect(localPort)) {
                sock.setSoTimeout(20_000);
                byte[] payload = "0123456789".getBytes(StandardCharsets.UTF_8);
                sock.getOutputStream().write(payload);
                sock.getOutputStream().flush();
                sock.shutdownOutput();

                assertEquals("got " + payload.length, readLine(sock),
                        "the destination should see EOF and still be able to answer");
            } finally {
                stop(conn, t);
            }
        }
    }

    /** More bytes than one flow-control window, to prove credit is returned and order kept. */
    @Test
    @Timeout(120)
    void bulkTransferSurvivesFlowControl() throws Exception {
        int size = 4 * 1024 * 1024;
        byte[] payload = new byte[size];
        new Random(1234).nextBytes(payload);

        int localPort = freePort();
        ForwardSpec local = ForwardSpec.parse(localPort + ":127.0.0.1:" + echo.port());
        ClientConnection conn = new ClientConnection(url(), null, 25, List.of(local), List.of());
        Thread t = runAsync(conn);
        try (Socket sock = connect(localPort)) {
            sock.setSoTimeout(60_000);
            // Write on another thread: 4 MB does not fit in any socket buffer, so writing
            // and reading have to overlap or both ends would block forever.
            Thread writer = new Thread(() -> {
                try {
                    OutputStream out = sock.getOutputStream();
                    out.write(payload);
                    out.flush();
                    sock.shutdownOutput();
                } catch (IOException e) {
                    // the assertion on the read side reports the real failure
                }
            }, "bulk-writer");
            writer.setDaemon(true);
            writer.start();

            byte[] got = readFully(sock, size);
            writer.join(10_000);
            assertArrayEquals(payload, got, "every byte should come back unchanged and in order");
        } finally {
            stop(conn, t);
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
                // run() ends when the connection is stopped
            }
        });
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static void stop(ClientConnection conn, Thread t) throws InterruptedException {
        conn.stop();
        t.join(5_000);
    }

    /** Connect to a tunnel entry port, retrying until its listener is up. */
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

    private static String roundTrip(int port, String message) throws Exception {
        try (Socket sock = connect(port)) {
            sock.setSoTimeout(10_000);
            OutputStream out = sock.getOutputStream();
            out.write((message + "\n").getBytes(StandardCharsets.UTF_8));
            out.flush();
            return readLine(sock);
        }
    }

    private static String readLine(Socket sock) throws IOException {
        InputStream in = sock.getInputStream();
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) != -1 && c != '\n') {
            sb.append((char) c);
        }
        return sb.toString();
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
