package com.inyeqai.tl.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import com.inyeqai.tl.common.ForwardSpec;
import com.inyeqai.tl.common.Reverse;

/**
 * {@code GET /status} is the runtime view of the tunnel, so the things worth asserting are
 * that it is actually gated by the token and that the numbers it reports move when real
 * traffic goes through.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "tl.auth=TL:statustest")
class StatusEndpointTest {

    private static final String TOKEN = "TL:statustest";

    @LocalServerPort
    int serverPort;

    @Test
    void theTokenIsRequired() throws Exception {
        assertEquals(401, status(null).statusCode(), "no token must not reveal the TL's state");
        assertEquals(401, status("TL:wrong").statusCode());
        assertEquals(200, status(TOKEN).statusCode());
    }

    @Test
    void anIdleServerSaysSo() throws Exception {
        String body = status(TOKEN).body();
        assertTrue(body.contains("no clients connected"), body);
        assertTrue(body.contains("sessions 0"), body);
    }

    @Test
    @Timeout(60)
    void aLiveStreamAndItsBytesShowUp() throws Exception {
        try (EchoServer echo = EchoServer.echoing()) {
            int localPort = freePort();
            int reversePort = freePort();
            ForwardSpec local = ForwardSpec.parse(localPort + ":127.0.0.1:" + echo.port());
            Reverse reverse = ForwardSpec.parse("R:" + reversePort + ":127.0.0.1:" + echo.port()).toReverse();
            ClientConnection conn =
                    new ClientConnection(url(), TOKEN, 25, List.of(local), List.of(reverse));
            Thread t = runAsync(conn);
            try {
                // Hold a connection open through the forward so a live stream is on display.
                try (Socket held = connect(localPort)) {
                    held.setSoTimeout(15_000);
                    OutputStream out = held.getOutputStream();
                    out.write("ping\n".getBytes(StandardCharsets.UTF_8));
                    out.flush();
                    assertEquals("ping", readLine(held), "sanity: the TL carries bytes");

                    String body = status(TOKEN).body();
                    assertTrue(body.contains("sessions 1"), body);
                    assertTrue(body.contains("reverse listeners: [" + reversePort + "]"), body);
                    assertTrue(body.contains("streams open 1"), body);
                    assertTrue(body.contains("127.0.0.1:" + echo.port()),
                            "the live stream should name its destination:\n" + body);
                    assertFalse(body.contains("from clients 0 B"),
                            "bytes from the client should have been counted:\n" + body);
                    assertFalse(body.contains("to clients 0 B"),
                            "bytes echoed back to the client should have been counted:\n" + body);

                    // Pin the per-stream line. It used to be built as text and parsed back
                    // apart on spaces, which mangled every byte count into the next column.
                    Pattern streamLine = Pattern.compile(
                            "^ {4}(\\d+) +127\\.0\\.0\\.1:" + echo.port()
                                    + " +open +\\S+ +sent +\\d+(\\.\\d+)? [A-Z]+ +received +\\d+(\\.\\d+)? [A-Z]+$",
                            Pattern.MULTILINE);
                    Matcher m = streamLine.matcher(body);
                    assertTrue(m.find(), "the live stream line should be well formed:\n" + body);
                    assertFalse(m.group(1).startsWith("-"), "stream ids must read unsigned");
                }
            } finally {
                conn.stop();
                t.join(5_000);
            }
        }
    }

    private HttpResponse<String> status(String token) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + serverPort + "/status"));
        if (token != null) {
            b.header("X-TL-Auth", token);
        }
        return HttpClient.newHttpClient().send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private String url() {
        return "ws://127.0.0.1:" + serverPort + "/tl";
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
                    fail("TL entry port " + port + " never opened");
                }
                Thread.sleep(100);
            }
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

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
