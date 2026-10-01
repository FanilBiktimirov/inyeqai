package com.inyeqai.tl.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import com.inyeqai.tl.common.ForwardSpec;

/**
 * The point of the client's health endpoint is that it reports the state of the tunnel, not
 * of the process. These tests therefore check the two answers a liveness probe cannot give
 * on its own: 503 while the process is perfectly alive but the tunnel is not, and 200 only
 * once the far end is actually answering.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ClientHealthTest {

    @LocalServerPort
    int serverPort;

    @Test
    @Timeout(60)
    void withNoConnectionItReportsDownWhileTheProcessIsFine() throws Exception {
        AtomicReference<ClientConnection> live = new AtomicReference<>();
        try (ClientHealth health = new ClientHealth("127.0.0.1", 0, live::get)) {
            HttpResponse<String> r = get(health.port(), "/healthz");
            assertEquals(503, r.statusCode(), "no TL must not read as healthy");
            assertEquals("down\n", r.body());

            String status = get(health.port(), "/status").body();
            assertTrue(status.contains("no connection to the server"), status);
        }
    }

    @Test
    @Timeout(60)
    void withALiveTLItReportsUpAndShowsTheForwards() throws Exception {
        try (EchoServer echo = EchoServer.echoing()) {
            int localPort = freePort();
            ForwardSpec local = ForwardSpec.parse(localPort + ":127.0.0.1:" + echo.port());
            ClientConnection conn = new ClientConnection(url(), null, 1, List.of(local), List.of());
            AtomicReference<ClientConnection> live = new AtomicReference<>(conn);
            Thread t = runAsync(conn);
            try (ClientHealth health = new ClientHealth("127.0.0.1", 0, live::get)) {
                awaitStatus(health.port(), 200);

                assertEquals("up\n", get(health.port(), "/healthz").body());
                String status = get(health.port(), "/status").body();
                assertTrue(status.contains("TL client: up"), status);
                // Which carrier is in use, so an operator can see that auto picked HTTP.
                assertTrue(status.contains("transport: websocket"), status);
                assertTrue(status.contains("last pong"), status);
                assertTrue(status.contains("forward 127.0.0.1:" + localPort), status);
                assertTrue(status.contains("127.0.0.1:" + echo.port()), status);

                // Dropping the tunnel has to flip the verdict, with the process untouched.
                conn.stop();
                t.join(5_000);
                HttpResponse<String> after = get(health.port(), "/healthz");
                assertEquals(503, after.statusCode(), "a stopped TL must read as down");
                assertEquals("down\n", after.body());
            } finally {
                conn.stop();
                t.join(5_000);
            }
        }
    }

    /** {@code --keepalive 0s} leaves no evidence either way; that must be said, not faked. */
    @Test
    @Timeout(60)
    void withKeepaliveOffItSaysTheCheckIsUnavailable() throws Exception {
        try (EchoServer echo = EchoServer.echoing()) {
            int localPort = freePort();
            ForwardSpec local = ForwardSpec.parse(localPort + ":127.0.0.1:" + echo.port());
            ClientConnection conn = new ClientConnection(url(), null, 0, List.of(local), List.of());
            AtomicReference<ClientConnection> live = new AtomicReference<>(conn);
            Thread t = runAsync(conn);
            try (ClientHealth health = new ClientHealth("127.0.0.1", 0, live::get)) {
                awaitStatus(health.port(), 200);
                assertEquals("unknown\n", get(health.port(), "/healthz").body());
                String status = get(health.port(), "/status").body();
                assertTrue(status.contains("keepalive is off"), status);
            } finally {
                conn.stop();
                t.join(5_000);
            }
        }
    }

    /**
     * The mapping from verdict to HTTP code is the whole contract a probe relies on, and
     * STALE is the one case an integration test cannot easily stage: the client's own
     * watchdog tears the link down within a keepalive tick of the pong going stale, so the
     * window where a probe can observe it is narrow. Pin the codes directly.
     */
    @Test
    void everyVerdictMapsToTheRightCode() {
        assertEquals(200, ClientHealth.Verdict.UP.code);
        assertEquals(200, ClientHealth.Verdict.UNKNOWN.code, "keepalive off is not a failure");
        assertEquals(503, ClientHealth.Verdict.STALE.code, "a peer that stopped answering is not healthy");
        assertEquals(503, ClientHealth.Verdict.DOWN.code);
    }

    @Test
    void bindSpecParsing() {
        assertEquals(new TLClient.HostPort("127.0.0.1", 9000), TLClient.HostPort.parse("9000"));
        assertEquals(new TLClient.HostPort("0.0.0.0", 9000), TLClient.HostPort.parse("0.0.0.0:9000"));
        assertEquals(new TLClient.HostPort("::1", 9000), TLClient.HostPort.parse("[::1]:9000"));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> TLClient.HostPort.parse("0.0.0.0:0"));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> TLClient.HostPort.parse("nope"));
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

    private static void awaitStatus(int port, int expected) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            if (get(port, "/healthz").statusCode() == expected) {
                return;
            }
            Thread.sleep(100);
        }
        fail("health never reported " + expected);
    }

    private static HttpResponse<String> get(int port, String path) throws Exception {
        return HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
