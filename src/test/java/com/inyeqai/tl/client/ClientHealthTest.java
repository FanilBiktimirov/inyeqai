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
 * Смысл health-эндпойнта клиента в том, что он показывает состояние туннеля, а не процесса.
 * Поэтому тесты проверяют два ответа, которые liveness-проба сама по себе дать не может:
 * 503, пока процесс совершенно жив, а туннеля нет, и 200 — только когда дальний конец
 * действительно отвечает.
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
                // Эндпойнт называет транспорт, который в работе. Здесь это websocket: туннель
                // поднят напрямую, без выбора по auto. Важно само наличие строки — когда
                // транспорт выберет автоматика, только она и покажет, что выбран был HTTP.
                assertTrue(status.contains("transport: websocket"), status);
                assertTrue(status.contains("last pong"), status);
                assertTrue(status.contains("forward 127.0.0.1:" + localPort), status);
                assertTrue(status.contains("127.0.0.1:" + echo.port()), status);

                // Обрыв туннеля обязан перевернуть вердикт, при том что процесс не тронут.
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

    /**
     * При {@code --keepalive 0s} доказательств нет ни за, ни против; это надо прямо сказать,
     * а не выдумывать ответ.
     */
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
     * Соответствие вердикта и HTTP-кода — весь контракт, на который опирается проба, а STALE —
     * единственный случай, который интеграционным тестом так просто не подстроить: сторожевой
     * таймер самого клиента рвёт связь в пределах одного такта keepalive после того, как pong
     * протух, так что окно, в котором проба успеет это увидеть, узкое. Поэтому коды
     * закрепляем напрямую.
     */
    @Test
    void everyVerdictMapsToTheRightCode() {
        assertEquals(200, ClientHealth.Verdict.UP.code);
        assertEquals(200, ClientHealth.Verdict.UNKNOWN.code, "keepalive off is not a failure");
        assertEquals(503, ClientHealth.Verdict.STALE.code, "a peer that stopped answering is not healthy");
        assertEquals(503, ClientHealth.Verdict.DOWN.code);
    }

    private String url() {
        return "ws://127.0.0.1:" + serverPort + "/TL";
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
