package com.laptopkit.tunnel.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import com.laptopkit.tunnel.common.Frames;
import com.laptopkit.tunnel.common.Framing;
import com.laptopkit.tunnel.common.Reverse;

/**
 * Тот же отказ, на котором проверяется WebSocket-транспорт, но на HTTP: клиент перестаёт
 * отвечать, ничего не закрывая. Усыплённый ноутбук оставляет сервер с TCP-соединением, на другом
 * конце которого никого нет, а вместе с ним — с обратными портами, которые заняла та сессия.
 *
 * <p>На HTTP-транспорте закрытия TCP тоже не дождёшься, и ту же работу должен сделать keepalive
 * обычными кадрами. Тест дёргает эндпойнты вручную, без всякого клиента, поэтому сессия
 * по-настоящему молчит: открывает поток вниз, просит обратный слушатель и больше не отвечает
 * ни на что.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"tunnel.keepalive=1s", "tunnel.pong-timeout=2s"})
class HttpStaleSessionTest {

    @LocalServerPort
    int serverPort;

    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1).build();

    @Test
    @Timeout(90)
    void aMuteHttpSessionStopsHoldingItsReversePort() throws Exception {
        int reversePort = freePort();

        HttpResponse<String> connect = http.send(
                HttpRequest.newBuilder(uri("/tunnel/http/connect"))
                        .POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, connect.statusCode());
        String id = connect.body().trim();

        // Открываем поток вниз и оставляем открытым. Назад ничего не постится, поэтому до
        // сервера не доходит ни один pong: именно так отсюда выглядит исчезнувший клиент.
        HttpResponse<InputStream> down = http.send(
                HttpRequest.newBuilder(uri("/tunnel/http/down/" + id)).GET().build(),
                HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(200, down.statusCode());

        try (InputStream ignored = down.body()) {
            askForReverseListener(id, reversePort);
            awaitBound(reversePort);

            // За таймаутом pong сессию обязаны бросить, а вместе с ней отпустить и её порт.
            Thread.sleep(5_000);
            assertTrue(portIsFree(reversePort),
                    "the reverse port should be released once the session stops answering");
        }
    }

    private void askForReverseListener(String id, int reversePort) throws Exception {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        Framing.write(body, Frames.config(
                List.of(new Reverse("127.0.0.1", reversePort, "127.0.0.1", 9)))); // 9 — discard
        HttpResponse<Void> res = http.send(
                HttpRequest.newBuilder(uri("/tunnel/http/up/" + id + "?batch=1"))
                        .timeout(Duration.ofSeconds(10))
                        .header("Content-Type", "application/octet-stream")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body.toByteArray())).build(),
                HttpResponse.BodyHandlers.discarding());
        assertEquals(204, res.statusCode());
    }

    private static void awaitBound(int port) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            try (Socket s = new Socket("127.0.0.1", port)) {
                return;
            } catch (ConnectException e) {
                Thread.sleep(100);
            }
        }
        fail("the server never opened the reverse listener on " + port);
    }

    /** Доказательство — занять порт самим: отказ в соединении может быть просто гонкой. */
    private static boolean portIsFree(int port) {
        try (ServerSocket s = new ServerSocket(port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private URI uri(String path) {
        return URI.create("http://127.0.0.1:" + serverPort + path);
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
