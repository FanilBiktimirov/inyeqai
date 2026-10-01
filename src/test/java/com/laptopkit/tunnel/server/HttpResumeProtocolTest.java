package com.laptopkit.tunnel.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import com.laptopkit.tunnel.common.Frames;
import com.laptopkit.tunnel.common.Framing;
import com.laptopkit.tunnel.common.Reverse;

/**
 * Правила восстановления сессии, прогоняемые вручную поверх HTTP, чтобы каждое можно было
 * нарушить намеренно.
 *
 * <p>Это те случаи, где ошибка проходит незаметно. Повторённая пачка, применённая дважды,
 * удваивает байты внутри потока; пропущенная пачка их теряет; восстановление с точки, которую
 * сервер уже не может воспроизвести, оставляет дырку. Ничего из этого не всплывает в туннеле
 * как ошибка — байты просто выходят на дальнем конце неправильными, поэтому каждый такой
 * случай обязан получить здесь громкий отказ, а не молчаливое прощение.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class HttpResumeProtocolTest {

    @LocalServerPort
    int serverPort;

    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1).build();

    private ByteSink sink;

    @AfterEach
    void closeSink() throws IOException {
        if (sink != null) {
            sink.close();
        }
    }

    @Test
    @Timeout(60)
    void aRetriedBatchIsAnsweredButNotAppliedAgain() throws Exception {
        sink = new ByteSink();
        String id = connect();

        // Один поток с пятью байтами, а затем тот же самый запрос ещё раз — так его пришлёт
        // клиент, который не увидел нашего ответа.
        byte[] batch = frames(
                Frames.open(1, "127.0.0.1", sink.port()),
                Frames.data(1, "hello".getBytes(StandardCharsets.UTF_8), 0, 5));
        assertEquals(204, postBatch(id, 1, batch).statusCode());
        awaitStatusContains("from clients 5 B");

        assertEquals(204, postBatch(id, 1, batch).statusCode(),
                "a retry must be answered, since the client could not tell our answer was lost");

        // Дальше идёт настоящая вторая пачка — ещё три байта. Ждать ровно «8 B» надёжнее, чем
        // подождать полсекунды и убедиться, что счётчик не вырос: применённый повтор дал бы 13,
        // и восьмёрки не случилось бы никогда. Проверка через паузу зеленела бы ложно каждый
        // раз, когда применение повтора просто задержалось дольше паузы.
        assertEquals(204, postBatch(id, 2,
                frames(Frames.data(1, "xyz".getBytes(StandardCharsets.UTF_8), 0, 3))).statusCode());
        awaitStatusContains("from clients 8 B");
    }

    @Test
    @Timeout(60)
    void aBatchThatSkipsOneEndsTheSession() throws Exception {
        sink = new ByteSink();
        String id = connect();
        assertEquals(204, postBatch(id, 1, frames(Frames.ping())).statusCode());

        // Пачка 2 не доезжает никогда. Продолжить с третьей — значит молча потерять всё, что
        // в ней было, поэтому вместо этого сессия обязана закончиться.
        HttpResponse<String> res = postBatch(id, 3, frames(Frames.ping()));
        assertEquals(410, res.statusCode(), res.body());
        awaitSessionGone(id);
    }

    @Test
    @Timeout(60)
    void resumingFromAFrameThatWasNeverSentEndsTheSession() throws Exception {
        String id = connect();
        try (InputStream down = attach(id, 0)) {
            assertNotNull(Framing.readSequenced(Framing.reader(down)), "expected the greeting");
        }
        // Столько кадров и близко нет; клиент, который утверждает обратное, потерял счёт тому,
        // где он находится, и всё, что мы отправим ему с этого места, будет гаданием.
        assertEquals(410, attachStatus(id, 9_999));
        awaitSessionGone(id);
    }

    @Test
    @Timeout(60)
    void aSessionOutlivesItsDownstreamResponseAndKeepsItsReverseListener() throws Exception {
        int reversePort = freePort();
        String id = connect();

        long lastSeq;
        try (InputStream down = attach(id, 0)) {
            lastSeq = Framing.readSequenced(Framing.reader(down)).seq();
            postBatch(id, 1, frames(Frames.config(
                    List.of(new Reverse("127.0.0.1", reversePort, "127.0.0.1", 9)))));
            awaitBound(reversePort);
        } // ответ заканчивается здесь — ровно так, как его оборвал бы шлюз с лимитом

        // Целую секунду потока вниз нет вообще, потом возвращаемся за остатком. Слушатель,
        // который держала сессия, обязан остаться на месте: продолжить сессию, а не собрать
        // её заново — это разница между сохранением её потоков и их потерей.
        Thread.sleep(1_000);
        assertTrue(portIsBound(reversePort),
                "the reverse listener should outlive the response that asked for it");

        try (InputStream down = attach(id, lastSeq)) {
            assertTrue(status().contains("session " + id), status());
            assertTrue(portIsBound(reversePort), "the listener should still be there after resuming");
        }
    }

    /** Назначение, которое принимает соединения и вычитывает всё, что приходит. */
    private static final class ByteSink implements AutoCloseable {
        private final ServerSocket listener;

        ByteSink() throws IOException {
            listener = new ServerSocket();
            listener.bind(new InetSocketAddress("127.0.0.1", 0));
            Thread t = new Thread(() -> {
                while (!listener.isClosed()) {
                    try {
                        Socket s = listener.accept();
                        Thread reader = new Thread(() -> {
                            try (Socket open = s) {
                                byte[] buf = new byte[4096];
                                while (open.getInputStream().read(buf) != -1) {
                                    // смысл только в том, чтобы байты забирались
                                }
                            } catch (IOException ignored) {
                            }
                        }, "sink-reader");
                        reader.setDaemon(true);
                        reader.start();
                    } catch (IOException e) {
                        return;
                    }
                }
            }, "sink-accept");
            t.setDaemon(true);
            t.start();
        }

        int port() {
            return listener.getLocalPort();
        }

        @Override
        public void close() throws IOException {
            listener.close();
        }
    }

    private String connect() throws Exception {
        HttpResponse<String> res = http.send(
                HttpRequest.newBuilder(uri("/tunnel/http/connect"))
                        .POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, res.statusCode());
        return res.body().trim();
    }

    private InputStream attach(String id, long from) throws Exception {
        HttpResponse<InputStream> res = http.send(
                HttpRequest.newBuilder(uri("/tunnel/http/down/" + id + "?from=" + from)).GET().build(),
                HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(200, res.statusCode());
        return res.body();
    }

    private int attachStatus(String id, long from) throws Exception {
        HttpResponse<InputStream> res = http.send(
                HttpRequest.newBuilder(uri("/tunnel/http/down/" + id + "?from=" + from)).GET().build(),
                HttpResponse.BodyHandlers.ofInputStream());
        res.body().close();
        return res.statusCode();
    }

    private HttpResponse<String> postBatch(String id, long batch, byte[] body) throws Exception {
        return http.send(
                HttpRequest.newBuilder(uri("/tunnel/http/up/" + id + "?batch=" + batch))
                        .timeout(Duration.ofSeconds(10))
                        .header("Content-Type", "application/octet-stream")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static byte[] frames(byte[]... frames) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] frame : frames) {
            Framing.write(out, frame);
        }
        return out.toByteArray();
    }

    private String status() throws Exception {
        return http.send(HttpRequest.newBuilder(uri("/status")).build(),
                HttpResponse.BodyHandlers.ofString()).body();
    }

    /**
     * Ждёт, пока одна конкретная сессия исчезнет из {@code /status}. Привязка к id сессии здесь
     * намеренная: контекст общий с остальными тестами этого класса, поэтому проверка «не осталось
     * ни одной сессии» проверяла бы их, а не этот тест.
     */
    private void awaitSessionGone(String id) throws Exception {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            if (!status().contains("session " + id)) {
                return;
            }
            Thread.sleep(100);
        }
        fail("/status still shows session " + id + ":\n" + status());
    }

    private void awaitStatusContains(String text) throws Exception {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            if (status().contains(text)) {
                return;
            }
            Thread.sleep(100);
        }
        fail("/status never said " + text + ":\n" + status());
    }

    private static void awaitBound(int port) throws Exception {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            if (portIsBound(port)) {
                return;
            }
            Thread.sleep(100);
        }
        fail("the server never opened the reverse listener on " + port);
    }

    private static boolean portIsBound(int port) {
        try (Socket s = new Socket("127.0.0.1", port)) {
            return true;
        } catch (ConnectException e) {
            return false;
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
