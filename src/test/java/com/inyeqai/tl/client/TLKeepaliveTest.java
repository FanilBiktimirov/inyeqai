package com.inyeqai.tl.client;

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

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import com.inyeqai.tl.common.ForwardSpec;

/**
 * Keepalive обязан работать в обе стороны, и единственный честный способ это проверить —
 * оставить простаивающий туннель лежать дольше обоих таймаутов, а потом им воспользоваться.
 *
 * <p>С этими настройками клиент рвёт туннель через 2 с без pong, а сервер закрывает сессию
 * через 3 с без него. Поэтому туннель, который всё ещё несёт байты после нескольких секунд
 * простоя, доказывает сразу две вещи: контейнер отвечает на ping клиента, а JDK-клиент — на
 * ping сервера. Пропажа любой из половин вылезла бы здесь оборванным туннелем, а не тихой
 * регрессией в продакшене.
 *
 * <p>Проверяются оба транспорта, потому что доказывают они это по-разному. У WebSocket есть
 * свои собственные кадры ping и pong; HTTP-транспорт вынужден нести свой keepalive обычными
 * кадрами и отвечать на них в своём транспортном слое, на обоих концах. Простаивающий
 * HTTP-туннель, который здесь выжил, и есть доказательство, что так и происходит, — и что
 * отстрел замолчавших сессий на сервере не уносит тихо живые HTTP-сессии из-за pong, которого
 * сервер так и не распознал.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"tl.keepalive=1s", "tl.pong-timeout=3s"})
class TLKeepaliveTest {

    @LocalServerPort
    int serverPort;

    @ParameterizedTest
    @EnumSource(Transport.class)
    @Timeout(60)
    void anIdleTLSurvivesBothPongTimeouts(Transport transport) throws Exception {
        try (EchoServer echo = EchoServer.echoing()) {
            int localPort = freePort();
            ForwardSpec local = ForwardSpec.parse(localPort + ":127.0.0.1:" + echo.port());
            ClientConnection conn =
                    new ClientConnection(url(), null, 1, List.of(local), List.of(), transport);
            Thread t = runAsync(conn);
            try {
                assertEquals("before", roundTrip(localPort, "before"));
                // Шесть секунд простоя: вдвое дольше таймаута сервера на pong, втрое — клиентского.
                Thread.sleep(6_000);
                assertEquals("after", roundTrip(localPort, "after"),
                        "the TL should still be up after idling past both pong timeouts");
            } finally {
                conn.stop();
                t.join(5_000);
            }
        }
    }

    /** chisel документирует {@code 0s} как «без keepalive»; это не должно ронять клиент. */
    @ParameterizedTest
    @EnumSource(Transport.class)
    @Timeout(60)
    void keepaliveOffStillCarriesTraffic(Transport transport) throws Exception {
        try (EchoServer echo = EchoServer.echoing()) {
            int localPort = freePort();
            ForwardSpec local = ForwardSpec.parse(localPort + ":127.0.0.1:" + echo.port());
            ClientConnection conn =
                    new ClientConnection(url(), null, 0, List.of(local), List.of(), transport);
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
                    fail("TL entry port " + port + " never opened");
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
