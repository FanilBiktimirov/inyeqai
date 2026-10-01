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
 * Случай переподключения, после которого обратный проброс раньше оставался мёртвым: клиент
 * исчезает, не закрыв своё TCP-соединение, поэтому сервер по-прежнему считает сессию живой и
 * держит занятым её обратный порт. Когда тот же клиент возвращается, он обязан снова суметь
 * занять этот порт.
 *
 * <p>Таймаут на pong здесь нарочно сделан гораздо длиннее самого теста, чтобы порт не мог
 * освободил периодический отстрел замолчавших сессий. Пройти этот тест можно только за счёт проверки, которая
 * выполняется, когда новый клиент просит порт, занятый неотвечающей сессией.
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

                // Даём сессии пропустить круг keepalive, чтобы сервер понял: её больше нет.
                // Она никогда не отвечает на ping — ровно так выглядит уснувший хост.
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
        // Перехват происходит, когда приходит CONFIG нового клиента, так что между смертью
        // старого слушателя и привязкой нового порт может на мгновение оказаться закрыт.
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
                // проваливаемся на повторную попытку
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
