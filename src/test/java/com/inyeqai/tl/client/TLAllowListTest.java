package com.inyeqai.tl.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import com.inyeqai.tl.common.ForwardSpec;
import com.inyeqai.tl.common.Reverse;

/**
 * Список разрешённых адресов — это разница между туннелем и открытой дверью во всё, куда сервер
 * может достать. Здесь он разрешает ровно один адрес, которым ни один тест не пользуется, так
 * что любое настоящее направление обязано получить отказ.
 *
 * <p>Проверяется на обоих транспортах, потому что транспорт, обходящий политику, — баг куда хуже
 * того, который не донёс байты: второй заметен сразу, первый молчит. Запасной HTTP-транспорт
 * попадает в тот же обработчик сессии, а значит и в ту же политику, и именно это его к ней и
 * привязывает.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "tl.allow[0]=^127\\.0\\.0\\.1:1$")
class TLAllowListTest {

    @LocalServerPort
    int serverPort;

    @ParameterizedTest
    @EnumSource(Transport.class)
    @Timeout(60)
    void aForwardStreamToADeniedDestinationIsClosed(Transport transport) throws Exception {
        try (EchoServer echo = EchoServer.echoing()) {
            int localPort = freePort();
            ForwardSpec local = ForwardSpec.parse(localPort + ":127.0.0.1:" + echo.port());
            ClientConnection conn =
                    new ClientConnection(url(), null, 25, List.of(local), List.of(), transport);
            Thread t = runAsync(conn);
            try (Socket sock = connect(localPort)) {
                sock.setSoTimeout(20_000);
                sock.getOutputStream().write("knock\n".getBytes());
                sock.getOutputStream().flush();
                // Слушатель локальный, так что подключиться получается всегда; отказ проявляется
                // тем, что туннельный поток закрывают и ничего не возвращают эхом.
                assertEquals(-1, sock.getInputStream().read(),
                        "a denied destination must end the stream, not relay data");
            } finally {
                conn.stop();
                t.join(5_000);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Transport.class)
    @Timeout(60)
    void aDeniedReverseListenerIsNeverOpened(Transport transport) throws Exception {
        try (EchoServer echo = EchoServer.echoing()) {
            int serverListen = freePort();
            Reverse reverse = ForwardSpec.parse("R:" + serverListen + ":127.0.0.1:" + echo.port()).toReverse();
            ClientConnection conn =
                    new ClientConnection(url(), null, 25, List.of(), List.of(reverse), transport);
            Thread t = runAsync(conn);
            try {
                // Даём серверу время отработать CONFIG, а потом убеждаемся, что порт так и
                // остался закрыт, а не побыл недолго открытым.
                Thread.sleep(2_000);
                assertThrows(ConnectException.class, () -> new Socket("127.0.0.1", serverListen).close(),
                        "the server must not bind a reverse port the allow list rejects");
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
