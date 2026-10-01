package com.inyeqai.tl.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

/**
 * Переход на запасной транспорт чего-то стоит только если происходит сам. Оператору, которому
 * надо заметить заблокированный upgrade и руками передать {@code --transport http}, куда полезнее
 * было бы внятное сообщение об ошибке.
 *
 * <p>Поэтому здесь гоняется весь клиент целиком, вместе с циклом переподключений, через прокси,
 * который отказывает в WebSocket-upgrade и пробрасывает всё остальное. Никто клиенту не говорит,
 * что не так с сетью: он пробует WebSocket, падает и на следующей попытке возвращается по HTTP.
 *
 * <p>Вердикт читается с сервера, а не из собственного отчёта клиента, — чтобы тест нельзя было
 * удовлетворить клиентом, который всего лишь считает, что переключился. Сессии, пришедшие по
 * HTTP, несут id с префиксом {@code http-}, и именно это показывает {@code /status}.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class HttpFallbackTest {

    @LocalServerPort
    int serverPort;

    @Test
    @Timeout(120)
    void aClientWhoseUpgradeIsRefusedCarriesOnOverHttp() throws Exception {
        try (EchoServer echo = EchoServer.echoing();
             UpgradeBlockingPy Py = UpgradeBlockingPy.inFrontOf(serverPort)) {

            int localPort = freePort();
            Thread client = runClient(setup(Py.port(), localPort, echo.port(), null));
            try {
                // Первая попытка — WebSocket, и прокси отвечает 400. Клиент выжидает, чередует
                // транспорты, а локальный слушатель открывается только когда попытка удалась.
                assertEquals("through-a-hostile-Py",
                        roundTrip(localPort, "through-a-hostile-Py"),
                        "the TL should carry bytes without anyone choosing a transport");

                String status = serverStatus();
                assertTrue(status.contains("session http-"),
                        "the server should see this session as an HTTP one, not a WebSocket:\n" + status);
            } finally {
                client.interrupt();
                client.join(15_000);
            }
        }
    }

    /**
     * Прибитый {@code --transport ws} не должен тихо переключаться: навязанный выбор — это выбор.
     */
    @Test
    @Timeout(60)
    void aPinnedWebSocketDoesNotFallBack() throws Exception {
        try (EchoServer echo = EchoServer.echoing();
             UpgradeBlockingPy Py = UpgradeBlockingPy.inFrontOf(serverPort)) {

            int localPort = freePort();
            Thread client = runClient(
                    setup(Py.port(), localPort, echo.port(), Transport.WEBSOCKET));
            try {
                // Времени хватает на несколько попыток переподключения, и все обязаны провалиться.
                Thread.sleep(6_000);
                assertTrue(serverStatus().contains("no clients connected"),
                        "a pinned WebSocket client should keep failing, not switch transports");
            } finally {
                client.interrupt();
                client.join(15_000);
            }
        }
    }

    /** @param transport null — это auto, именно про него первый тест */
    private static ClientSetup setup(int PyPort, int localPort, int echoPort, Transport transport) {
        return ClientSetup.of("ws://127.0.0.1:" + PyPort + "/TL", null, 5,
                List.of(localPort + ":127.0.0.1:" + echoPort), transport, "127.0.0.1", 0);
    }

    private Thread runClient(ClientSetup setup) {
        Thread t = new Thread(() -> {
            try {
                TLClient.run(setup);
            } catch (Exception ignored) {
                // цикл заканчивается, когда рабочий поток прерывают
            }
        }, "client-under-test");
        t.setDaemon(true);
        t.start();
        return t;
    }

    /** Прямо на сервер, не через прокси: это собственный взгляд теста на истину. */
    private String serverStatus() throws Exception {
        HttpResponse<String> res = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + serverPort + "/status")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, res.statusCode());
        return res.body();
    }

    private static String roundTrip(int port, String message) throws Exception {
        try (Socket sock = connect(port)) {
            sock.setSoTimeout(20_000);
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

    /** Повторяет, пока клиент не остановится на работающем транспорте, иначе падает громко. */
    private static Socket connect(int port) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (true) {
            try {
                return new Socket("127.0.0.1", port);
            } catch (ConnectException e) {
                if (System.currentTimeMillis() > deadline) {
                    fail("the client never opened its local listener on " + port);
                }
                Thread.sleep(200);
            }
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
