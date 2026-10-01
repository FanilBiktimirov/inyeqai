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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import com.laptopkit.tunnel.common.ForwardSpec;
import com.laptopkit.tunnel.common.Reverse;

/**
 * Поднимает настоящий Spring-сервер туннеля на случайном порту и гоняет против него настоящий
 * {@link ClientConnection}, проверяя, что байты переживают круг туда-обратно и в локальном
 * направлении, и в обратном.
 *
 * <p>Каждый случай прогоняется на обоих транспортах. В этом и смысл параметризации вместо
 * второго набора тестов: запасной HTTP-транспорт стоит иметь только если он берёт ту же планку,
 * что и WebSocket, а скопированный набор разъехался бы с этим при первом же изменении любого
 * из них.
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

    @ParameterizedTest
    @EnumSource(Transport.class)
    @Timeout(60)
    void localForwardCarriesBytes(Transport transport) throws Exception {
        int localPort = freePort();
        ForwardSpec local = ForwardSpec.parse(localPort + ":127.0.0.1:" + echo.port());
        ClientConnection conn =
                new ClientConnection(url(), null, 25, List.of(local), List.of(), transport);
        Thread t = runAsync(conn);
        try {
            assertEquals("hello-local", roundTrip(localPort, "hello-local"));
        } finally {
            stop(conn, t);
        }
    }

    @ParameterizedTest
    @EnumSource(Transport.class)
    @Timeout(60)
    void reverseForwardCarriesBytes(Transport transport) throws Exception {
        int serverListen = freePort();
        Reverse reverse = ForwardSpec.parse("R:" + serverListen + ":127.0.0.1:" + echo.port()).toReverse();
        ClientConnection conn =
                new ClientConnection(url(), null, 25, List.of(), List.of(reverse), transport);
        Thread t = runAsync(conn);
        try {
            assertEquals("hello-reverse", roundTrip(serverListen, "hello-reverse"));
        } finally {
            stop(conn, t);
        }
    }

    /**
     * Клиент, который перестал писать, но продолжает читать, обязан получить свой ответ. Пока
     * не было кадра EOF, конец одного направления обрушивал весь поток, и этот ответ не
     * приходил никогда.
     */
    @ParameterizedTest
    @EnumSource(Transport.class)
    @Timeout(60)
    void halfCloseLetsTheDestinationReplyAfterEof(Transport transport) throws Exception {
        try (EchoServer counting = EchoServer.countingUntilEof()) {
            int localPort = freePort();
            ForwardSpec local = ForwardSpec.parse(localPort + ":127.0.0.1:" + counting.port());
            ClientConnection conn =
                    new ClientConnection(url(), null, 25, List.of(local), List.of(), transport);
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

    /**
     * Байт больше, чем одно окно управления потоком, — чтобы доказать, что кредит возвращается
     * и порядок сохраняется.
     */
    @ParameterizedTest
    @EnumSource(Transport.class)
    @Timeout(180)
    void bulkTransferSurvivesFlowControl(Transport transport) throws Exception {
        int size = 4 * 1024 * 1024;
        byte[] payload = new byte[size];
        new Random(1234).nextBytes(payload);

        int localPort = freePort();
        ForwardSpec local = ForwardSpec.parse(localPort + ":127.0.0.1:" + echo.port());
        ClientConnection conn =
                new ClientConnection(url(), null, 25, List.of(local), List.of(), transport);
        Thread t = runAsync(conn);
        try (Socket sock = connect(localPort)) {
            sock.setSoTimeout(60_000);
            // Пишем из отдельного потока: 4 МБ не влезут ни в один буфер сокета, так что запись
            // и чтение обязаны идти вперемешку, иначе оба конца заблокируются навсегда.
            Thread writer = new Thread(() -> {
                try {
                    OutputStream out = sock.getOutputStream();
                    out.write(payload);
                    out.flush();
                    sock.shutdownOutput();
                } catch (IOException e) {
                    // настоящую ошибку сообщит проверка на стороне чтения
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
                // run() заканчивается, когда соединение останавливают
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

    /** Подключается к входному порту туннеля, повторяя попытки, пока слушатель не поднимется. */
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
