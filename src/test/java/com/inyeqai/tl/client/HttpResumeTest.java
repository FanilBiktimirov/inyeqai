package com.inyeqai.tl.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import com.inyeqai.tl.common.ForwardSpec;

/**
 * Восстановление сессии имеет смысл только если оборванный поток вниз стоит не дороже одного
 * круга туда-обратно. Эти тесты ставят на пути прокси, который время от времени обрывает ответ
 * потока вниз, и требуют того единственного, что не подделать: все байты обратно и по порядку.
 *
 * <p>Интересен случай передачи больше буфера переотправки — он означает, что кадры и правда
 * подтверждаются и выбрасываются по ходу дела, а не просто все хранятся. А проверка самих
 * вернувшихся байт — то, что придаёт тесту смысл: восстановление, пропустившее кадр или
 * повторившее его дважды, испортит поток так, что в туннеле об этом ничто не сообщит — он просто
 * отдаст другие байты.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class HttpResumeTest {

    @LocalServerPort
    int serverPort;

    @Test
    @Timeout(180)
    void bytesSurviveADownstreamThatIsCutOverAndOver() throws Exception {
        int size = 4 * 1024 * 1024;
        byte[] payload = new byte[size];
        new Random(4321).nextBytes(payload);

        try (EchoServer echo = EchoServer.echoing();
             // Обрыв каждые 256 КБ: 4-мегабайтное эхо обязано пережить больше десятка таких.
             DownstreamCappingPy Py =
                     DownstreamCappingPy.inFrontOf(serverPort, 256 * 1024)) {

            int localPort = freePort();
            ForwardSpec local = ForwardSpec.parse(localPort + ":127.0.0.1:" + echo.port());
            ClientConnection conn = new ClientConnection(
                    "ws://127.0.0.1:" + Py.port() + "/tl", null, 25,
                    List.of(local), List.of(), Transport.HTTP);
            Thread t = runAsync(conn);
            try (Socket sock = connect(localPort)) {
                sock.setSoTimeout(120_000);
                Thread writer = new Thread(() -> {
                    try {
                        OutputStream out = sock.getOutputStream();
                        out.write(payload);
                        out.flush();
                        sock.shutdownOutput();
                    } catch (IOException e) {
                        // настоящую ошибку сообщит сторона чтения
                    }
                }, "bulk-writer");
                writer.setDaemon(true);
                writer.start();

                byte[] got = readFully(sock, size);
                writer.join(10_000);
                assertArrayEquals(payload, got,
                        "every byte should come back once, in order, across the cuts");
                assertTrue(Py.cuts() > 1,
                        "the Py should have cut the downstream repeatedly, but cut "
                                + Py.cuts() + " time(s)");
            } finally {
                stop(conn, t);
            }
        }
    }

    /**
     * Сами потоки не должны ничего заметить. Соединение, открытое до обрыва, обязано остаться
     * годным и после: в этом и разница между восстановлением сессии и её пересборкой, ведь
     * пересобранный туннель теряет все потоки, которые несла предыдущая сессия.
     */
    @Test
    @Timeout(120)
    void aStreamOpenedBeforeACutKeepsWorkingAfterIt() throws Exception {
        try (EchoServer echo = EchoServer.echoing();
             DownstreamCappingPy Py = DownstreamCappingPy.inFrontOf(serverPort, 64 * 1024)) {

            int localPort = freePort();
            ForwardSpec local = ForwardSpec.parse(localPort + ":127.0.0.1:" + echo.port());
            ClientConnection conn = new ClientConnection(
                    "ws://127.0.0.1:" + Py.port() + "/tl", null, 25,
                    List.of(local), List.of(), Transport.HTTP);
            Thread t = runAsync(conn);
            try (Socket sock = connect(localPort)) {
                sock.setSoTimeout(60_000);
                assertEquals("before-the-cut", exchange(sock, "before-the-cut"));

                // Прогоняем по тому же соединению достаточно, чтобы сработало ограничение,
                // а потом продолжаем им пользоваться.
                byte[] filler = new byte[96 * 1024];
                new Random(7).nextBytes(filler);
                sock.getOutputStream().write(filler);
                sock.getOutputStream().flush();
                readFully(sock, filler.length);
                assertTrue(Py.cuts() > 0, "the cap should have cut the downstream by now");

                assertEquals("after-the-cut", exchange(sock, "after-the-cut"),
                        "the same stream should still carry bytes once the downstream resumed");
            } finally {
                stop(conn, t);
            }
        }
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
        t.join(10_000);
    }

    private static String exchange(Socket sock, String message) throws IOException {
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

    private static Socket connect(int port) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
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
