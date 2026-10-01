package com.inyeqai.tl.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Прокси, который обрывает ответ потока вниз, как только тот пронёс заданное число байт, и не
 * трогает все остальные запросы. Это ровно та сеть, ради которой и нужен восстанавливаемый поток
 * вниз: шлюз с ограничением на размер или длительность одного ответа — для туннеля это значит,
 * что поток вниз обрывают по расписанию, а запросы при этом работают безупречно.
 *
 * <p>Обрывать только поток вниз — именно это придаёт тесту на таком прокси смысл: запросы
 * продолжают проходить, так что проверяется восстановление одного конкретного ответа, а не общий
 * сбой сети, из которого выбрался бы любой переподключающийся клиент.
 *
 * <p>Выцепить этот самый ответ сложнее, чем кажется. Клиент держит соединения открытыми и
 * переиспользует их, поэтому {@code GET} за потоком вниз часто идёт по соединению, чей первый
 * запрос был совсем про другое — решать по одной лишь первой голове запроса значило бы тихо не
 * обрывать вообще ничего. Вместо этого в направлении запросов ищется путь потока вниз, в том
 * числе на стыке двух чтений, и ограничение включается для соединения после того, как оно такой
 * запрос пронесло.
 */
final class DownstreamCappingPy implements AutoCloseable {

    private static final byte[] MARKER = "/http/down/".getBytes(StandardCharsets.ISO_8859_1);

    private final ServerSocket listener;
    private final int targetPort;
    private final int capBytes;
    private final AtomicInteger cuts = new AtomicInteger();

    private DownstreamCappingPy(int targetPort, int capBytes) throws IOException {
        this.targetPort = targetPort;
        this.capBytes = capBytes;
        this.listener = new ServerSocket();
        listener.bind(new InetSocketAddress("127.0.0.1", 0));
        Thread t = new Thread(this::acceptLoop, "capping-Py");
        t.setDaemon(true);
        t.start();
    }

    static DownstreamCappingPy inFrontOf(int targetPort, int capBytes) throws IOException {
        return new DownstreamCappingPy(targetPort, capBytes);
    }

    int port() {
        return listener.getLocalPort();
    }

    /**
     * Сколько ответов оборвано. Тест на восстановление сессии должен убедиться, что это правда
     * сработало.
     */
    int cuts() {
        return cuts.get();
    }

    @Override
    public void close() throws IOException {
        listener.close();
    }

    private void acceptLoop() {
        while (!listener.isClosed()) {
            Socket client;
            try {
                client = listener.accept();
            } catch (IOException e) {
                return;
            }
            Thread worker = new Thread(() -> handle(client), "capping-Py-conn");
            worker.setDaemon(true);
            worker.start();
        }
    }

    private void handle(Socket client) {
        try {
            InputStream fromClient = client.getInputStream();
            byte[] head = readHead(fromClient);
            if (head == null) {
                client.close();
                return;
            }
            AtomicBoolean capped = new AtomicBoolean(indexOf(head, head.length) >= 0);
            Socket server = new Socket();
            server.connect(new InetSocketAddress("127.0.0.1", targetPort), 5_000);
            server.getOutputStream().write(head);
            server.getOutputStream().flush();
            pipeRequests(fromClient, server.getOutputStream(), capped, client, server);
            pipeResponses(server.getInputStream(), client.getOutputStream(), capped, client, server);
        } catch (IOException e) {
            closeQuiet(client);
        }
    }

    /** Читает голову запроса до пустой строки включительно — и ни байта дальше. */
    private static byte[] readHead(InputStream in) throws IOException {
        ByteArrayOutputStream head = new ByteArrayOutputStream();
        int[] tail = new int[4];
        int b;
        while ((b = in.read()) != -1) {
            head.write(b);
            tail[0] = tail[1];
            tail[1] = tail[2];
            tail[2] = tail[3];
            tail[3] = b;
            if (tail[0] == '\r' && tail[1] == '\n' && tail[2] == '\r' && tail[3] == '\n') {
                return head.toByteArray();
            }
            if (head.size() > 64 * 1024) {
                throw new IOException("request head too long");
            }
        }
        return head.size() == 0 ? null : head.toByteArray();
    }

    /** Пробрасывает запросы и замечает, когда это соединение просит поток вниз. */
    private void pipeRequests(InputStream in, OutputStream out, AtomicBoolean capped,
                              Socket a, Socket b) {
        Thread t = new Thread(() -> {
            // Чтения перекрываются на длину маркера, чтобы маркер, разрезанный между двумя
            // чтениями, всё равно нашёлся.
            byte[] buf = new byte[8192];
            byte[] window = new byte[MARKER.length - 1 + buf.length];
            int carried = 0;
            try {
                int n;
                while ((n = in.read(buf)) != -1) {
                    System.arraycopy(buf, 0, window, carried, n);
                    if (indexOf(window, carried + n) >= 0) {
                        capped.set(true);
                    }
                    int keep = Math.min(MARKER.length - 1, carried + n);
                    System.arraycopy(window, carried + n - keep, window, 0, keep);
                    carried = keep;
                    out.write(buf, 0, n);
                    out.flush();
                }
            } catch (IOException ignored) {
                // уход любого из концов заканчивает это направление
            } finally {
                closeQuiet(a);
                closeQuiet(b);
            }
        }, "capping-Py-up");
        t.setDaemon(true);
        t.start();
    }

    /** Пробрасывает ответы и рвёт соединение, когда помеченное пронесло достаточно. */
    private void pipeResponses(InputStream in, OutputStream out, AtomicBoolean capped,
                               Socket a, Socket b) {
        Thread t = new Thread(() -> {
            byte[] buf = new byte[8192];
            long carried = 0;
            try {
                int n;
                while ((n = in.read(buf)) != -1) {
                    out.write(buf, 0, n);
                    out.flush();
                    carried += n;
                    if (capped.get() && carried > capBytes) {
                        // Ровно так ведёт себя шлюз с ограничением: ответ просто прекращается,
                        // и ни намёка на то, сколько из него другой конец на самом деле забрал.
                        cuts.incrementAndGet();
                        break;
                    }
                }
            } catch (IOException ignored) {
            } finally {
                closeQuiet(a);
                closeQuiet(b);
            }
        }, "capping-Py-down");
        t.setDaemon(true);
        t.start();
    }

    /** Позиция {@link #MARKER} в первых {@code length} байтах или -1. */
    private static int indexOf(byte[] haystack, int length) {
        outer:
        for (int i = 0; i + MARKER.length <= length; i++) {
            for (int j = 0; j < MARKER.length; j++) {
                if (haystack[i + j] != MARKER[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    private static void closeQuiet(Socket s) {
        try {
            s.close();
        } catch (IOException ignored) {
        }
    }
}
