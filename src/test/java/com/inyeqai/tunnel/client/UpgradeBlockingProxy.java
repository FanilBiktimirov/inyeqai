package com.inyeqai.tunnel.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Прокси, который пропускает обычный HTTP и отказывается нести WebSocket-upgrade, отвечая на него
 * 400. Это ровно та сеть, ради которой и существует запасной транспорт: ничего не сломано, ничего
 * недостижимого нет, просто upgrade никогда не доходит до конца.
 *
 * <p>Воспроизводить это именно так, а не, скажем, направив клиента на путь, который отдаёт 404, —
 * то, что придаёт тесту смысл: клиент должен обнаружить проблему так же, как обнаружил бы её в
 * настоящей сети, — по неудавшемуся рукопожатию, пока тот же хост прекрасно обслуживает запросы.
 */
final class UpgradeBlockingProxy implements AutoCloseable {

    private static final byte[] REFUSAL = ("HTTP/1.1 400 Bad Request\r\n"
            + "Content-Length: 0\r\n"
            + "Connection: close\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1);

    private final ServerSocket listener;
    private final int targetPort;

    private UpgradeBlockingProxy(int targetPort) throws IOException {
        this.targetPort = targetPort;
        this.listener = new ServerSocket();
        listener.bind(new InetSocketAddress("127.0.0.1", 0));
        Thread t = new Thread(this::acceptLoop, "blocking-proxy");
        t.setDaemon(true);
        t.start();
    }

    static UpgradeBlockingProxy inFrontOf(int targetPort) throws IOException {
        return new UpgradeBlockingProxy(targetPort);
    }

    int port() {
        return listener.getLocalPort();
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
            Thread worker = new Thread(() -> handle(client), "blocking-proxy-conn");
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
            String text = new String(head, StandardCharsets.ISO_8859_1).toLowerCase(Locale.ROOT);
            if (text.contains("upgrade:")) {
                client.getOutputStream().write(REFUSAL);
                client.getOutputStream().flush();
                client.close();
                return;
            }
            // Всё остальное пробрасывается дословно, включая тело, которое идёт за головой,
            // и все дальнейшие запросы по этому соединению.
            Socket server = new Socket();
            server.connect(new InetSocketAddress("127.0.0.1", targetPort), 5_000);
            server.getOutputStream().write(head);
            server.getOutputStream().flush();
            pipe(fromClient, server.getOutputStream(), client, server);
            pipe(server.getInputStream(), client.getOutputStream(), client, server);
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

    private static void pipe(InputStream in, OutputStream out, Socket a, Socket b) {
        Thread t = new Thread(() -> {
            byte[] buf = new byte[8192];
            try {
                int n;
                while ((n = in.read(buf)) != -1) {
                    out.write(buf, 0, n);
                    out.flush();
                }
            } catch (IOException ignored) {
                // уход любого из концов заканчивает это направление
            } finally {
                closeQuiet(a);
                closeQuiet(b);
            }
        }, "blocking-proxy-pipe");
        t.setDaemon(true);
        t.start();
    }

    private static void closeQuiet(Socket s) {
        try {
            s.close();
        } catch (IOException ignored) {
        }
    }
}
