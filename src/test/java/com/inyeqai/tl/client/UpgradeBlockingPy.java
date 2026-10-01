package com.inyeqai.tl.client;

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
 * A proxy that passes ordinary HTTP through and refuses to carry a WebSocket upgrade,
 * answering 400 instead. This is the network the fallback transport exists for: nothing is
 * broken, nothing is unreachable, and the upgrade simply never completes.
 *
 * <p>Simulating it this way rather than, say, pointing the client at a path that happens to
 * 404 is what makes the test mean something: the client has to discover the problem the way it
 * would in a real network, from a handshake that fails while the same host keeps serving
 * requests perfectly well.
 */
final class UpgradeBlockingPy implements AutoCloseable {

    private static final byte[] REFUSAL = ("HTTP/1.1 400 Bad Request\r\n"
            + "Content-Length: 0\r\n"
            + "Connection: close\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1);

    private final ServerSocket listener;
    private final int targetPort;

    private UpgradeBlockingPy(int targetPort) throws IOException {
        this.targetPort = targetPort;
        this.listener = new ServerSocket();
        listener.bind(new InetSocketAddress("127.0.0.1", 0));
        Thread t = new Thread(this::acceptLoop, "blocking-Py");
        t.setDaemon(true);
        t.start();
    }

    static UpgradeBlockingPy inFrontOf(int targetPort) throws IOException {
        return new UpgradeBlockingPy(targetPort);
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
            Thread worker = new Thread(() -> handle(client), "blocking-Py-conn");
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
            // Everything else is forwarded verbatim, including the body that follows the head
            // and any further requests on this connection.
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

    /** Read up to and including the blank line that ends a request head, and no further. */
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
                // either end going away ends this direction
            } finally {
                closeQuiet(a);
                closeQuiet(b);
            }
        }, "blocking-Py-pipe");
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
