package com.laptopkit.tunnel.client;

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
 * A proxy that severs the tunnel's downstream response once it has carried a set number of bytes,
 * and leaves every other request alone. This is the network the resumable downstream exists for: a
 * gateway with a cap on how large or how long one response may be, which to a tunnel means its
 * downstream is cut on a schedule while requests keep working perfectly.
 *
 * <p>Capping only the downstream is what makes a test using this mean something: requests keep
 * going through, so what is being exercised is resumption of that one response rather than a
 * general outage that any reconnect would recover from.
 *
 * <p>Picking out that response is less obvious than it sounds. The client keeps connections alive
 * and reuses them, so the downstream {@code GET} often travels on a connection whose first request
 * was something else entirely &mdash; deciding from the first request head alone would quietly cap
 * nothing at all. Instead the request direction is watched for the downstream path, including
 * across read boundaries, and the cap is applied to a connection once it has carried one.
 */
final class DownstreamCappingProxy implements AutoCloseable {

    private static final byte[] MARKER = "/http/down/".getBytes(StandardCharsets.ISO_8859_1);

    private final ServerSocket listener;
    private final int targetPort;
    private final int capBytes;
    private final AtomicInteger cuts = new AtomicInteger();

    private DownstreamCappingProxy(int targetPort, int capBytes) throws IOException {
        this.targetPort = targetPort;
        this.capBytes = capBytes;
        this.listener = new ServerSocket();
        listener.bind(new InetSocketAddress("127.0.0.1", 0));
        Thread t = new Thread(this::acceptLoop, "capping-proxy");
        t.setDaemon(true);
        t.start();
    }

    static DownstreamCappingProxy inFrontOf(int targetPort, int capBytes) throws IOException {
        return new DownstreamCappingProxy(targetPort, capBytes);
    }

    int port() {
        return listener.getLocalPort();
    }

    /** How many responses were cut. A test asserting resumption should check this really fired. */
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
            Thread worker = new Thread(() -> handle(client), "capping-proxy-conn");
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

    /** Forward requests, noticing when this connection asks for a downstream. */
    private void pipeRequests(InputStream in, OutputStream out, AtomicBoolean capped,
                              Socket a, Socket b) {
        Thread t = new Thread(() -> {
            // Overlap the reads by the length of the marker so one split across two of them is
            // still found.
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
                // either end going away ends this direction
            } finally {
                closeQuiet(a);
                closeQuiet(b);
            }
        }, "capping-proxy-up");
        t.setDaemon(true);
        t.start();
    }

    /** Forward responses, cutting the connection once a capped one has carried enough. */
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
                        // Exactly how a capped gateway behaves: the response simply stops, with
                        // no hint about how much of it the other end actually took.
                        cuts.incrementAndGet();
                        break;
                    }
                }
            } catch (IOException ignored) {
            } finally {
                closeQuiet(a);
                closeQuiet(b);
            }
        }, "capping-proxy-down");
        t.setDaemon(true);
        t.start();
    }

    /** Index of {@link #MARKER} within the first {@code length} bytes, or -1. */
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
