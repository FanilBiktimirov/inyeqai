package com.inyeqai.tl.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * A throwaway TCP server on a random loopback port, used as the far end of a tunnel in the
 * end-to-end tests. Closing it stops the accept loop.
 */
final class EchoServer implements AutoCloseable {

    private final ServerSocket listener;

    private EchoServer(Consumer<Socket> handler) throws IOException {
        listener = new ServerSocket();
        listener.bind(new InetSocketAddress("127.0.0.1", 0));
        Thread t = new Thread(() -> {
            while (!listener.isClosed()) {
                try {
                    Socket s = listener.accept();
                    Thread worker = new Thread(() -> {
                        try (Socket sock = s) {
                            handler.accept(sock);
                        } catch (IOException ignored) {
                        }
                    }, "echo-worker");
                    worker.setDaemon(true);
                    worker.start();
                } catch (IOException e) {
                    return;
                }
            }
        }, "echo-accept");
        t.setDaemon(true);
        t.start();
    }

    /** Sends every byte straight back. */
    static EchoServer echoing() throws IOException {
        return new EchoServer(EchoServer::pipeBack);
    }

    /**
     * Reads until the client half-closes, then reports how many bytes arrived. A client
     * that shuts down its write side and waits for this reply only gets an answer if the
     * tunnel carried the half-close instead of tearing the whole stream down.
     */
    static EchoServer countingUntilEof() throws IOException {
        return new EchoServer(sock -> {
            try {
                InputStream in = sock.getInputStream();
                long total = 0;
                byte[] buf = new byte[4096];
                int n;
                while ((n = in.read(buf)) != -1) {
                    total += n;
                }
                OutputStream out = sock.getOutputStream();
                out.write(("got " + total + "\n").getBytes(StandardCharsets.UTF_8));
                out.flush();
            } catch (IOException ignored) {
            }
        });
    }

    int port() {
        return listener.getLocalPort();
    }

    @Override
    public void close() throws IOException {
        listener.close();
    }

    private static void pipeBack(Socket sock) {
        try {
            InputStream in = sock.getInputStream();
            OutputStream out = sock.getOutputStream();
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) != -1) {
                out.write(buf, 0, n);
                out.flush();
            }
        } catch (IOException ignored) {
        }
    }
}
