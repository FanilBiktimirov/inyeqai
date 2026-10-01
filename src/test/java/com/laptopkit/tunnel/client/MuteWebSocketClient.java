package com.laptopkit.tunnel.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * A deliberately unhelpful WebSocket client for tests: it completes the handshake, can send
 * one binary frame, and then goes silent &mdash; it never reads and never answers a ping.
 *
 * <p>No real client library behaves this way, which is the point. It reproduces the state a
 * suspended laptop leaves behind: the TCP connection is still established from the server's
 * side, so the session and the reverse listeners it owns look alive, yet nothing will ever
 * come back. Killing a normal client process cannot reproduce that, because the operating
 * system closes its sockets on the way out.
 */
final class MuteWebSocketClient implements AutoCloseable {

    private static final SecureRandom RANDOM = new SecureRandom();

    private final Socket socket;

    MuteWebSocketClient(String host, int port, String path, String auth) throws IOException {
        socket = new Socket(host, port);
        socket.setSoTimeout(10_000);
        handshake(host, port, path, auth);
    }

    private void handshake(String host, int port, String path, String auth) throws IOException {
        byte[] nonce = new byte[16];
        RANDOM.nextBytes(nonce);
        StringBuilder req = new StringBuilder()
                .append("GET ").append(path).append(" HTTP/1.1\r\n")
                .append("Host: ").append(host).append(':').append(port).append("\r\n")
                .append("Upgrade: websocket\r\n")
                .append("Connection: Upgrade\r\n")
                .append("Sec-WebSocket-Key: ").append(Base64.getEncoder().encodeToString(nonce)).append("\r\n")
                .append("Sec-WebSocket-Version: 13\r\n");
        if (auth != null && !auth.isEmpty()) {
            req.append("X-Tunnel-Auth: ").append(auth).append("\r\n");
        }
        req.append("\r\n");

        OutputStream out = socket.getOutputStream();
        out.write(req.toString().getBytes(StandardCharsets.ISO_8859_1));
        out.flush();

        String status = readHeaders();
        if (!status.contains(" 101")) {
            throw new IOException("handshake refused: " + status);
        }
    }

    /** Read up to the blank line that ends the response headers, returning the status line. */
    private String readHeaders() throws IOException {
        InputStream in = socket.getInputStream();
        StringBuilder sb = new StringBuilder();
        int consecutiveNewlines = 0;
        while (consecutiveNewlines < 2) {
            int c = in.read();
            if (c < 0) {
                throw new IOException("server closed during handshake");
            }
            if (c == '\n') {
                consecutiveNewlines++;
            } else if (c != '\r') {
                consecutiveNewlines = 0;
            }
            sb.append((char) c);
        }
        String all = sb.toString();
        int eol = all.indexOf('\r');
        return eol < 0 ? all : all.substring(0, eol);
    }

    /** Send one masked binary frame, as a client must. */
    void sendBinary(byte[] payload) throws IOException {
        byte[] mask = new byte[4];
        RANDOM.nextBytes(mask);
        OutputStream out = socket.getOutputStream();
        out.write(0x82); // FIN + opcode 2 (binary)
        int len = payload.length;
        if (len < 126) {
            out.write(0x80 | len);
        } else if (len < 65536) {
            out.write(0x80 | 126);
            out.write((len >>> 8) & 0xff);
            out.write(len & 0xff);
        } else {
            out.write(0x80 | 127);
            for (int shift = 56; shift >= 0; shift -= 8) {
                out.write((int) (((long) len >>> shift) & 0xff));
            }
        }
        out.write(mask);
        byte[] masked = new byte[len];
        for (int i = 0; i < len; i++) {
            masked[i] = (byte) (payload[i] ^ mask[i % 4]);
        }
        out.write(masked);
        out.flush();
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
