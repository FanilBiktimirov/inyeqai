package com.inyeqai.tunnel.client;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inyeqai.tunnel.common.ForwardSpec;
import com.inyeqai.tunnel.common.Mux;
import com.inyeqai.tunnel.common.Reverse;
import com.inyeqai.tunnel.common.StreamInfo;

/**
 * A health endpoint for the client, on the JDK's own HTTP server so the client keeps its
 * "no dependencies beyond the JDK" shape.
 *
 * <p>It deliberately does not answer "the process is running", which is all an ordinary
 * liveness probe can tell and is worth nothing here: a tunnel client whose peer vanished
 * without a TCP close keeps its socket, keeps its listeners open, and keeps accepting
 * connections it can no longer carry. The question that matters is whether the far end is
 * still answering, and the honest evidence for that is the age of the last keepalive reply.
 *
 * <pre>
 *   GET /healthz   200 when the tunnel is usable, 503 when it is not
 *   GET /status    the same verdict with the numbers behind it, as plain text
 * </pre>
 *
 * <p>With {@code --keepalive 0s} there are no pongs, so no such evidence exists. The
 * endpoint then reports {@code unknown} and still answers 200: refusing to call a link
 * healthy is right, but so is not calling it broken on the strength of a check the operator
 * switched off. The reason is always printed.
 */
final class ClientHealth implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ClientHealth.class);

    /** What the endpoint concluded, and why. */
    enum Verdict {
        /** Connected and the peer is answering keepalives. */
        UP(200),
        /** Connected, but keepalive is off, so nothing proves the peer is there. */
        UNKNOWN(200),
        /** Connected, yet the peer stopped answering: the link is about to be dropped. */
        STALE(503),
        /** No connection at all; the client is between reconnect attempts. */
        DOWN(503);

        final int code;

        Verdict(int code) {
            this.code = code;
        }
    }

    private final HttpServer http;
    private final Supplier<ClientConnection> current;

    /**
     * @param current the live connection, or null while reconnecting; read on every request
     *                so the endpoint follows the client across reconnects
     */
    ClientHealth(String bindHost, int port, Supplier<ClientConnection> current) throws IOException {
        this.current = current;
        this.http = HttpServer.create(new InetSocketAddress(bindHost, port), 0);
        http.createContext("/healthz", this::healthz);
        http.createContext("/status", this::status);
        http.setExecutor(Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "health");
            t.setDaemon(true);
            return t;
        }));
        http.start();
        log.info("health endpoint on {}:{} (/healthz, /status)", bindHost, http.getAddress().getPort());
    }

    int port() {
        return http.getAddress().getPort();
    }

    @Override
    public void close() {
        http.stop(0);
    }

    private void healthz(HttpExchange ex) throws IOException {
        Verdict v = verdict(current.get());
        respond(ex, v.code, v.name().toLowerCase(Locale.ROOT) + "\n");
    }

    private void status(HttpExchange ex) throws IOException {
        ClientConnection c = current.get();
        respond(ex, verdict(c).code, render(c));
    }

    /**
     * The whole judgement, in one place. Order matters: a connection that is gone is DOWN
     * whatever the counters say, and a stale pong outranks being nominally connected.
     */
    private static Verdict verdict(ClientConnection c) {
        if (c == null || !c.connected()) {
            return Verdict.DOWN;
        }
        Optional<Duration> pong = c.sinceLastPong();
        if (pong.isEmpty()) {
            return Verdict.UNKNOWN;
        }
        return pong.get().compareTo(c.pongDeadline()) > 0 ? Verdict.STALE : Verdict.UP;
    }

    private String render(ClientConnection c) {
        StringBuilder sb = new StringBuilder();
        Verdict v = verdict(c);
        sb.append("tunnel client: ").append(v.name().toLowerCase(Locale.ROOT)).append('\n');

        if (c == null || !c.connected()) {
            sb.append("reason: no connection to the server, reconnecting\n");
            return sb.toString();
        }
        sb.append("server: ").append(c.url()).append('\n');
        // Which carrier is in use, because with --transport auto the client chose it, not the
        // operator: "http" here is the visible sign that the WebSocket did not survive.
        sb.append("transport: ").append(c.transport().label()).append('\n');
        sb.append("connected for ").append(duration(c.uptime())).append('\n');

        Optional<Duration> pong = c.sinceLastPong();
        if (pong.isEmpty()) {
            sb.append("last pong: keepalive is off, nothing proves the server is answering\n");
        } else {
            sb.append("last pong: ").append(duration(pong.get()))
                    .append(" ago (dropped after ").append(duration(c.pongDeadline())).append(")\n");
            if (v == Verdict.STALE) {
                sb.append("reason: the server stopped answering keepalives\n");
            }
        }

        for (ForwardSpec f : c.locals()) {
            sb.append("forward ").append(f.bindHost()).append(':').append(f.bindPort())
                    .append(" -> server dials ").append(f.dstHost()).append(':').append(f.dstPort()).append('\n');
        }
        for (Reverse r : c.reverses()) {
            sb.append("reverse server:").append(r.serverPort())
                    .append(" -> we dial ").append(r.clientHost()).append(':').append(r.clientPort()).append('\n');
        }

        c.mux().ifPresent(m -> {
            sb.append("streams open ").append(m.openStreams())
                    .append(", carried ").append(m.streamsOpened())
                    .append(", sent ").append(Mux.bytes(m.bytesToPeer()))
                    .append(", received ").append(Mux.bytes(m.bytesFromPeer())).append('\n');
            List<StreamInfo> live = m.streams();
            for (StreamInfo s : live) {
                sb.append(String.format(Locale.ROOT,
                        "  %-10s %-28s open %8s  sent %-10s received %s%n",
                        s.label(), s.dst(), duration(Duration.ofMillis(s.ageMillis())),
                        Mux.bytes(s.sent()), Mux.bytes(s.received())));
            }
        });
        return sb.toString();
    }

    private static void respond(HttpExchange ex, int code, String body) throws IOException {
        byte[] out = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
        // No caching: a cached health answer is worse than none.
        ex.getResponseHeaders().add("Cache-Control", "no-store");
        ex.sendResponseHeaders(code, out.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(out);
        }
    }

    /** Compact human durations, matching the server's status view. */
    static String duration(Duration d) {
        long millis = d.toMillis();
        if (millis < 1000) {
            return millis + "ms";
        }
        long seconds = millis / 1000;
        if (seconds < 60) {
            return String.format(Locale.ROOT, "%.1fs", millis / 1000.0);
        }
        if (seconds < 3600) {
            return String.format(Locale.ROOT, "%dm%02ds", seconds / 60, seconds % 60);
        }
        return String.format(Locale.ROOT, "%dh%02dm", seconds / 3600, (seconds % 3600) / 60);
    }
}
