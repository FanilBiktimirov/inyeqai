package com.inyeqai.tl.server;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import com.inyeqai.tl.common.Mux;
import com.inyeqai.tl.common.StreamInfo;

/**
 * {@code GET /status} &mdash; what the tunnel is doing right now, as plain text meant to be
 * read by a person with curl.
 *
 * <p>The logs say when a client connects and when a listener opens, and nothing after that.
 * This is the missing other half: which streams are live, where each one goes, how much it
 * has carried, and how long ago each session last answered a keepalive. A tunnel that is
 * quietly dropping streams and one that is working look identical without it.
 *
 * <p>It is behind the same {@code X-TL-Auth} token as the tunnel itself, because the
 * output names internal destinations and traffic volumes.
 */
@RestController
class StatusController {

    private final TLWebSocketHandler handler;
    private final TLProperties props;
    private final SharedSecret secret;
    private final long startedAtNanos = System.nanoTime();

    StatusController(TLWebSocketHandler handler, TLProperties props, SharedSecret secret) {
        this.handler = handler;
        this.props = props;
        this.secret = secret;
    }

    @GetMapping(value = "/status", produces = MediaType.TEXT_PLAIN_VALUE)
    ResponseEntity<String> status(
            @RequestHeader(name = "X-TL-Auth", required = false) String token) {
        if (!props.isStatus()) {
            return ResponseEntity.notFound().build();
        }
        if (!secret.accepts(token)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("unauthorized\n");
        }
        return ResponseEntity.ok(render(handler.snapshot()));
    }

    private String render(List<SessionSnapshot> sessions) {
        int openStreams = 0;
        long carried = 0;
        long toClients = 0;
        long fromClients = 0;
        for (SessionSnapshot s : sessions) {
            openStreams += s.openStreams();
            carried += s.streamsCarried();
            toClients += s.bytesToClient();
            fromClients += s.bytesFromClient();
        }

        StringBuilder sb = new StringBuilder();
        sb.append("TL server, up ")
                .append(duration(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos)))
                .append('\n');
        sb.append("sessions ").append(sessions.size())
                .append(", streams open ").append(openStreams)
                .append(", carried ").append(carried).append('\n');
        sb.append("to clients ").append(Mux.bytes(toClients))
                .append(", from clients ").append(Mux.bytes(fromClients)).append('\n');
        sb.append("allow list: ")
                .append(secret.open() ? "no token set (open server) " : "")
                .append(props.getAllow().isEmpty()
                        ? "empty, any address reachable"
                        : props.getAllow().size() + " pattern(s)")
                .append('\n');

        if (sessions.isEmpty()) {
            sb.append("\nno clients connected\n");
            return sb.toString();
        }
        for (SessionSnapshot s : sessions) {
            sb.append('\n').append("session ").append(s.id()).append(" from ").append(s.remote()).append('\n');
            sb.append("  up ").append(duration(s.upMillis()))
                    .append(", last pong ").append(duration(s.lastPongMillis())).append(" ago").append('\n');
            if (s.resumes() > 0 || s.holdingBytes() > 0) {
                // Printed only when there is something to print, so a WebSocket session and a
                // healthy idle HTTP one stay as quiet as they were.
                sb.append("  carrier resumed ").append(s.resumes()).append(" time(s), holding ")
                        .append(Mux.bytes(s.holdingBytes())).append(" unconfirmed").append('\n');
            }
            sb.append("  reverse listeners: ")
                    .append(s.reversePorts().isEmpty() ? "none" : s.reversePorts()).append('\n');
            sb.append("  streams open ").append(s.openStreams())
                    .append(", carried ").append(s.streamsCarried())
                    .append(", sent ").append(Mux.bytes(s.bytesToClient()))
                    .append(", received ").append(Mux.bytes(s.bytesFromClient())).append('\n');
            for (StreamInfo st : s.streams()) {
                sb.append(String.format(Locale.ROOT,
                        "    %-10s %-28s open %8s  sent %-10s received %s%n",
                        st.label(), st.dst(), duration(st.ageMillis()),
                        Mux.bytes(st.sent()), Mux.bytes(st.received())));
            }
        }
        return sb.toString();
    }

    /** Compact human durations: 980ms, 4.2s, 3m12s, 2h05m. */
    static String duration(long millis) {
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
