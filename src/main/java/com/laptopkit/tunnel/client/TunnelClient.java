package com.laptopkit.tunnel.client;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.laptopkit.tunnel.common.ForwardSpec;
import com.laptopkit.tunnel.common.Reverse;

/**
 * The client entry point. Parses chisel-style arguments, then loops forever: connect,
 * serve until the link drops, back off, reconnect.
 *
 * <pre>
 *   client [--auth user:pass] [--keepalive 25s] &lt;ws-url&gt; &lt;forward&gt; [forward ...]
 * </pre>
 */
public final class TunnelClient {

    private static final Logger log = LoggerFactory.getLogger(TunnelClient.class);

    private static final long MIN_BACKOFF_MS = 1_000;
    private static final long MAX_BACKOFF_MS = 5 * 60_000;
    /** A connection that lasted this long counts as healthy, so the backoff starts over. */
    private static final Duration SETTLED = Duration.ofSeconds(5);

    private TunnelClient() {
    }

    public static void run(String[] args) throws Exception {
        String auth = null;
        int keepaliveSec = 25;
        String health = null;
        // null means auto: try a WebSocket, fall back to HTTP if it does not survive.
        Transport forced = null;
        List<String> positional = new ArrayList<>();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--auth" -> auth = value(args, ++i, "--auth");
                case "--keepalive" -> keepaliveSec = parseDuration(value(args, ++i, "--keepalive"));
                case "-v", "--verbose" -> enableVerbose();
                case "--health" -> health = value(args, ++i, "--health");
                case "--transport" -> forced = parseTransport(value(args, ++i, "--transport"));
                default -> positional.add(args[i]);
            }
        }

        // Catch a mistyped flag before it is mistaken for a URL or a forward. Without this
        // "--kepalive 25s" ends up parsed as a port-forward spec and the failure reads like a
        // problem with the forwards.
        List<String> unknown = positional.stream().filter(a -> a.startsWith("-")).toList();
        if (!unknown.isEmpty()) {
            System.err.println("client: unrecognised argument(s): " + String.join(" ", unknown));
            System.err.println("client: known flags are --auth, --keepalive, --health, "
                    + "--transport, -v/--verbose");
            return;
        }

        if (positional.isEmpty()) {
            System.err.println("client: missing <ws-url> and at least one forward, e.g.");
            System.err.println("  client --auth tunnel:PASS --keepalive 25s \\");
            System.err.println("      wss://host/ 0.0.0.0:3128:127.0.0.1:3129 R:3130:host.docker.internal:3129");
            return;
        }

        String url = normalizeUrl(positional.get(0));
        List<ForwardSpec> locals = new ArrayList<>();
        List<Reverse> reverses = new ArrayList<>();
        for (String spec : positional.subList(1, positional.size())) {
            ForwardSpec fs = ForwardSpec.parse(spec);
            if (fs.reverse()) {
                reverses.add(fs.toReverse());
            } else {
                locals.add(fs);
            }
        }
        if (locals.isEmpty() && reverses.isEmpty()) {
            System.err.println("client: no forwards given");
            return;
        }

        Transport transport = forced == null ? Transport.WEBSOCKET : forced;
        log.info("connecting to {} over {} ({} local, {} reverse forward(s))", url,
                forced == null ? "websocket, falling back to http if it does not hold" : transport.label(),
                locals.size(), reverses.size());

        // The health endpoint outlives any single connection: it has to keep answering
        // while the client is between reconnect attempts, which is exactly when a probe
        // most needs a truthful "down".
        AtomicReference<ClientConnection> live = new AtomicReference<>();
        if (health != null) {
            HostPort hp = HostPort.parse(health);
            try {
                new ClientHealth(hp.host(), hp.port(), live::get);
            } catch (IOException e) {
                System.err.println("client: cannot open the health endpoint on " + health + ": " + e);
                return;
            } catch (NoClassDefFoundError e) {
                // A runtime trimmed with jlink can be missing jdk.httpserver. Say which
                // module, because the bare NoClassDefFoundError names a class nobody would
                // connect to a --health flag.
                System.err.println("client: --health needs the jdk.httpserver module, "
                        + "which this Java runtime does not have (" + e.getMessage() + ")");
                return;
            }
        }

        int attempt = 0;
        // Interruption is the way out: a client asked to stop should stop, not reconnect.
        // Nothing interrupts this thread in normal operation, where the loop runs forever.
        while (!Thread.currentThread().isInterrupted()) {
            Duration uptime;
            String reason;
            ClientConnection conn =
                    new ClientConnection(url, auth, keepaliveSec, locals, reverses, transport);
            live.set(conn);
            try {
                conn.run();
                uptime = conn.uptime();
                reason = "connection closed after " + uptime.toSeconds() + "s";
            } catch (InterruptedException e) {
                conn.stop();
                live.set(null);
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                uptime = conn.uptime();
                reason = "connect failed (" + rootCause(e) + ")";
            }
            // Only a connection that actually settled resets the backoff; a link failing
            // on every attempt must keep backing off instead of hammering the server.
            boolean settled = uptime.compareTo(SETTLED) >= 0;
            attempt = settled ? 0 : attempt;
            long delayMs = backoff(attempt);
            attempt++;
            // Clear it before sleeping, so a probe during the backoff sees "down" rather
            // than the corpse of the connection that just died.
            live.set(null);
            if (forced == null && !settled) {
                // An attempt that never settled does not say which carrier was at fault, so
                // the other one gets the next try. This is what carries a client through a
                // network that refuses the WebSocket upgrade, or accepts it and then eats
                // the frames, without anyone having to notice and pass --transport by hand.
                // A transport that does settle is kept, because it demonstrably works.
                transport = transport.other();
                log.warn("{}, trying the {} transport, reconnecting in {} ms",
                        reason, transport.label(), delayMs);
            } else {
                log.warn("{}, reconnecting in {} ms", reason, delayMs);
            }
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * Turn on per-stream logging. In client mode Spring never starts, so there is no
     * {@code logging.level.*} to set and no logback.xml in play &mdash; the level is moved on
     * the backend directly. Guarded by a type check so an unexpected backend degrades to a
     * warning instead of a ClassCastException on startup.
     */
    private static void enableVerbose() {
        org.slf4j.Logger pkg = LoggerFactory.getLogger("com.laptopkit.tunnel");
        if (pkg instanceof ch.qos.logback.classic.Logger logback) {
            logback.setLevel(ch.qos.logback.classic.Level.DEBUG);
            log.info("verbose logging on: every stream will be logged as it opens and closes");
        } else {
            log.warn("--verbose: logging backend is {}, cannot raise the level",
                    pkg.getClass().getName());
        }
    }

    /**
     * Where the health endpoint listens, parsed from {@code [host:]port}. Defaults to
     * loopback: the endpoint names internal destinations and traffic volumes, so it should
     * not be on a public interface unless the operator asks for it by writing the bind out.
     */
    record HostPort(String host, int port) {

        static HostPort parse(String raw) {
            String s = raw.trim();
            int colon = s.lastIndexOf(':');
            // No colon at all, or the only colons are inside an IPv6 literal with no port.
            if (colon < 0 || s.endsWith("]")) {
                return new HostPort("127.0.0.1", port(s, raw));
            }
            String host = s.substring(0, colon).trim();
            if (host.length() > 1 && host.charAt(0) == '[' && host.endsWith("]")) {
                host = host.substring(1, host.length() - 1);
            }
            if (host.isEmpty()) {
                throw new IllegalArgumentException("--health: empty host in " + raw);
            }
            return new HostPort(host, port(s.substring(colon + 1), raw));
        }

        private static int port(String v, String raw) {
            int n;
            try {
                n = Integer.parseInt(v.trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("--health: bad port in " + raw);
            }
            if (n < 1 || n > 65535) {
                throw new IllegalArgumentException("--health: port out of range in " + raw);
            }
            return n;
        }
    }

    /**
     * The {@code --transport} value.
     *
     * @return the transport to pin, or null for {@code auto}: the client then starts on a
     *         WebSocket and alternates after any attempt that fails to settle, so a network
     *         hostile to the upgrade ends up on HTTP by itself
     */
    static Transport parseTransport(String raw) {
        if (raw.trim().equalsIgnoreCase("auto")) {
            return null;
        }
        return Transport.parse(raw);
    }

    /** Exponential backoff, {@code MIN << attempt} capped at {@code MAX}. */
    static long backoff(int attempt) {
        if (attempt >= 20) { // 1s << 20 already exceeds the cap; avoid the shift overflowing
            return MAX_BACKOFF_MS;
        }
        return Math.min(MIN_BACKOFF_MS << attempt, MAX_BACKOFF_MS);
    }

    /** If the URL has no path, append the default endpoint so a bare host:port works. */
    static String normalizeUrl(String url) {
        String u = url;
        if (!u.startsWith("ws://") && !u.startsWith("wss://")) {
            // accept http/https too, as chisel does, and map to the ws scheme
            if (u.startsWith("https://")) {
                u = "wss://" + u.substring("https://".length());
            } else if (u.startsWith("http://")) {
                u = "ws://" + u.substring("http://".length());
            } else {
                u = "wss://" + u;
            }
        }
        URI parsed = URI.create(u);
        String path = parsed.getPath();
        if (path == null || path.isEmpty() || path.equals("/")) {
            u = stripTrailingSlash(u) + "/tunnel";
        }
        return u;
    }

    private static String stripTrailingSlash(String s) {
        return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
    }

    /**
     * Parse a chisel-style duration into whole seconds. {@code 0} is meaningful: as with
     * chisel, {@code --keepalive 0s} turns keepalive off.
     */
    static int parseDuration(String raw) {
        String s = raw.trim().toLowerCase();
        int mult = 1;
        if (s.endsWith("ms")) {
            int ms = parseAmount(s.substring(0, s.length() - 2), raw);
            // Sub-second keepalive is pointless here; round up to a second unless it is off.
            return ms == 0 ? 0 : Math.max(1, ms / 1000);
        }
        if (s.endsWith("s")) {
            s = s.substring(0, s.length() - 1);
        } else if (s.endsWith("m")) {
            mult = 60;
            s = s.substring(0, s.length() - 1);
        }
        return parseAmount(s, raw) * mult;
    }

    private static int parseAmount(String s, String raw) {
        try {
            int n = Integer.parseInt(s.trim());
            if (n < 0) {
                throw new IllegalArgumentException("negative duration: " + raw);
            }
            return n;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("bad duration: " + raw);
        }
    }

    private static String value(String[] args, int i, String flag) {
        if (i >= args.length) {
            throw new IllegalArgumentException(flag + " needs a value");
        }
        return args[i];
    }

    private static String rootCause(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) {
            c = c.getCause();
        }
        return c.toString();
    }
}
