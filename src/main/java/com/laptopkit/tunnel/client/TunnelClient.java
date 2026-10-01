package com.laptopkit.tunnel.client;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The client entry point. Parses chisel-style arguments, then loops forever: connect,
 * serve until the link drops, back off, reconnect.
 *
 * <p>What to connect to and what to forward comes from {@link ClientSetup}, built from the
 * fields in {@code TunnelConfig}. Nothing here reads a command line.
 */
public final class TunnelClient {

    private static final Logger log = LoggerFactory.getLogger(TunnelClient.class);

    private static final long MIN_BACKOFF_MS = 1_000;
    private static final long MAX_BACKOFF_MS = 5 * 60_000;
    /** A connection that lasted this long counts as healthy, so the backoff starts over. */
    private static final Duration SETTLED = Duration.ofSeconds(5);

    private TunnelClient() {
    }

    /**
     * Serve until stopped: connect, carry traffic, back off, reconnect. Everything it needs
     * arrives in {@code setup}; there is nothing to parse and nothing to get wrong in a run
     * configuration.
     */
    public static void run(ClientSetup setup) throws Exception {
        Transport transport = setup.transport() == null ? Transport.WEBSOCKET : setup.transport();
        log.info("connecting to {} over {} ({} local, {} reverse forward(s))", setup.url(),
                setup.transport() == null
                        ? "websocket, falling back to http if it does not hold"
                        : transport.label(),
                setup.locals().size(), setup.reverses().size());

        // The health endpoint outlives any single connection: it has to keep answering
        // while the client is between reconnect attempts, which is exactly when a probe
        // most needs a truthful "down".
        AtomicReference<ClientConnection> live = new AtomicReference<>();
        if (setup.healthPort() > 0) {
            try {
                new ClientHealth(setup.healthHost(), setup.healthPort(), live::get);
            } catch (IOException e) {
                System.err.println("client: cannot open the health endpoint on "
                        + setup.healthHost() + ":" + setup.healthPort() + ": " + e);
                return;
            } catch (NoClassDefFoundError e) {
                // A runtime trimmed with jlink can be missing jdk.httpserver. Say which
                // module, because the bare NoClassDefFoundError names a class nobody would
                // connect to a health endpoint.
                System.err.println("client: the health endpoint needs the jdk.httpserver module, "
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
            ClientConnection conn = new ClientConnection(setup.url(), setup.auth(),
                    setup.keepaliveSeconds(), setup.locals(), setup.reverses(), transport);
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
            if (setup.transport() == null && !settled) {
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
     * Turn on per-stream logging. With no Spring in client mode there is no
     * {@code logging.level.*} to set and no logback.xml in play &mdash; the level is moved on
     * the backend directly. Guarded by a type check so an unexpected backend degrades to a
     * warning instead of a ClassCastException on startup.
     */
    public static void enableVerbose() {
        org.slf4j.Logger pkg = LoggerFactory.getLogger("com.laptopkit.tunnel");
        if (pkg instanceof ch.qos.logback.classic.Logger logback) {
            logback.setLevel(ch.qos.logback.classic.Level.DEBUG);
            log.info("verbose logging on: every stream will be logged as it opens and closes");
        } else {
            log.warn("--verbose: logging backend is {}, cannot raise the level",
                    pkg.getClass().getName());
        }
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

    private static String rootCause(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) {
            c = c.getCause();
        }
        return c.toString();
    }
}
