package com.laptopkit.tunnel;

import java.time.Duration;
import java.util.List;

import com.laptopkit.tunnel.client.Transport;

/**
 * Every run parameter, as a field. This is the only file to edit before pressing Run.
 *
 * <p>On this branch the tunnel takes no command-line arguments at all: there is nothing to put
 * in a run configuration, and nothing between a breakpoint and the value it is looking at. Set
 * {@link #MODE}, adjust whatever else matters, and run {@link TunnelApplication} straight from
 * the editor.
 *
 * <p>The fields are deliberately not {@code final}. A constant would be inlined by the compiler
 * and could not be changed from a running debugger; these can, so a value can be corrected at a
 * breakpoint before the line that reads it.
 *
 * <p>Ports default high on purpose. The usual suspects &mdash; 3128, 3129, 3130, 8080, 8090
 * &mdash; belong to the live chain this tunnel was written for, and a debug run that quietly
 * took one of them would break something real while looking like it worked.
 */
public final class TunnelConfig {

    /** What to start. */
    public enum Mode {
        /** The public side only. */
        SERVER,
        /** The tunnel entrance only; needs a server already running at {@link #CLIENT_URL}. */
        CLIENT,
        /**
         * Both, in one JVM. The point of this mode: one process, both ends, so a breakpoint in
         * {@code Mux} or {@code HttpLink} catches whichever side reaches it first and the whole
         * frame exchange is visible in one debugger.
         */
        BOTH,
        /** Probe a health endpoint and exit, as a container HEALTHCHECK does. */
        HEALTHCHECK
    }

    public static Mode MODE = Mode.BOTH;

    /** Log every stream as it opens and closes, on both sides. Worth having while debugging. */
    public static boolean VERBOSE = true;

    // ─── server ────────────────────────────────────────────────────────────────────────────

    public static int SERVER_PORT = 18080;

    /** Where the server listens. Empty means every interface. */
    public static String SERVER_HOST = "127.0.0.1";

    /** Shared secret the client must present. Empty disables the check. */
    public static String AUTH = "tunnel:debug";

    /** Path the WebSocket endpoint is mounted at; the HTTP fallback lives under it. */
    public static String PATH = "/tunnel";

    /**
     * Where clients may dial, as regexes over {@code host:port} and {@code R:bind:port}. Empty
     * means anywhere the server can reach, which is the convenient setting for debugging and the
     * wrong one for anything public.
     */
    public static List<String> ALLOW = List.of();

    /** How often the server pings clients. Zero turns server-side keepalive and reaping off. */
    public static Duration SERVER_KEEPALIVE = Duration.ofSeconds(25);

    /** Silence after which a client counts as gone and its reverse listeners are freed. */
    public static Duration PONG_TIMEOUT = Duration.ofSeconds(75);

    /** Serve the HTTP fallback transport alongside the WebSocket one. */
    public static boolean HTTP_FALLBACK = true;

    /** Serve {@code GET /status}. */
    public static boolean STATUS = true;

    // ─── client ────────────────────────────────────────────────────────────────────────────

    /**
     * Which server to connect to. Ignored in {@link Mode#BOTH}, where the address is built from
     * {@link #SERVER_PORT} and {@link #PATH} so the port is not kept in two places.
     */
    public static String CLIENT_URL = "ws://127.0.0.1:18080/tunnel";

    /**
     * How frames are carried. {@code null} means auto: start on a WebSocket and fall back to
     * HTTP if an attempt does not hold. Pin it to reproduce one carrier on purpose.
     */
    public static Transport TRANSPORT = null;

    /** How often the client pings. Zero turns its keepalive off, pongs included. */
    public static int CLIENT_KEEPALIVE_SECONDS = 25;

    /**
     * Forwards, in chisel syntax: {@code [bind:]port:host:port}, with {@code R:} for a reverse
     * one. Kept as text because that is how they are written everywhere else &mdash; the README,
     * the compose file, the usual command line &mdash; and a bad one fails loudly at startup.
     *
     * <p>The default makes a loop needing nothing else running: a connection to
     * {@code 127.0.0.1:18128} goes down the tunnel and comes back out at the server's own HTTP
     * port, so {@code curl http://127.0.0.1:18128/healthz} exercises the whole path.
     */
    public static List<String> FORWARDS = List.of(
            "18128:127.0.0.1:18080"
            // , "R:18130:127.0.0.1:18080"   // reverse: the server listens, the client dials
    );

    /** Where the client's own health endpoint listens. Zero turns it off. */
    public static int HEALTH_PORT = 19000;

    public static String HEALTH_HOST = "127.0.0.1";

    // ─── healthcheck ───────────────────────────────────────────────────────────────────────

    /** What {@link Mode#HEALTHCHECK} probes before setting an exit code. */
    public static String HEALTHCHECK_URL = "http://127.0.0.1:19000/healthz";

    private TunnelConfig() {
    }

    /** The address the client should use, which in {@link Mode#BOTH} is the server we just started. */
    public static String clientUrl() {
        if (MODE == Mode.BOTH) {
            return "ws://127.0.0.1:" + SERVER_PORT + PATH;
        }
        return CLIENT_URL;
    }
}
