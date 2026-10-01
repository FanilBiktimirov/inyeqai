package com.inyeqai.tunnel;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.inyeqai.tunnel.client.TunnelClient;

/**
 * One jar, two roles, chosen by the first argument.
 *
 * <pre>
 *   java -jar inyeqai.jar server [--port 8080] [--host 0.0.0.0] [--auth user:pass] [--path /tunnel]
 *   java -jar inyeqai.jar client [--auth user:pass] [--keepalive 25s] &lt;ws-url&gt; &lt;forward&gt; [forward ...]
 * </pre>
 *
 * In {@code client} mode Spring is never started: the client is plain JDK networking,
 * so none of the server beans are created.
 */
@SpringBootApplication
public class TunnelApplication {

    /** {@code -v} for the server, mirroring the client's flag. */
    static final String VERBOSE = "--logging.level.com.inyeqai.tunnel=DEBUG";

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "";
        switch (mode) {
            case "client" -> TunnelClient.run(Arrays.copyOfRange(args, 1, args.length));
            case "server" -> runServer(Arrays.copyOfRange(args, 1, args.length));
            case "healthcheck" -> healthcheck(Arrays.copyOfRange(args, 1, args.length));
            default -> usage();
        }
    }

    /**
     * Probe a health endpoint and exit 0 or 1, so a container HEALTHCHECK can use it.
     *
     * <p>It lives in the jar because the runtime image carries no curl or wget, and adding
     * one just to ask a local port a question is a worse trade than reusing the HTTP client
     * already in the runtime. The cost is a JVM start per probe, so keep the interval
     * generous rather than every few seconds.
     */
    private static void healthcheck(String[] args) {
        String url = args.length > 0 ? args[0] : "http://127.0.0.1:9000/healthz";
        try {
            HttpResponse<String> res = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(5)).build()
                    .send(HttpRequest.newBuilder(URI.create(url))
                                    .timeout(Duration.ofSeconds(5)).GET().build(),
                            HttpResponse.BodyHandlers.ofString());
            // Print it either way: docker keeps this output, and "stale" vs "down" is the
            // difference between a peer that stopped answering and no connection at all.
            System.out.print(res.body());
            System.exit(res.statusCode() >= 200 && res.statusCode() < 300 ? 0 : 1);
        } catch (Exception e) {
            System.out.println("unreachable: " + e);
            System.exit(1);
        }
    }

    /**
     * The server's command line, translated for Spring.
     *
     * @param springArgs what to hand {@link SpringApplication#run}
     * @param unknown    arguments that matched nothing; a non-empty list must stop startup
     */
    record ServerArgs(List<String> springArgs, List<String> unknown) {
    }

    /**
     * Translate our chisel-style flags into Spring's {@code --key=value} command-line
     * property form. Command-line args are a high-precedence property source, so they
     * override application.yml (setDefaultProperties would not).
     */
    static ServerArgs translate(String[] args) {
        List<String> springArgs = new ArrayList<>();
        List<String> unknown = new ArrayList<>();
        int allowCount = 0;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> springArgs.add("--server.port=" + value(args, ++i, "--port"));
                case "--host" -> springArgs.add("--server.address=" + value(args, ++i, "--host"));
                case "--auth" -> springArgs.add("--tunnel.auth=" + value(args, ++i, "--auth"));
                case "--path" -> springArgs.add("--tunnel.path=" + value(args, ++i, "--path"));
                case "--keepalive" ->
                        springArgs.add("--tunnel.keepalive=" + value(args, ++i, "--keepalive"));
                case "--pong-timeout" ->
                        springArgs.add("--tunnel.pong-timeout=" + value(args, ++i, "--pong-timeout"));
                // Takes no value: the fallback is on unless it is switched off, so there is
                // nothing to say but "off".
                case "--no-http-fallback" -> springArgs.add("--tunnel.http-fallback=false");
                // Repeatable, like chisel's authfile entries; Spring binds a list by index.
                case "--allow" -> springArgs.add("--tunnel.allow[" + allowCount++ + "]="
                        + value(args, ++i, "--allow"));
                case "-v", "--verbose" -> springArgs.add(VERBOSE);
                default -> {
                    // Anything already in Spring's own --key=value form goes straight through,
                    // so all of application.yml stays reachable from the command line
                    // (--logging.level.*, --server.ssl.*, ...). Anything else is collected as
                    // unknown rather than dropped.
                    if (args[i].startsWith("--") && args[i].indexOf('=') > 2) {
                        springArgs.add(args[i]);
                    } else {
                        unknown.add(args[i]);
                    }
                }
            }
        }
        return new ServerArgs(springArgs, unknown);
    }

    private static void runServer(String[] args) {
        ServerArgs parsed = translate(args);
        if (!parsed.unknown().isEmpty()) {
            // Refuse to start rather than start misconfigured. A mistyped "--alow" would
            // otherwise leave the allow list empty, and an empty allow list means every
            // address is reachable: the one mistake here that must never look like success.
            System.err.println("server: unrecognised argument(s): "
                    + String.join(" ", parsed.unknown()));
            System.err.println("server: run without arguments to see the usage");
            System.exit(2);
        }
        SpringApplication app = new SpringApplication(TunnelApplication.class);
        app.run(parsed.springArgs().toArray(String[]::new));
    }

    private static String value(String[] args, int i, String flag) {
        if (i >= args.length) {
            throw new IllegalArgumentException(flag + " needs a value");
        }
        return args[i];
    }

    private static void usage() {
        System.out.println("""
                TCP-over-WebSocket tunnel (chisel-like)

                Usage:
                  java -jar inyeqai.jar server [--port 8080] [--host 0.0.0.0] [--auth user:pass]
                                              [--path /tunnel] [--allow REGEX ...]
                                              [--keepalive 25s] [--pong-timeout 75s]
                                              [--no-http-fallback] [-v]
                  java -jar inyeqai.jar client [--auth user:pass] [--keepalive 25s] [-v]
                                              [--transport auto|ws|http]
                                              [--health [bind:]port] <ws-url> <forward> [forward ...]
                  java -jar inyeqai.jar healthcheck [url]      probe a health endpoint, exit 0 or 1

                Forwards:
                  [bind:]port:dsthost:dstport     local   (client listens, server dials the destination)
                  R:[bind:]port:dsthost:dstport   reverse (server listens, client dials the destination)
                  Shorter forms fill in from the back: 3000 means 3000:127.0.0.1:3000.

                --allow restricts what clients may reach, as regexes over "host:port" for
                forward streams and "R:bind:port" for reverse listeners. Repeat the flag per
                pattern. Without it an authenticated client may dial anything this server can.

                -v logs every stream as it opens and closes, with its destination and byte
                counts. GET /status on the server shows the same thing as a live snapshot.

                --transport picks how frames are carried. "auto" (the default) starts on a
                WebSocket and falls back to plain HTTP when the upgrade does not survive the
                network, which is what a proxy stripping "Upgrade" looks like from here. "ws"
                and "http" pin one. The server serves both at once; --no-http-fallback leaves
                only the WebSocket endpoint. The client's /status names the one in use.

                --health opens GET /healthz and GET /status on the client. They report the
                tunnel, not the process: 503 while there is no working connection to the
                server, 200 once the server is answering keepalives. Default bind is
                loopback. "healthcheck" probes such an endpoint and sets an exit code, for a
                container HEALTHCHECK in an image without curl.

                Any --key=value is passed to Spring as-is, so application.yml settings work on
                the command line too (--logging.level.com.inyeqai.tunnel=TRACE, --server.ssl.*).

                Example (mirrors the compose.cloud.yaml chisel client):
                  java -jar inyeqai.jar client --auth tunnel:PASS --keepalive 25s \\
                      wss://alfa.example/ 0.0.0.0:3128:127.0.0.1:3129 R:3130:host.docker.internal:3129
                """);
    }
}
