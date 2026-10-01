package com.laptopkit.tunnel;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MapPropertySource;

import com.laptopkit.tunnel.client.ClientSetup;
import com.laptopkit.tunnel.client.TunnelClient;

/**
 * One entry point, no arguments. What runs and how is decided by the fields in
 * {@link TunnelConfig}; this class only turns them into a running server, a running client, or
 * both at once.
 *
 * <p>That is the whole point of this branch. A tunnel is awkward to debug through a command
 * line: the interesting failures are in the frames between the two ends, and getting there used
 * to mean two run configurations, two terminals and a careful pair of argument lists. Here it is
 * one green arrow, and in {@link TunnelConfig.Mode#BOTH} one process with both ends in it.
 */
@SpringBootApplication
public class TunnelApplication {

    public static void main(String[] args) throws Exception {
        switch (TunnelConfig.MODE) {
            case SERVER -> {
                startServer();
                Thread.currentThread().join(); // Spring's own threads keep it alive; wait here
            }
            case CLIENT -> runClient();
            case BOTH -> runBoth();
            case HEALTHCHECK -> healthcheck();
        }
    }

    /**
     * Server first, then the client in this thread. Starting the server is synchronous, so by
     * the time the client connects there is something to connect to.
     *
     * <p>Running the client on the main thread is deliberate: it keeps the stack a debugger shows
     * on a breakpoint short and recognisable, rather than rooted in a pool thread.
     */
    private static void runBoth() throws Exception {
        ConfigurableApplicationContext context = startServer();
        try {
            runClient();
        } finally {
            context.close();
        }
    }

    /**
     * Boot the server with the fields as Spring properties, which is how Spring is configured.
     *
     * <p>The properties are pushed to the front of the environment rather than handed to
     * {@code SpringApplicationBuilder.properties}, which registers them as <em>default</em>
     * properties &mdash; the lowest precedence there is, below {@code application.yml}. Done that
     * way, every field here would be silently overridden by the yml: the server would come up on
     * 8080 instead of {@link TunnelConfig#SERVER_PORT}, and the client would spend its life
     * failing to reach a port nothing is listening on. The old command line avoided this by
     * accident, because command-line arguments outrank the yml.
     */
    private static ConfigurableApplicationContext startServer() {
        Map<String, Object> props = new HashMap<>();
        props.put("server.port", TunnelConfig.SERVER_PORT);
        if (!TunnelConfig.SERVER_HOST.isBlank()) {
            props.put("server.address", TunnelConfig.SERVER_HOST);
        }
        props.put("tunnel.auth", TunnelConfig.AUTH);
        props.put("tunnel.path", TunnelConfig.PATH);
        props.put("tunnel.status", TunnelConfig.STATUS);
        props.put("tunnel.http-fallback", TunnelConfig.HTTP_FALLBACK);
        props.put("tunnel.keepalive", TunnelConfig.SERVER_KEEPALIVE);
        props.put("tunnel.pong-timeout", TunnelConfig.PONG_TIMEOUT);
        // Spring binds a list by index, the same way the repeated flag used to.
        List<String> allow = TunnelConfig.ALLOW;
        for (int i = 0; i < allow.size(); i++) {
            props.put("tunnel.allow[" + i + "]", allow.get(i));
        }
        if (TunnelConfig.VERBOSE) {
            props.put("logging.level.com.laptopkit.tunnel", "DEBUG");
        }
        return new SpringApplicationBuilder(TunnelApplication.class)
                .initializers(context -> context.getEnvironment().getPropertySources()
                        .addFirst(new MapPropertySource("tunnelConfig", props)))
                .run();
    }

    /** Hand the client its parameters, resolved. Blocks until the process is stopped. */
    private static void runClient() throws Exception {
        ClientSetup setup;
        try {
            setup = ClientSetup.of(
                    TunnelConfig.clientUrl(),
                    TunnelConfig.AUTH,
                    TunnelConfig.CLIENT_KEEPALIVE_SECONDS,
                    TunnelConfig.FORWARDS,
                    TunnelConfig.TRANSPORT,
                    TunnelConfig.HEALTH_HOST,
                    TunnelConfig.HEALTH_PORT);
            if (TunnelConfig.VERBOSE) {
                TunnelClient.enableVerbose();
            }
        } catch (IllegalArgumentException e) {
            // A bad field should say which field, and say it before anything starts listening.
            System.err.println("TunnelConfig: " + e.getMessage());
            return;
        }
        TunnelClient.run(setup);
    }

    /**
     * Probe a health endpoint and exit 0 or 1, so a container HEALTHCHECK can use it.
     *
     * <p>It lives in the jar because the runtime image carries no curl or wget, and adding
     * one just to ask a local port a question is a worse trade than reusing the HTTP client
     * already in the runtime.
     */
    private static void healthcheck() {
        String url = TunnelConfig.HEALTHCHECK_URL;
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
}
