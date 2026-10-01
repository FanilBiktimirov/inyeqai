package com.inyeqai.tunnel.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

/**
 * The fallback is only worth anything if it happens by itself. An operator who has to notice a
 * blocked upgrade and pass {@code --transport http} by hand would have been better served by a
 * clear error message.
 *
 * <p>So this drives the whole client, reconnect loop and all, through a proxy that refuses the
 * WebSocket upgrade and forwards everything else. Nobody tells the client what is wrong with
 * the network: it tries a WebSocket, fails, and comes back over HTTP on the next attempt.
 *
 * <p>The verdict is read from the server rather than from the client's own report, so the test
 * cannot be satisfied by a client that merely believes it fell back. Sessions that arrived over
 * HTTP carry an {@code http-} prefixed id, which is what {@code /status} shows.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class HttpFallbackTest {

    @LocalServerPort
    int serverPort;

    @Test
    @Timeout(120)
    void aClientWhoseUpgradeIsRefusedCarriesOnOverHttp() throws Exception {
        try (EchoServer echo = EchoServer.echoing();
             UpgradeBlockingProxy proxy = UpgradeBlockingProxy.inFrontOf(serverPort)) {

            int localPort = freePort();
            Thread client = runClient(
                    "--keepalive", "5s",
                    "ws://127.0.0.1:" + proxy.port() + "/tunnel",
                    localPort + ":127.0.0.1:" + echo.port());
            try {
                // The first attempt is a WebSocket and the proxy answers 400. The client backs
                // off, alternates, and the local listener only opens once an attempt is up.
                assertEquals("through-a-hostile-proxy",
                        roundTrip(localPort, "through-a-hostile-proxy"),
                        "the tunnel should carry bytes without anyone choosing a transport");

                String status = serverStatus();
                assertTrue(status.contains("session http-"),
                        "the server should see this session as an HTTP one, not a WebSocket:\n" + status);
            } finally {
                client.interrupt();
                client.join(15_000);
            }
        }
    }

    /** Pinning {@code --transport ws} must not quietly fall back: a forced choice is a choice. */
    @Test
    @Timeout(60)
    void aPinnedWebSocketDoesNotFallBack() throws Exception {
        try (EchoServer echo = EchoServer.echoing();
             UpgradeBlockingProxy proxy = UpgradeBlockingProxy.inFrontOf(serverPort)) {

            int localPort = freePort();
            Thread client = runClient(
                    "--keepalive", "5s", "--transport", "ws",
                    "ws://127.0.0.1:" + proxy.port() + "/tunnel",
                    localPort + ":127.0.0.1:" + echo.port());
            try {
                // Several reconnect attempts' worth of time, all of which must fail.
                Thread.sleep(6_000);
                assertTrue(serverStatus().contains("no clients connected"),
                        "a pinned WebSocket client should keep failing, not switch transports");
            } finally {
                client.interrupt();
                client.join(15_000);
            }
        }
    }

    private Thread runClient(String... args) {
        Thread t = new Thread(() -> {
            try {
                TunnelClient.run(args);
            } catch (Exception ignored) {
                // the loop ends when the thread is interrupted
            }
        }, "client-under-test");
        t.setDaemon(true);
        t.start();
        return t;
    }

    /** Straight to the server, not through the proxy: this is the test's own view of truth. */
    private String serverStatus() throws Exception {
        HttpResponse<String> res = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + serverPort + "/status")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, res.statusCode());
        return res.body();
    }

    private static String roundTrip(int port, String message) throws Exception {
        try (Socket sock = connect(port)) {
            sock.setSoTimeout(20_000);
            OutputStream out = sock.getOutputStream();
            out.write((message + "\n").getBytes(StandardCharsets.UTF_8));
            out.flush();
            InputStream in = sock.getInputStream();
            StringBuilder sb = new StringBuilder();
            int c;
            while ((c = in.read()) != -1 && c != '\n') {
                sb.append((char) c);
            }
            return sb.toString();
        }
    }

    /** Retry until the client has settled on a transport that works, or give up loudly. */
    private static Socket connect(int port) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (true) {
            try {
                return new Socket("127.0.0.1", port);
            } catch (ConnectException e) {
                if (System.currentTimeMillis() > deadline) {
                    fail("the client never opened its local listener on " + port);
                }
                Thread.sleep(200);
            }
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
