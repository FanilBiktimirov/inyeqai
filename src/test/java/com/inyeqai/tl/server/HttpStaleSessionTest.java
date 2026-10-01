package com.inyeqai.tl.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import com.inyeqai.tl.common.Frames;
import com.inyeqai.tl.common.Framing;
import com.inyeqai.tl.common.Reverse;

/**
 * The same failure the WebSocket transport is tested against, on the HTTP one: a client that
 * stops answering without closing anything. A suspended laptop leaves the server holding a TCP
 * connection nobody is on the other end of, and with it the reverse ports that session bound.
 *
 * <p>On the HTTP carrier there is no TCP close to wait for either, and the keepalive has to do
 * the same job through ordinary frames. This drives the endpoints by hand, with no client
 * involved, so the session is genuinely mute: it opens a downstream, asks for a reverse
 * listener, and then never answers another thing.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"tl.keepalive=1s", "tl.pong-timeout=2s"})
class HttpStaleSessionTest {

    @LocalServerPort
    int serverPort;

    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1).build();

    @Test
    @Timeout(90)
    void aMuteHttpSessionStopsHoldingItsReversePort() throws Exception {
        int reversePort = freePort();

        HttpResponse<String> connect = http.send(
                HttpRequest.newBuilder(uri("/tl/http/connect"))
                        .POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, connect.statusCode());
        String id = connect.body().trim();

        // Open the downstream and leave it open. Nothing is ever posted back, so no pong
        // reaches the server: this is what a vanished client looks like from here.
        HttpResponse<InputStream> down = http.send(
                HttpRequest.newBuilder(uri("/tl/http/down/" + id)).GET().build(),
                HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(200, down.statusCode());

        try (InputStream ignored = down.body()) {
            askForReverseListener(id, reversePort);
            awaitBound(reversePort);

            // Past the pong timeout the session must be given up on, and its port with it.
            Thread.sleep(5_000);
            assertTrue(portIsFree(reversePort),
                    "the reverse port should be released once the session stops answering");
        }
    }

    private void askForReverseListener(String id, int reversePort) throws Exception {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        Framing.write(body, Frames.config(
                List.of(new Reverse("127.0.0.1", reversePort, "127.0.0.1", 9)))); // 9 = discard
        HttpResponse<Void> res = http.send(
                HttpRequest.newBuilder(uri("/tl/http/up/" + id + "?batch=1"))
                        .timeout(Duration.ofSeconds(10))
                        .header("Content-Type", "application/octet-stream")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body.toByteArray())).build(),
                HttpResponse.BodyHandlers.discarding());
        assertEquals(204, res.statusCode());
    }

    private static void awaitBound(int port) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            try (Socket s = new Socket("127.0.0.1", port)) {
                return;
            } catch (ConnectException e) {
                Thread.sleep(100);
            }
        }
        fail("the server never opened the reverse listener on " + port);
    }

    /** Binding it ourselves is the proof: a refused connection could be a passing race. */
    private static boolean portIsFree(int port) {
        try (ServerSocket s = new ServerSocket(port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private URI uri(String path) {
        return URI.create("http://127.0.0.1:" + serverPort + path);
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
