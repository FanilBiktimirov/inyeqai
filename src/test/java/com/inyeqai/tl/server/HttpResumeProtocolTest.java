package com.inyeqai.tl.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import com.inyeqai.tl.common.Frames;
import com.inyeqai.tl.common.Framing;
import com.inyeqai.tl.common.Reverse;

/**
 * The resumption rules, driven by hand over HTTP so each one can be put wrong on purpose.
 *
 * <p>These are the cases where getting it wrong is silent. A retried batch applied twice doubles
 * bytes inside a stream; a skipped batch loses them; a resume from a point the server can no
 * longer produce leaves a hole. None of that surfaces as an error anywhere in the tunnel &mdash;
 * the bytes simply come out wrong at the far end, which is why every one of these has to be
 * refused loudly here rather than tolerated.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class HttpResumeProtocolTest {

    @LocalServerPort
    int serverPort;

    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1).build();

    private ByteSink sink;

    @AfterEach
    void closeSink() throws IOException {
        if (sink != null) {
            sink.close();
        }
    }

    @Test
    @Timeout(60)
    void aRetriedBatchIsAnsweredButNotAppliedAgain() throws Exception {
        sink = new ByteSink();
        String id = connect();

        // One stream carrying five bytes, then the very same request again, as a client that
        // never saw our answer would send it.
        byte[] batch = frames(
                Frames.open(1, "127.0.0.1", sink.port()),
                Frames.data(1, "hello".getBytes(StandardCharsets.UTF_8), 0, 5));
        assertEquals(204, postBatch(id, 1, batch).statusCode());
        awaitStatusContains("from clients 5 B");

        assertEquals(204, postBatch(id, 1, batch).statusCode(),
                "a retry must be answered, since the client could not tell our answer was lost");
        Thread.sleep(500);
        assertTrue(status().contains("from clients 5 B"),
                "the retry must not be applied a second time:\n" + status());
    }

    @Test
    @Timeout(60)
    void aBatchThatSkipsOneEndsTheSession() throws Exception {
        sink = new ByteSink();
        String id = connect();
        assertEquals(204, postBatch(id, 1, frames(Frames.ping())).statusCode());

        // Batch 2 never arrives. Carrying on from 3 would mean silently dropping whatever was
        // in it, so the session has to end instead.
        HttpResponse<String> res = postBatch(id, 3, frames(Frames.ping()));
        assertEquals(410, res.statusCode(), res.body());
        awaitSessionGone(id);
    }

    @Test
    @Timeout(60)
    void resumingFromAFrameThatWasNeverSentEndsTheSession() throws Exception {
        String id = connect();
        try (InputStream down = attach(id, 0)) {
            assertNotNull(Framing.readSequenced(Framing.reader(down)), "expected the greeting");
        }
        // Nowhere near that many frames exist; a client claiming otherwise has lost track of
        // where it is, and anything we sent it from here would be guesswork.
        assertEquals(410, attachStatus(id, 9_999));
        awaitSessionGone(id);
    }

    @Test
    @Timeout(60)
    void aSessionOutlivesItsDownstreamResponseAndKeepsItsReverseListener() throws Exception {
        int reversePort = freePort();
        String id = connect();

        long lastSeq;
        try (InputStream down = attach(id, 0)) {
            lastSeq = Framing.readSequenced(Framing.reader(down)).seq();
            postBatch(id, 1, frames(Frames.config(
                    List.of(new Reverse("127.0.0.1", reversePort, "127.0.0.1", 9)))));
            awaitBound(reversePort);
        } // the response ends here, exactly as a capped gateway would end it

        // A whole second with no downstream at all, then back for the rest. The listener the
        // session was holding has to still be there: resuming a session rather than rebuilding
        // one is the difference between keeping its streams and dropping them.
        Thread.sleep(1_000);
        assertTrue(portIsBound(reversePort),
                "the reverse listener should outlive the response that asked for it");

        try (InputStream down = attach(id, lastSeq)) {
            assertTrue(status().contains("session " + id), status());
            assertTrue(portIsBound(reversePort), "the listener should still be there after resuming");
        }
    }

    /** A destination that accepts connections and reads whatever arrives. */
    private static final class ByteSink implements AutoCloseable {
        private final ServerSocket listener;

        ByteSink() throws IOException {
            listener = new ServerSocket();
            listener.bind(new InetSocketAddress("127.0.0.1", 0));
            Thread t = new Thread(() -> {
                while (!listener.isClosed()) {
                    try {
                        Socket s = listener.accept();
                        Thread reader = new Thread(() -> {
                            try (Socket open = s) {
                                byte[] buf = new byte[4096];
                                while (open.getInputStream().read(buf) != -1) {
                                    // the point is only that the bytes are taken
                                }
                            } catch (IOException ignored) {
                            }
                        }, "sink-reader");
                        reader.setDaemon(true);
                        reader.start();
                    } catch (IOException e) {
                        return;
                    }
                }
            }, "sink-accept");
            t.setDaemon(true);
            t.start();
        }

        int port() {
            return listener.getLocalPort();
        }

        @Override
        public void close() throws IOException {
            listener.close();
        }
    }

    private String connect() throws Exception {
        HttpResponse<String> res = http.send(
                HttpRequest.newBuilder(uri("/tl/http/connect"))
                        .POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, res.statusCode());
        return res.body().trim();
    }

    private InputStream attach(String id, long from) throws Exception {
        HttpResponse<InputStream> res = http.send(
                HttpRequest.newBuilder(uri("/tl/http/down/" + id + "?from=" + from)).GET().build(),
                HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(200, res.statusCode());
        return res.body();
    }

    private int attachStatus(String id, long from) throws Exception {
        HttpResponse<InputStream> res = http.send(
                HttpRequest.newBuilder(uri("/tl/http/down/" + id + "?from=" + from)).GET().build(),
                HttpResponse.BodyHandlers.ofInputStream());
        res.body().close();
        return res.statusCode();
    }

    private HttpResponse<String> postBatch(String id, long batch, byte[] body) throws Exception {
        return http.send(
                HttpRequest.newBuilder(uri("/tl/http/up/" + id + "?batch=" + batch))
                        .timeout(Duration.ofSeconds(10))
                        .header("Content-Type", "application/octet-stream")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static byte[] frames(byte[]... frames) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] frame : frames) {
            Framing.write(out, frame);
        }
        return out.toByteArray();
    }

    private String status() throws Exception {
        return http.send(HttpRequest.newBuilder(uri("/status")).build(),
                HttpResponse.BodyHandlers.ofString()).body();
    }

    /**
     * Wait for one session to disappear from {@code /status}. Named per session on purpose: this
     * context is shared with the other tests here, so asserting that no sessions at all remain
     * would be asserting something about them rather than about this one.
     */
    private void awaitSessionGone(String id) throws Exception {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            if (!status().contains("session " + id)) {
                return;
            }
            Thread.sleep(100);
        }
        fail("/status still shows session " + id + ":\n" + status());
    }

    private void awaitStatusContains(String text) throws Exception {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            if (status().contains(text)) {
                return;
            }
            Thread.sleep(100);
        }
        fail("/status never said " + text + ":\n" + status());
    }

    private static void awaitBound(int port) throws Exception {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            if (portIsBound(port)) {
                return;
            }
            Thread.sleep(100);
        }
        fail("the server never opened the reverse listener on " + port);
    }

    private static boolean portIsBound(int port) {
        try (Socket s = new Socket("127.0.0.1", port)) {
            return true;
        } catch (ConnectException e) {
            return false;
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
