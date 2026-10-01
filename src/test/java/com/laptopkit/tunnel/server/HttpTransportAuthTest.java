package com.laptopkit.tunnel.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

/**
 * A second way in is a second way to get the token wrong. The WebSocket endpoint refuses the
 * handshake without one; every endpoint of the HTTP transport has to refuse just as firmly,
 * because each one on its own is enough to use the tunnel: {@code /connect} opens a session,
 * {@code /up} injects frames into one, {@code /down} reads what the server is sending.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "tunnel.auth=tunnel:secret")
class HttpTransportAuthTest {

    @LocalServerPort
    int serverPort;

    @Test
    void connectWithoutATokenIsRefused() throws Exception {
        assertEquals(401, post("/tunnel/http/connect", null).statusCode());
    }

    @Test
    void connectWithTheWrongTokenIsRefused() throws Exception {
        assertEquals(401, post("/tunnel/http/connect", "tunnel:wrong").statusCode());
    }

    @Test
    void postingFramesIntoAnUnknownSessionWithoutATokenIsRefused() throws Exception {
        // 401 rather than 404: an unauthenticated caller must not even learn whether a
        // session id exists.
        assertEquals(401, post("/tunnel/http/up/http-whatever?batch=1", null).statusCode());
    }

    @Test
    void withoutATokenNothingAboutASessionIsRevealed() throws Exception {
        // The guarantee is about sessions, not about the endpoint: a 401 already tells a caller
        // the path exists, and that is fine. What must not differ is the answer for a session
        // that exists and one that does not, or an unauthenticated caller could enumerate them.
        HttpResponse<String> real = post("/tunnel/http/connect", "tunnel:secret");
        String existing = real.body().trim();
        assertEquals(401, post("/tunnel/http/up/" + existing + "?batch=1", null).statusCode());
        assertEquals(401, post("/tunnel/http/up/http-no-such-session?batch=1", null).statusCode());
    }

    @Test
    void aBatchWithoutANumberIsRefused() throws Exception {
        // Past the token, a batch with no number cannot be told from a retry, so it is refused
        // rather than guessed at.
        assertEquals(400, post("/tunnel/http/up/http-whatever", "tunnel:secret").statusCode());
    }

    @Test
    void readingTheDownstreamWithoutATokenIsRefused() throws Exception {
        HttpResponse<String> res = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(uri("/tunnel/http/down/http-whatever")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(401, res.statusCode());
    }

    @Test
    void theRightTokenOpensASession() throws Exception {
        HttpResponse<String> res = post("/tunnel/http/connect", "tunnel:secret");
        assertEquals(200, res.statusCode());
        assertTrue(res.body().startsWith("http-"), res.body());
    }

    private HttpResponse<String> post(String path, String token) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(uri(path))
                .POST(HttpRequest.BodyPublishers.noBody());
        if (token != null) {
            b.header("X-Tunnel-Auth", token);
        }
        return HttpClient.newHttpClient().send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private URI uri(String path) {
        return URI.create("http://127.0.0.1:" + serverPort + path);
    }
}
