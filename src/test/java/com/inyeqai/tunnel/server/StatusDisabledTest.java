package com.inyeqai.tunnel.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

/** {@code tunnel.status=false} должен убрать эндпойнт, а не просто спрятать его содержимое. */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"tunnel.auth=tunnel:off", "tunnel.status=false"})
class StatusDisabledTest {

    @LocalServerPort
    int serverPort;

    @Test
    void statusIsGoneEvenWithTheRightToken() throws Exception {
        HttpRequest req = HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + serverPort + "/status"))
                .header("X-Tunnel-Auth", "tunnel:off")
                .build();
        HttpResponse<String> res = HttpClient.newHttpClient()
                .send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, res.statusCode());
    }

    @Test
    void healthzStillWorks() throws Exception {
        HttpRequest req = HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + serverPort + "/healthz")).build();
        HttpResponse<String> res = HttpClient.newHttpClient()
                .send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, res.statusCode());
        assertEquals("ok", res.body());
    }
}
