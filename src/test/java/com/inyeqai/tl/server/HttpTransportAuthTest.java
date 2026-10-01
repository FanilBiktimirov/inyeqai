package com.inyeqai.tl.server;

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
 * Второй вход — это второй способ ошибиться с токеном. WebSocket-эндпойнт без токена не даёт
 * рукопожатия; каждый эндпойнт HTTP-транспорта должен отказывать так же твёрдо, потому что
 * любого из них в одиночку хватает, чтобы пользоваться туннелем: {@code /connect} открывает
 * сессию, {@code /up} вливает в неё кадры, {@code /down} читает то, что отправляет сервер.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "TL.auth=TL:secret")
class HttpTransportAuthTest {

    @LocalServerPort
    int serverPort;

    @Test
    void connectWithoutATokenIsRefused() throws Exception {
        assertEquals(401, post("/TL/http/connect", null).statusCode());
    }

    @Test
    void connectWithTheWrongTokenIsRefused() throws Exception {
        assertEquals(401, post("/TL/http/connect", "TL:wrong").statusCode());
    }

    @Test
    void postingFramesIntoAnUnknownSessionWithoutATokenIsRefused() throws Exception {
        // 401, а не 404: тот, кто не прошёл аутентификацию, не должен даже узнать, существует
        // ли такой id сессии.
        assertEquals(401, post("/TL/http/up/http-whatever?batch=1", null).statusCode());
    }

    @Test
    void withoutATokenNothingAboutASessionIsRevealed() throws Exception {
        // Гарантия про сессии, а не про эндпойнт: 401 и так говорит вызывающему, что путь есть,
        // и это нормально. Различаться не должны ответы для существующей сессии и для той,
        // которой нет, иначе их можно перебрать без аутентификации.
        HttpResponse<String> real = post("/TL/http/connect", "TL:secret");
        String existing = real.body().trim();
        assertEquals(401, post("/TL/http/up/" + existing + "?batch=1", null).statusCode());
        assertEquals(401, post("/TL/http/up/http-no-such-session?batch=1", null).statusCode());
    }

    @Test
    void aBatchWithoutANumberIsRefused() throws Exception {
        // Дальше токена: пачку без номера не отличить от повтора, поэтому ей отказывают,
        // а не угадывают номер.
        assertEquals(400, post("/TL/http/up/http-whatever", "TL:secret").statusCode());
    }

    @Test
    void readingTheDownstreamWithoutATokenIsRefused() throws Exception {
        HttpResponse<String> res = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(uri("/TL/http/down/http-whatever")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(401, res.statusCode());
    }

    @Test
    void theRightTokenOpensASession() throws Exception {
        HttpResponse<String> res = post("/TL/http/connect", "TL:secret");
        assertEquals(200, res.statusCode());
        assertTrue(res.body().startsWith("http-"), res.body());
    }

    private HttpResponse<String> post(String path, String token) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(uri(path))
                .POST(HttpRequest.BodyPublishers.noBody());
        if (token != null) {
            b.header("X-TL-Auth", token);
        }
        return HttpClient.newHttpClient().send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private URI uri(String path) {
        return URI.create("http://127.0.0.1:" + serverPort + path);
    }
}
