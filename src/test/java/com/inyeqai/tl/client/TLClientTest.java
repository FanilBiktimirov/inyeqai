package com.inyeqai.tl.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TLClientTest {

    @Test
    void bareHostBecomesWssWithTheDefaultPath() {
        assertEquals("wss://alfa.example/TL", TLClient.normalizeUrl("alfa.example"));
        assertEquals("wss://alfa.example/TL", TLClient.normalizeUrl("wss://alfa.example/"));
        assertEquals("wss://alfa.example/TL", TLClient.normalizeUrl("https://alfa.example"));
        assertEquals("ws://127.0.0.1:8080/TL", TLClient.normalizeUrl("http://127.0.0.1:8080"));
    }

    @Test
    void anExplicitPathIsLeftAlone() {
        assertEquals("wss://alfa.example/custom", TLClient.normalizeUrl("wss://alfa.example/custom"));
    }

    @Test
    void theHttpEndpointsMirrorTheWebSocketUrl() {
        assertEquals("https://alfa.example/TL/http", HttpLink.httpBase("wss://alfa.example/TL"));
        assertEquals("http://127.0.0.1:8080/TL/http", HttpLink.httpBase("ws://127.0.0.1:8080/TL"));
        assertEquals("https://alfa.example/custom/http", HttpLink.httpBase("wss://alfa.example/custom/"));
    }

    @Test
    void backoffGrowsAndIsCapped() {
        assertEquals(1_000, TLClient.backoff(0));
        assertEquals(2_000, TLClient.backoff(1));
        assertEquals(4_000, TLClient.backoff(2));
        long cap = 5 * 60_000;
        assertEquals(cap, TLClient.backoff(30), "a long run of failures must not overflow the shift");
        assertTrue(TLClient.backoff(9) <= cap);
    }
}
