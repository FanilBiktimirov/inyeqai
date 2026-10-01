package com.inyeqai.tunnel.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TunnelClientTest {

    @Test
    void bareHostBecomesWssWithTheDefaultPath() {
        assertEquals("wss://alfa.example/tunnel", TunnelClient.normalizeUrl("alfa.example"));
        assertEquals("wss://alfa.example/tunnel", TunnelClient.normalizeUrl("wss://alfa.example/"));
        assertEquals("wss://alfa.example/tunnel", TunnelClient.normalizeUrl("https://alfa.example"));
        assertEquals("ws://127.0.0.1:8080/tunnel", TunnelClient.normalizeUrl("http://127.0.0.1:8080"));
    }

    @Test
    void anExplicitPathIsLeftAlone() {
        assertEquals("wss://alfa.example/custom", TunnelClient.normalizeUrl("wss://alfa.example/custom"));
    }

    @Test
    void theHttpEndpointsMirrorTheWebSocketUrl() {
        assertEquals("https://alfa.example/tunnel/http", HttpLink.httpBase("wss://alfa.example/tunnel"));
        assertEquals("http://127.0.0.1:8080/tunnel/http", HttpLink.httpBase("ws://127.0.0.1:8080/tunnel"));
        assertEquals("https://alfa.example/custom/http", HttpLink.httpBase("wss://alfa.example/custom/"));
    }

    @Test
    void backoffGrowsAndIsCapped() {
        assertEquals(1_000, TunnelClient.backoff(0));
        assertEquals(2_000, TunnelClient.backoff(1));
        assertEquals(4_000, TunnelClient.backoff(2));
        long cap = 5 * 60_000;
        assertEquals(cap, TunnelClient.backoff(30), "a long run of failures must not overflow the shift");
        assertTrue(TunnelClient.backoff(9) <= cap);
    }
}
