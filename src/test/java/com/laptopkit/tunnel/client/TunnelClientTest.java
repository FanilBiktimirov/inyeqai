package com.laptopkit.tunnel.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    void durationsParseToSeconds() {
        assertEquals(25, TunnelClient.parseDuration("25s"));
        assertEquals(25, TunnelClient.parseDuration("25"));
        assertEquals(120, TunnelClient.parseDuration("2m"));
        assertEquals(1, TunnelClient.parseDuration("1500ms"));
    }

    @Test
    void zeroMeansKeepaliveOff() {
        // chisel documents "0s" as the way to disable keepalive; it must parse to 0 rather
        // than throw or round up, since a zero period would otherwise reach the scheduler.
        assertEquals(0, TunnelClient.parseDuration("0s"));
        assertEquals(0, TunnelClient.parseDuration("0"));
        assertEquals(0, TunnelClient.parseDuration("0ms"));
    }

    @Test
    void rejectsBadDurations() {
        assertThrows(IllegalArgumentException.class, () -> TunnelClient.parseDuration("soon"));
        assertThrows(IllegalArgumentException.class, () -> TunnelClient.parseDuration("-5s"));
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
