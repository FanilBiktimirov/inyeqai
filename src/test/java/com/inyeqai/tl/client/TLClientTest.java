package com.inyeqai.tl.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TLClientTest {

    @Test
    void bareHostBecomesWssWithTheDefaultPath() {
        assertEquals("wss://alfa.example/tl", TLClient.normalizeUrl("alfa.example"));
        assertEquals("wss://alfa.example/tl", TLClient.normalizeUrl("wss://alfa.example/"));
        assertEquals("wss://alfa.example/tl", TLClient.normalizeUrl("https://alfa.example"));
        assertEquals("ws://127.0.0.1:8080/tl", TLClient.normalizeUrl("http://127.0.0.1:8080"));
    }

    @Test
    void anExplicitPathIsLeftAlone() {
        assertEquals("wss://alfa.example/custom", TLClient.normalizeUrl("wss://alfa.example/custom"));
    }

    @Test
    void durationsParseToSeconds() {
        assertEquals(25, TLClient.parseDuration("25s"));
        assertEquals(25, TLClient.parseDuration("25"));
        assertEquals(120, TLClient.parseDuration("2m"));
        assertEquals(1, TLClient.parseDuration("1500ms"));
    }

    @Test
    void zeroMeansKeepaliveOff() {
        // "0s" is the way to disable keepalive; it must parse to 0 rather
        // than throw or round up, since a zero period would otherwise reach the scheduler.
        assertEquals(0, TLClient.parseDuration("0s"));
        assertEquals(0, TLClient.parseDuration("0"));
        assertEquals(0, TLClient.parseDuration("0ms"));
    }

    @Test
    void rejectsBadDurations() {
        assertThrows(IllegalArgumentException.class, () -> TLClient.parseDuration("soon"));
        assertThrows(IllegalArgumentException.class, () -> TLClient.parseDuration("-5s"));
    }

    @Test
    void transportNamesParse() {
        assertEquals(Transport.WEBSOCKET, TLClient.parseTransport("ws"));
        assertEquals(Transport.WEBSOCKET, TLClient.parseTransport("websocket"));
        assertEquals(Transport.HTTP, TLClient.parseTransport("http"));
        assertEquals(Transport.HTTP, TLClient.parseTransport(" HTTP "));
    }

    @Test
    void autoIsTheAbsenceOfAChoice() {
        // Null rather than a third enum value: "auto" is not a transport the client can open,
        // it is the decision to find out which one works.
        assertNull(TLClient.parseTransport("auto"));
        assertNull(TLClient.parseTransport("AUTO"));
    }

    @Test
    void rejectsUnknownTransports() {
        assertThrows(IllegalArgumentException.class, () -> TLClient.parseTransport("socks"));
        assertThrows(IllegalArgumentException.class, () -> TLClient.parseTransport(""));
    }

    @Test
    void theHttpEndpointsMirrorTheWebSocketUrl() {
        assertEquals("https://alfa.example/tl/http", HttpLink.httpBase("wss://alfa.example/tl"));
        assertEquals("http://127.0.0.1:8080/tl/http", HttpLink.httpBase("ws://127.0.0.1:8080/tl"));
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
