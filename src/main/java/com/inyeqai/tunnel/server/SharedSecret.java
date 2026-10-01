package com.inyeqai.tunnel.server;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * The {@code --auth} token, and the single place that decides whether a presented one is
 * acceptable. Both the WebSocket handshake and {@code GET /status} go through here, so the
 * two cannot drift apart into different ideas of what counts as authorised.
 *
 * <p>An empty configured token means no check at all. That is a deliberate dev-only mode,
 * documented loudly in the README, and it applies uniformly: an open server has an open
 * status page too, rather than a surprising exception in one of the two.
 */
final class SharedSecret {

    private final byte[] expected;

    SharedSecret(String expected) {
        this.expected = (expected == null ? "" : expected).getBytes(StandardCharsets.UTF_8);
    }

    boolean open() {
        return expected.length == 0;
    }

    /** Constant-time comparison, so a wrong token cannot be guessed byte by byte. */
    boolean accepts(String presented) {
        if (open()) {
            return true;
        }
        if (presented == null) {
            return false;
        }
        return MessageDigest.isEqual(expected, presented.getBytes(StandardCharsets.UTF_8));
    }
}
