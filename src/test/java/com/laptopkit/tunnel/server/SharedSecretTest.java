package com.laptopkit.tunnel.server;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SharedSecretTest {

    @Test
    void noTokenConfiguredAcceptsAnything() {
        SharedSecret open = new SharedSecret("");
        assertTrue(open.open());
        assertTrue(open.accepts(null));
        assertTrue(open.accepts("whatever"));

        assertTrue(new SharedSecret(null).open(), "an unset token behaves like an empty one");
    }

    @Test
    void aConfiguredTokenMustMatchExactly() {
        SharedSecret s = new SharedSecret("tunnel:hunter2");
        assertFalse(s.open());
        assertTrue(s.accepts("tunnel:hunter2"));

        assertFalse(s.accepts(null), "a missing header is not a pass");
        assertFalse(s.accepts(""));
        assertFalse(s.accepts("tunnel:hunter"), "a prefix must not be accepted");
        assertFalse(s.accepts("tunnel:hunter2x"));
        assertFalse(s.accepts("TUNNEL:hunter2"));
    }
}
