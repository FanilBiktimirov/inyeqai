package com.inyeqai.tunnel.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ForwardSpecTest {

    @Test
    void parsesTheFourPartForm() {
        ForwardSpec s = ForwardSpec.parse("0.0.0.0:3128:127.0.0.1:3129");
        assertFalse(s.reverse());
        assertEquals("0.0.0.0", s.bindHost());
        assertEquals(3128, s.bindPort());
        assertEquals("127.0.0.1", s.dstHost());
        assertEquals(3129, s.dstPort());
    }

    @Test
    void parsesTheThreePartFormWithTheDefaultBind() {
        ForwardSpec s = ForwardSpec.parse("3128:example.com:80");
        assertEquals("127.0.0.1", s.bindHost(), "a local forward binds loopback by default");
        assertEquals(3128, s.bindPort());
        assertEquals("example.com", s.dstHost());
        assertEquals(80, s.dstPort());
    }

    @Test
    void reverseDefaultsToBindingEveryInterface() {
        ForwardSpec s = ForwardSpec.parse("R:3130:host.docker.internal:3129");
        assertTrue(s.reverse());
        assertEquals("0.0.0.0", s.bindHost());
        assertEquals(3130, s.bindPort());
        Reverse r = s.toReverse();
        assertEquals(3130, r.serverPort());
        assertEquals("host.docker.internal", r.clientHost());
        assertEquals(3129, r.clientPort());
    }

    @Test
    void shortFormsFillInFromTheBack() {
        ForwardSpec bare = ForwardSpec.parse("3000");
        assertEquals(3000, bare.bindPort());
        assertEquals("127.0.0.1", bare.dstHost());
        assertEquals(3000, bare.dstPort());

        ForwardSpec hostPort = ForwardSpec.parse("example.com:3000");
        assertEquals(3000, hostPort.bindPort());
        assertEquals("example.com", hostPort.dstHost());
        assertEquals(3000, hostPort.dstPort());
    }

    @Test
    void bracketedIpv6StaysOnePieceAndLosesItsBrackets() {
        ForwardSpec s = ForwardSpec.parse("[::1]:3128:[fe80::1]:3129");
        assertEquals("::1", s.bindHost());
        assertEquals(3128, s.bindPort());
        assertEquals("fe80::1", s.dstHost());
        assertEquals(3129, s.dstPort());
    }

    @Test
    void reverseIpv6KeepsTheReverseFlag() {
        ForwardSpec s = ForwardSpec.parse("R:[::]:3130:[::1]:3129");
        assertTrue(s.reverse());
        assertEquals("::", s.bindHost());
        assertEquals("::1", s.dstHost());
    }

    @Test
    void rejectsNonsense() {
        assertThrows(IllegalArgumentException.class, () -> ForwardSpec.parse("a:b:c:d:e"));
        assertThrows(IllegalArgumentException.class, () -> ForwardSpec.parse("3128:host:notaport"));
        assertThrows(IllegalArgumentException.class, () -> ForwardSpec.parse("3128:host:0"));
        assertThrows(IllegalArgumentException.class, () -> ForwardSpec.parse("3128:host:65536"));
        assertThrows(IllegalArgumentException.class, () -> ForwardSpec.parse("[::1:3128:host:80"));
        assertThrows(IllegalArgumentException.class, () -> ForwardSpec.parse("3128::80"));
    }

    @Test
    void toReverseRefusesAForwardSpec() {
        assertThrows(IllegalStateException.class, () -> ForwardSpec.parse("3128:host:80").toReverse());
    }
}
