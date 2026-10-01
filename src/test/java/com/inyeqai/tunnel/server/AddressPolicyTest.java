package com.inyeqai.tunnel.server;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

class AddressPolicyTest {

    @Test
    void anEmptyListAllowsEverything() {
        AddressPolicy p = new AddressPolicy(List.of());
        assertTrue(p.unrestricted());
        assertTrue(p.allowsDial("anywhere.example", 443));
        assertTrue(p.allowsReverse("0.0.0.0", 3130));
    }

    @Test
    void blankEntriesAreIgnoredRatherThanMatchingEverything() {
        // A stray empty line in configuration must not silently open the server: an empty
        // pattern would match every address.
        AddressPolicy p = new AddressPolicy(Arrays.asList("", "   ", "^127\\.0\\.0\\.1:3129$"));
        assertFalse(p.unrestricted());
        assertTrue(p.allowsDial("127.0.0.1", 3129));
        assertFalse(p.allowsDial("127.0.0.1", 22));
    }

    @Test
    void theChainsTwoAddressesAreAllowedAndNothingElse() {
        AddressPolicy p = new AddressPolicy(List.of("^127\\.0\\.0\\.1:3129$", "^R:0\\.0\\.0\\.0:3130$"));
        assertTrue(p.allowsDial("127.0.0.1", 3129));
        assertTrue(p.allowsReverse("0.0.0.0", 3130));

        assertFalse(p.allowsDial("127.0.0.1", 22));
        assertFalse(p.allowsDial("169.254.169.254", 80), "cloud metadata must not be reachable");
        assertFalse(p.allowsReverse("0.0.0.0", 2222));
        // A forward pattern must not be usable to open a reverse listener, or vice versa.
        assertFalse(p.allowsReverse("127.0.0.1", 3129));
    }

    @Test
    void aBadPatternFailsLoudlyInsteadOfSilentlyDenying() {
        assertThrows(IllegalArgumentException.class, () -> new AddressPolicy(List.of("[unclosed")));
    }
}
