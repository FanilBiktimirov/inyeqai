package com.inyeqai.tunnel.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class StreamInfoTest {

    @Test
    void clientIdsReadAsThemselves() {
        assertEquals("1", new StreamInfo(1, "h:1", 0, 0, 0).label());
        assertEquals("42", new StreamInfo(42, "h:1", 0, 0, 0).label());
    }

    @Test
    void serverIdsReadUnsignedRatherThanNegative() {
        // The server sets the top bit of every id it allocates, so as a signed int the first
        // one is -2147483647. Logs on both ends have to show the same token, and a minus sign
        // in front of a stream id reads like an error.
        int firstServerId = 1 | 0x80000000;
        assertTrue(firstServerId < 0, "sanity: the raw id really is negative");
        assertEquals("2147483649", new StreamInfo(firstServerId, "h:1", 0, 0, 0).label());
        assertEquals("4294967295", new StreamInfo(-1, "h:1", 0, 0, 0).label());
    }
}
