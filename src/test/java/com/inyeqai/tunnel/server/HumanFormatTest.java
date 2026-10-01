package com.inyeqai.tunnel.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.inyeqai.tunnel.common.Mux;

/** The status view exists to be read at a glance, so its two formatters are worth pinning. */
class HumanFormatTest {

    @Test
    void byteCountsReadAsSizes() {
        assertEquals("0 B", Mux.bytes(0));
        assertEquals("1023 B", Mux.bytes(1023));
        assertEquals("1.0 KB", Mux.bytes(1024));
        assertEquals("16.0 KB", Mux.bytes(16 * 1024));
        assertEquals("1.0 MB", Mux.bytes(1024 * 1024));
        assertEquals("4.0 MB", Mux.bytes(4L * 1024 * 1024));
        assertEquals("1.00 GB", Mux.bytes(1024L * 1024 * 1024));
    }

    @Test
    void durationsReadAsTimes() {
        assertEquals("0ms", StatusController.duration(0));
        assertEquals("999ms", StatusController.duration(999));
        assertEquals("1.0s", StatusController.duration(1000));
        assertEquals("4.2s", StatusController.duration(4234));
        assertEquals("1m00s", StatusController.duration(60_000));
        assertEquals("3m12s", StatusController.duration(192_000));
        assertEquals("1h00m", StatusController.duration(3_600_000));
        assertEquals("2h05m", StatusController.duration(7_500_000));
    }
}
