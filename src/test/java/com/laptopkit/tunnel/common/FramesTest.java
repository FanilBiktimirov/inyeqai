package com.laptopkit.tunnel.common;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;

class FramesTest {

    @Test
    void openRoundTrips() {
        Frame f = Frames.decode(Frames.open(7, "host.docker.internal", 3129));
        assertEquals(Frames.OPEN, f.type);
        assertEquals(7, f.streamId);
        assertEquals("host.docker.internal", f.host);
        assertEquals(3129, f.port);
    }

    @Test
    void openCarriesHighPortsAndNegativeIds() {
        // The server tags its ids with the top bit, so streamId is routinely negative, and
        // ports above 32767 must survive the 16-bit field unsigned.
        Frame f = Frames.decode(Frames.open(0x80000001, "h", 65535));
        assertEquals(0x80000001, f.streamId);
        assertEquals(65535, f.port);
    }

    @Test
    void dataRoundTripsASlice() {
        byte[] buf = "xxhelloxx".getBytes(StandardCharsets.UTF_8);
        Frame f = Frames.decode(Frames.data(3, buf, 2, 5));
        assertEquals(Frames.DATA, f.type);
        assertEquals(3, f.streamId);
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), f.payload);
    }

    @Test
    void emptyDataRoundTrips() {
        Frame f = Frames.decode(Frames.data(3, new byte[0], 0, 0));
        assertEquals(Frames.DATA, f.type);
        assertEquals(0, f.payload.length);
    }

    @Test
    void closeEofAndWindowRoundTrip() {
        assertEquals(Frames.CLOSE, Frames.decode(Frames.close(9)).type);
        assertEquals(9, Frames.decode(Frames.close(9)).streamId);

        Frame eof = Frames.decode(Frames.eof(11));
        assertEquals(Frames.EOF, eof.type);
        assertEquals(11, eof.streamId);

        Frame win = Frames.decode(Frames.window(13, 4096));
        assertEquals(Frames.WINDOW, win.type);
        assertEquals(13, win.streamId);
        assertEquals(4096, win.credit);
    }

    @Test
    void configRoundTripsEveryField() {
        List<Reverse> rs = List.of(
                new Reverse("0.0.0.0", 3130, "host.docker.internal", 3129),
                new Reverse("127.0.0.1", 65535, "::1", 1));
        Frame f = Frames.decode(Frames.config(rs));
        assertEquals(Frames.CONFIG, f.type);
        assertEquals(rs, f.reverses);
    }

    @Test
    void rejectsMalformedFrames() {
        assertThrows(IllegalArgumentException.class, () -> Frames.decode(new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> Frames.decode(new byte[] {99, 0, 0, 0, 1}));
        // CLOSE missing most of its stream id
        assertThrows(IllegalArgumentException.class, () -> Frames.decode(new byte[] {Frames.CLOSE, 0}));
        // OPEN claiming a host longer than the frame
        assertThrows(IllegalArgumentException.class,
                () -> Frames.decode(new byte[] {Frames.OPEN, 0, 0, 0, 1, 0, 40, 'a'}));
        // WINDOW with no credit in it is a protocol error, not a no-op
        assertThrows(IllegalArgumentException.class,
                () -> Frames.decode(new byte[] {Frames.WINDOW, 0, 0, 0, 1, 0, 0, 0, 0}));
    }
}
