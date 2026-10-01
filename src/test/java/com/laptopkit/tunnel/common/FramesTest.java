package com.laptopkit.tunnel.common;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        // Сервер помечает свои id старшим битом, поэтому streamId сплошь и рядом отрицательный,
        // а порты выше 32767 должны пережить 16-битное поле без знака.
        Frame f = Frames.decode(Frames.open(0x80000001, "h", 65535));
        assertEquals(0x80000001, f.streamId);
        assertEquals(65535, f.port);
    }

    @Test
    void ackRoundTripsItsSequence() {
        Frame f = Frames.decode(Frames.ack(123456789012L));
        assertEquals(Frames.ACK, f.type);
        assertEquals(123456789012L, f.ackThrough);
        assertTrue(Frames.isCarrier(f.type));
    }

    @Test
    void ackZeroMeansNothingReceivedYet() {
        assertEquals(0L, Frames.decode(Frames.ack(0)).ackThrough);
    }

    @Test
    void carrierFramesRoundTripAndAreRecognisedAsSuch() {
        for (byte[] encoded : new byte[][]{Frames.ping(), Frames.pong(), Frames.bye()}) {
            Frame f = Frames.decode(encoded);
            assertEquals(encoded[0], f.type);
            assertTrue(Frames.isCarrier(f.type), "type " + f.type + " belongs to the carrier");
        }
    }

    @Test
    void tunnelFramesAreNotCarrierFrames() {
        // Транспорты вылавливают транспортные кадры, а всё остальное отдают в мультиплексор,
        // поэтому туннельный кадр, ошибочно записанный в транспортные, просто исчезнет.
        assertFalse(Frames.isCarrier(Frames.OPEN));
        assertFalse(Frames.isCarrier(Frames.DATA));
        assertFalse(Frames.isCarrier(Frames.CLOSE));
        assertFalse(Frames.isCarrier(Frames.CONFIG));
        assertFalse(Frames.isCarrier(Frames.EOF));
        assertFalse(Frames.isCarrier(Frames.WINDOW));
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
        // CLOSE, у которого обрезана почти вся часть с id потока
        assertThrows(IllegalArgumentException.class, () -> Frames.decode(new byte[] {Frames.CLOSE, 0}));
        // OPEN, который обещает хост длиннее самого кадра
        assertThrows(IllegalArgumentException.class,
                () -> Frames.decode(new byte[] {Frames.OPEN, 0, 0, 0, 1, 0, 40, 'a'}));
        // WINDOW без кредита внутри — ошибка протокола, а не пустая операция
        assertThrows(IllegalArgumentException.class,
                () -> Frames.decode(new byte[] {Frames.WINDOW, 0, 0, 0, 1, 0, 0, 0, 0}));
    }
}
