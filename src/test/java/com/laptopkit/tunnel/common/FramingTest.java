package com.laptopkit.tunnel.common;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

/**
 * The length prefix is the only thing keeping frames apart on the HTTP transport, so the
 * interesting cases are the ones where a reader could guess wrong: a stream that ends between
 * frames, which is normal, and one that ends inside a frame, which is corruption.
 */
class FramingTest {

    @Test
    void framesRoundTripBackToBack() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Framing.write(out, Frames.open(1, "host", 3129));
        Framing.write(out, Frames.data(1, "hello".getBytes(StandardCharsets.UTF_8), 0, 5));
        Framing.write(out, Frames.ping());

        DataInputStream in = Framing.reader(new ByteArrayInputStream(out.toByteArray()));
        assertArrayEquals(Frames.open(1, "host", 3129), Framing.read(in));
        Frame data = Frames.decode(Framing.read(in));
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), data.payload);
        assertArrayEquals(Frames.ping(), Framing.read(in));
        assertNull(Framing.read(in), "a stream that ends between frames has simply ended");
    }

    @Test
    void anEmptyStreamIsJustTheEnd() throws IOException {
        assertNull(Framing.read(Framing.reader(new ByteArrayInputStream(new byte[0]))));
    }

    @Test
    void aFrameCutShortIsAnError() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0);
        out.write(0);
        out.write(0);
        out.write(8); // promises eight bytes
        out.write(new byte[]{1, 2, 3}, 0, 3); // delivers three
        DataInputStream in = Framing.reader(new ByteArrayInputStream(out.toByteArray()));
        // Not an end of stream: treating it as one would hand the mux half a frame and
        // silently corrupt whatever stream it belonged to.
        assertThrows(IOException.class, () -> Framing.read(in));
    }

    @Test
    void anAbsurdLengthIsRefusedBeforeAnythingIsAllocated() {
        byte[] claim = {0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff}; // ~2 GB
        DataInputStream in = Framing.reader(new ByteArrayInputStream(claim));
        IOException e = assertThrows(IOException.class, () -> Framing.read(in));
        assertTrue(e.getMessage().contains("bad frame length"), e.getMessage());
    }

    @Test
    void aNegativeLengthIsRefused() {
        byte[] claim = {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};
        DataInputStream in = Framing.reader(new ByteArrayInputStream(claim));
        assertThrows(IOException.class, () -> Framing.read(in));
    }

    @Test
    void sequencedFramesRoundTrip() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Framing.write(out, 1L, Frames.ping());
        Framing.write(out, 2L, Frames.data(7, "abc".getBytes(StandardCharsets.UTF_8), 0, 3));
        Framing.write(out, Long.MAX_VALUE, Frames.bye());

        DataInputStream in = Framing.reader(new ByteArrayInputStream(out.toByteArray()));
        Framing.Sequenced first = Framing.readSequenced(in);
        assertEquals(1L, first.seq());
        assertArrayEquals(Frames.ping(), first.frame());

        Framing.Sequenced second = Framing.readSequenced(in);
        assertEquals(2L, second.seq());
        assertEquals(7, Frames.decode(second.frame()).streamId);

        assertEquals(Long.MAX_VALUE, Framing.readSequenced(in).seq());
        assertNull(Framing.readSequenced(in), "a response that ends between frames has ended");
    }

    @Test
    void aSequencedFrameCutShortIsAnErrorNotAnEnding() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            Framing.write(out, 5L, new byte[]{1, 2, 3, 4});
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        // Everything but the last payload byte: this is what a severed response looks like, and
        // mistaking it for the end would leave the reader believing it holds a frame it does not.
        byte[] truncated = new byte[out.size() - 1];
        System.arraycopy(out.toByteArray(), 0, truncated, 0, truncated.length);
        DataInputStream in = Framing.reader(new ByteArrayInputStream(truncated));
        assertThrows(IOException.class, () -> Framing.readSequenced(in));
    }

    @Test
    void aHeaderCutInHalfIsAnErrorToo() {
        // Four bytes: enough to start a sequence number, not enough to finish one. Returning
        // "ended" here would silently drop whatever frame was on its way.
        DataInputStream in = Framing.reader(new ByteArrayInputStream(new byte[]{0, 0, 0, 0}));
        assertThrows(IOException.class, () -> Framing.readSequenced(in));
    }

    @Test
    void sequenceNumbersStartAtOne() {
        // Zero means "I have nothing" when a client asks to resume, so it can never be a real
        // frame number; a stream claiming it is out of step.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            Framing.write(out, 0L, Frames.ping());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        DataInputStream in = Framing.reader(new ByteArrayInputStream(out.toByteArray()));
        assertThrows(IOException.class, () -> Framing.readSequenced(in));
    }

    @Test
    void anOversizedFrameIsRefusedOnTheWayOut() {
        byte[] huge = new byte[Framing.MAX_FRAME + 1];
        assertThrows(IOException.class, () -> Framing.write(new ByteArrayOutputStream(), huge));
    }
}
