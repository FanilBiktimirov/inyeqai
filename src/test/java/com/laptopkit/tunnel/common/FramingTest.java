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
 * На HTTP-транспорте кадры отделяет друг от друга только префикс длины, поэтому интересны те
 * случаи, где читающая сторона может угадать неверно: поток, который кончился между кадрами —
 * это нормально, и поток, который кончился внутри кадра — это порча данных.
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
        out.write(8); // обещает восемь байт
        out.write(new byte[]{1, 2, 3}, 0, 3); // отдаёт три
        DataInputStream in = Framing.reader(new ByteArrayInputStream(out.toByteArray()));
        // Это не конец потока: принять его за конец — значит отдать в мультиплексор половину
        // кадра и молча испортить тот поток, которому кадр принадлежал.
        assertThrows(IOException.class, () -> Framing.read(in));
    }

    @Test
    void anAbsurdLengthIsRefusedBeforeAnythingIsAllocated() {
        byte[] claim = {0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff}; // ~2 ГБ
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
        // Всё, кроме последнего байта полезной нагрузки: именно так выглядит обрубленный ответ,
        // и если принять его за конец, читающая сторона поверит, что держит кадр, которого нет.
        byte[] truncated = new byte[out.size() - 1];
        System.arraycopy(out.toByteArray(), 0, truncated, 0, truncated.length);
        DataInputStream in = Framing.reader(new ByteArrayInputStream(truncated));
        assertThrows(IOException.class, () -> Framing.readSequenced(in));
    }

    @Test
    void aHeaderCutInHalfIsAnErrorToo() {
        // Четыре байта: хватает начать номер кадра, не хватает его дочитать. Вернуть здесь
        // «поток кончился» — значит молча потерять кадр, который был уже в пути.
        DataInputStream in = Framing.reader(new ByteArrayInputStream(new byte[]{0, 0, 0, 0}));
        assertThrows(IOException.class, () -> Framing.readSequenced(in));
    }

    @Test
    void sequenceNumbersStartAtOne() {
        // Ноль означает «у меня ничего нет», когда клиент просит продолжить сессию, поэтому
        // настоящим номером кадра он быть не может; поток, который его присылает, сбился.
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
