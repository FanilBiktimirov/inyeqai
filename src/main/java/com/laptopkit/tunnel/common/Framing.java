package com.laptopkit.tunnel.common;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Frame boundaries for a carrier that has none.
 *
 * <p>A WebSocket delivers one message per send, so {@link Frames} needs no length field
 * there. An HTTP body is a plain byte stream: the same frames travelling over it have to say
 * how long they are.
 *
 * <pre>
 *   upstream     [length:4][frame bytes]              repeated until the stream ends
 *   downstream   [sequence:8][length:4][frame bytes]  repeated until the stream ends
 * </pre>
 *
 * <p>Only the downstream carries a sequence number, because only the downstream is resumed
 * frame by frame: a response that dies has to be replaced by one that picks up exactly where
 * the client stopped, and nothing else can say where that was. Upstream is a series of whole
 * requests instead, so a batch number on the request is enough to tell a retry from new work.
 *
 * <p>The length is checked against {@link #MAX_FRAME} before a single byte is allocated, so
 * a desynchronised or hostile peer cannot make the reader reserve an arbitrary buffer by
 * claiming a huge frame.
 */
public final class Framing {

    /**
     * Largest frame accepted off the wire. Comfortably above the 16 KB payload chunks the
     * mux produces; anything near it means the stream is out of step, not real traffic.
     */
    public static final int MAX_FRAME = 1024 * 1024;

    private Framing() {
    }

    /** One frame read off a sequenced stream. */
    public record Sequenced(long seq, byte[] frame) {
    }

    /** Write one length-prefixed frame. Does not flush; callers decide when to. */
    public static void write(OutputStream out, byte[] frame) throws IOException {
        checkSize(frame);
        writeInt(out, frame.length);
        out.write(frame);
    }

    /** Write one frame with its sequence number, for a stream that may have to be resumed. */
    public static void write(OutputStream out, long seq, byte[] frame) throws IOException {
        checkSize(frame);
        writeLong(out, seq);
        writeInt(out, frame.length);
        out.write(frame);
    }

    private static void checkSize(byte[] frame) throws IOException {
        if (frame.length > MAX_FRAME) {
            throw new IOException("frame of " + frame.length + " bytes exceeds the " + MAX_FRAME + " limit");
        }
    }

    private static void writeInt(OutputStream out, int v) throws IOException {
        out.write(v >>> 24);
        out.write(v >>> 16);
        out.write(v >>> 8);
        out.write(v);
    }

    private static void writeLong(OutputStream out, long v) throws IOException {
        writeInt(out, (int) (v >>> 32));
        writeInt(out, (int) v);
    }

    /**
     * Read one frame.
     *
     * @return the frame, or null at a clean end of stream: an end exactly on a frame boundary,
     *         with no part of a header read
     * @throws IOException on a truncated frame or header, a bad length, or an I/O failure. A
     *                     stream that stops inside a frame is a protocol error and must never be
     *                     mistaken for the end of one: upstream that would mean applying part of
     *                     a batch and reporting the whole of it as done.
     */
    public static byte[] read(DataInputStream in) throws IOException {
        int first = in.read();
        if (first < 0) {
            return null; // the peer closed between frames, which is how a stream ends
        }
        byte[] header = new byte[4];
        header[0] = (byte) first;
        in.readFully(header, 1, 3); // EOFException, an IOException, if the header is cut short
        int len = ByteBuffer.wrap(header).getInt();
        if (len < 0 || len > MAX_FRAME) {
            throw new IOException("bad frame length " + len);
        }
        byte[] frame = new byte[len];
        in.readFully(frame);
        return frame;
    }

    /**
     * Read one frame and its sequence number from a sequenced stream.
     *
     * @return the frame, or null at a clean end of stream
     * @throws IOException on a truncated frame or header, or a bad sequence or length, as
     *                     {@link #read} does and for the same reason
     */
    public static Sequenced readSequenced(DataInputStream in) throws IOException {
        int first = in.read();
        if (first < 0) {
            return null; // the response ended between frames
        }
        byte[] header = new byte[8 + 4];
        header[0] = (byte) first;
        in.readFully(header, 1, header.length - 1);
        ByteBuffer b = ByteBuffer.wrap(header);
        long seq = b.getLong();
        if (seq < 1) {
            // Zero is what a client sends to mean "I hold nothing", so it can never be a real
            // frame number; anything below it is nonsense.
            throw new IOException("bad frame sequence " + seq);
        }
        int len = b.getInt();
        if (len < 0 || len > MAX_FRAME) {
            throw new IOException("bad frame length " + len);
        }
        byte[] frame = new byte[len];
        in.readFully(frame);
        return new Sequenced(seq, frame);
    }

    /** Wrap a stream for {@link #read} or {@link #readSequenced}. */
    public static DataInputStream reader(InputStream in) {
        return new DataInputStream(in);
    }
}
