package com.laptopkit.tunnel.common;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Frame boundaries for a carrier that has none.
 *
 * <p>A WebSocket delivers one message per send, so {@link Frames} needs no length field
 * there. An HTTP body is a plain byte stream: the same frames travelling over it have to say
 * how long they are.
 *
 * <pre>
 *   [length:4][frame bytes]   repeated until the stream ends
 * </pre>
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

    /** Write one length-prefixed frame. Does not flush; callers decide when to. */
    public static void write(OutputStream out, byte[] frame) throws IOException {
        if (frame.length > MAX_FRAME) {
            throw new IOException("frame of " + frame.length + " bytes exceeds the " + MAX_FRAME + " limit");
        }
        out.write(frame.length >>> 24);
        out.write(frame.length >>> 16);
        out.write(frame.length >>> 8);
        out.write(frame.length);
        out.write(frame);
    }

    /**
     * Read one frame.
     *
     * @return the frame, or null at a clean end of stream (nothing at all was pending)
     * @throws IOException on a truncated frame, a negative or oversized length, or an I/O
     *                     failure. A frame that merely ends early is a protocol error and
     *                     must not be mistaken for the end of the stream.
     */
    public static byte[] read(DataInputStream in) throws IOException {
        int len;
        try {
            len = in.readInt();
        } catch (EOFException e) {
            return null; // the peer closed between frames, which is how a stream ends
        }
        if (len < 0 || len > MAX_FRAME) {
            throw new IOException("bad frame length " + len);
        }
        byte[] frame = new byte[len];
        in.readFully(frame); // throws EOFException, an IOException, on a truncated frame
        return frame;
    }

    /** Wrap a stream for {@link #read}. */
    public static DataInputStream reader(InputStream in) {
        return new DataInputStream(in);
    }
}
