package com.laptopkit.tunnel.common;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * The wire protocol: TCP streams multiplexed over a single WebSocket, big-endian.
 *
 * <pre>
 *   OPEN   [1][streamId:4][hostLen:2][host][port:2]   dial host:port for this stream
 *   DATA   [2][streamId:4][payload...]                bytes for a stream, either way
 *   CLOSE  [3][streamId:4]                            stream torn down, both directions
 *   CONFIG [4][count:2]{ bindLen:2 bind serverPort:2 dstLen:2 dst clientPort:2 }*
 *                                                     client -> server: open reverse listeners
 *   EOF    [5][streamId:4]                            sender is done writing (half-close)
 *   WINDOW [6][streamId:4][credit:4]                  receiver drained credit bytes
 * </pre>
 *
 * The {@code streamId} top bit marks the originator (0 = client, 1 = server) so ids
 * allocated independently on both ends never collide.
 *
 * <p>{@code EOF} mirrors a TCP half-close: the peer stops reading that direction but keeps
 * writing the other one, which plain {@code CLOSE} would have cut off. {@code WINDOW}
 * carries the credit-based flow control: a sender may have at most
 * {@link Mux#WINDOW_BYTES} unacknowledged bytes in flight per stream, so one stalled
 * destination cannot make the tunnel buffer without bound.
 */
public final class Frames {

    public static final byte OPEN = 1;
    public static final byte DATA = 2;
    public static final byte CLOSE = 3;
    public static final byte CONFIG = 4;
    public static final byte EOF = 5;
    public static final byte WINDOW = 6;

    private static final Charset UTF8 = StandardCharsets.UTF_8;

    private Frames() {
    }

    public static byte[] open(int streamId, String host, int port) {
        byte[] h = host.getBytes(UTF8);
        return ByteBuffer.allocate(1 + 4 + 2 + h.length + 2)
                .put(OPEN).putInt(streamId).putShort((short) h.length).put(h).putShort((short) port)
                .array();
    }

    public static byte[] data(int streamId, byte[] payload, int off, int len) {
        return ByteBuffer.allocate(1 + 4 + len)
                .put(DATA).putInt(streamId).put(payload, off, len)
                .array();
    }

    public static byte[] close(int streamId) {
        return ByteBuffer.allocate(1 + 4).put(CLOSE).putInt(streamId).array();
    }

    public static byte[] eof(int streamId) {
        return ByteBuffer.allocate(1 + 4).put(EOF).putInt(streamId).array();
    }

    public static byte[] window(int streamId, int credit) {
        return ByteBuffer.allocate(1 + 4 + 4).put(WINDOW).putInt(streamId).putInt(credit).array();
    }

    public static byte[] config(List<Reverse> reverses) {
        List<byte[]> binds = new ArrayList<>();
        List<byte[]> dsts = new ArrayList<>();
        int size = 1 + 2;
        for (Reverse r : reverses) {
            byte[] bh = r.bindHost().getBytes(UTF8);
            byte[] dh = r.clientHost().getBytes(UTF8);
            binds.add(bh);
            dsts.add(dh);
            size += 2 + bh.length + 2 + 2 + dh.length + 2;
        }
        ByteBuffer b = ByteBuffer.allocate(size).put(CONFIG).putShort((short) reverses.size());
        for (int i = 0; i < reverses.size(); i++) {
            Reverse r = reverses.get(i);
            byte[] bh = binds.get(i);
            byte[] dh = dsts.get(i);
            b.putShort((short) bh.length).put(bh).putShort((short) r.serverPort())
                    .putShort((short) dh.length).put(dh).putShort((short) r.clientPort());
        }
        return b.array();
    }

    /**
     * Decode one message. Throws {@link IllegalArgumentException} on anything malformed,
     * including a frame that ends early; callers treat that as a fatal session error
     * rather than guessing at the sender's intent.
     */
    public static Frame decode(byte[] msg) {
        try {
            return decodeOrThrow(msg);
        } catch (BufferUnderflowException e) {
            throw new IllegalArgumentException("truncated frame (" + msg.length + " bytes)");
        }
    }

    private static Frame decodeOrThrow(byte[] msg) {
        if (msg.length == 0) {
            throw new IllegalArgumentException("empty frame");
        }
        ByteBuffer b = ByteBuffer.wrap(msg);
        byte type = b.get();
        switch (type) {
            case OPEN -> {
                int id = b.getInt();
                String host = getString(b);
                int port = b.getShort() & 0xffff;
                return Frame.open(id, host, port);
            }
            case DATA -> {
                int id = b.getInt();
                byte[] payload = new byte[b.remaining()];
                b.get(payload);
                return Frame.data(id, payload);
            }
            case CLOSE -> {
                int id = b.getInt();
                return Frame.close(id);
            }
            case EOF -> {
                int id = b.getInt();
                return Frame.eof(id);
            }
            case WINDOW -> {
                int id = b.getInt();
                int credit = b.getInt();
                if (credit <= 0) {
                    throw new IllegalArgumentException("non-positive window credit " + credit);
                }
                return Frame.window(id, credit);
            }
            case CONFIG -> {
                int n = b.getShort() & 0xffff;
                List<Reverse> rs = new ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    String bind = getString(b);
                    int serverPort = b.getShort() & 0xffff;
                    String dst = getString(b);
                    int clientPort = b.getShort() & 0xffff;
                    rs.add(new Reverse(bind, serverPort, dst, clientPort));
                }
                return Frame.config(rs);
            }
            default -> throw new IllegalArgumentException("unknown frame type " + type);
        }
    }

    private static String getString(ByteBuffer b) {
        int len = b.getShort() & 0xffff;
        if (len > b.remaining()) {
            throw new IllegalArgumentException("string length " + len + " exceeds frame");
        }
        byte[] s = new byte[len];
        b.get(s);
        return new String(s, UTF8);
    }
}
