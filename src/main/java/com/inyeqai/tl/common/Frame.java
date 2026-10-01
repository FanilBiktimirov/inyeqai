package com.inyeqai.tl.common;

import java.util.List;

/**
 * A decoded protocol frame. One WebSocket binary message carries exactly one frame.
 * Fields not relevant to a given {@link #type} are left null / zero.
 */
public final class Frame {

    public final byte type;
    public final int streamId;
    public final String host;
    public final int port;
    public final byte[] payload;
    public final List<Reverse> reverses;
    /** Bytes the peer drained, for {@link Frames#WINDOW}. */
    public final int credit;
    /** Highest downstream sequence the peer has received, for {@link Frames#ACK}. */
    public final long ackThrough;

    private Frame(byte type, int streamId, String host, int port, byte[] payload,
                  List<Reverse> reverses, int credit, long ackThrough) {
        this.type = type;
        this.streamId = streamId;
        this.host = host;
        this.port = port;
        this.payload = payload;
        this.reverses = reverses;
        this.credit = credit;
        this.ackThrough = ackThrough;
    }

    static Frame open(int streamId, String host, int port) {
        return new Frame(Frames.OPEN, streamId, host, port, null, null, 0, 0);
    }

    static Frame data(int streamId, byte[] payload) {
        return new Frame(Frames.DATA, streamId, null, 0, payload, null, 0, 0);
    }

    static Frame close(int streamId) {
        return new Frame(Frames.CLOSE, streamId, null, 0, null, null, 0, 0);
    }

    static Frame eof(int streamId) {
        return new Frame(Frames.EOF, streamId, null, 0, null, null, 0, 0);
    }

    static Frame window(int streamId, int credit) {
        return new Frame(Frames.WINDOW, streamId, null, 0, null, null, credit, 0);
    }

    static Frame config(List<Reverse> reverses) {
        return new Frame(Frames.CONFIG, 0, null, 0, null, reverses, 0, 0);
    }

    /** A carrier-level frame ({@code PING}, {@code PONG}, {@code BYE}): type and nothing else. */
    static Frame carrier(byte type) {
        return new Frame(type, 0, null, 0, null, null, 0, 0);
    }

    /** The peer confirming it holds every downstream frame up to {@code through}. */
    static Frame ack(long through) {
        return new Frame(Frames.ACK, 0, null, 0, null, null, 0, through);
    }
}
