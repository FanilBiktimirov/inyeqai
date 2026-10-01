package com.inyeqai.tl.common;

import java.util.List;

/**
 * Разобранный кадр протокола. Одно бинарное сообщение WebSocket несёт ровно один кадр.
 * Поля, которые к конкретному {@link #type} не относятся, остаются null / нулём.
 */
public final class Frame {

    public final byte type;
    public final int streamId;
    public final String host;
    public final int port;
    public final byte[] payload;
    public final List<Reverse> reverses;
    /** Сколько байт другая сторона успела выгрести, для {@link Frames#WINDOW}. */
    public final int credit;
    /** Наибольший номер кадра вниз, который другая сторона получила, для {@link Frames#ACK}. */
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

    /**
     * Кадр уровня транспорта без полей: {@code PING}, {@code PONG} или {@code BYE}. У
     * {@code ACK}, который тоже относится к транспорту, поле есть — он собирается в
     * {@link #ack(long)}.
     */
    static Frame carrier(byte type) {
        return new Frame(type, 0, null, 0, null, null, 0, 0);
    }

    /** Другая сторона подтверждает, что держит у себя все кадры вниз до {@code through}. */
    static Frame ack(long through) {
        return new Frame(Frames.ACK, 0, null, 0, null, null, 0, through);
    }
}
