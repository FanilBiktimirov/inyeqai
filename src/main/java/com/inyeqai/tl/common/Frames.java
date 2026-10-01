package com.inyeqai.tl.common;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Протокол на проводе: TCP-потоки, мультиплексированные поверх одного соединения, big-endian.
 * Соединением может быть и WebSocket, и запасной HTTP-транспорт.
 *
 * <pre>
 *   OPEN   [1][streamId:4][hostLen:2][host][port:2]   дозвон до host:port для этого потока
 *   DATA   [2][streamId:4][payload...]                байты потока, в любую сторону
 *   CLOSE  [3][streamId:4]                            поток снесён, сразу в обе стороны
 *   CONFIG [4][count:2]{ bindLen:2 bind serverPort:2 dstLen:2 dst clientPort:2 }*
 *                                                     клиент -> сервер: поднять обратные слушатели
 *   EOF    [5][streamId:4]                            отправитель закончил писать (полузакрытие)
 *   WINDOW [6][streamId:4][credit:4]                  получатель выгреб credit байт
 *   PING   [7]                                        keepalive транспорта, ждёт PONG
 *   PONG   [8]                                        ответ на PING
 *   BYE    [9]                                        транспорт закрывается осознанно
 *   ACK    [10][through:8]                            сторона держит все кадры вниз до сих пор
 * </pre>
 *
 * Старший бит {@code streamId} помечает инициатора (0 = клиент, 1 = сервер), поэтому id,
 * которые обе стороны раздают независимо друг от друга, никогда не пересекаются.
 *
 * <p>{@code PING}, {@code PONG}, {@code BYE} и {@code ACK} принадлежат транспорту, а не
 * туннелю: они есть потому, что у запасного HTTP-транспорта нет своих кадров ping, pong и
 * закрытия, и нет способа переподключающемуся клиенту сказать, что он уже получил. Оба
 * HTTP-конца сами отвечают на них и съедают их внутри своего транспортного слоя, так что ни один
 * мультиплексор их не видит никогда. По WebSocket они не отправляются вообще — у того транспорта
 * есть свои, и он никогда не возобновляется.
 *
 * <p>{@code EOF} повторяет полузакрытие TCP: сторона перестаёт читать в одном направлении, но
 * продолжает писать в другом, которое обычный {@code CLOSE} бы отрезал. {@code WINDOW} несёт
 * управление потоком по кредитам: у отправителя может быть не больше
 * {@link Mux#WINDOW_BYTES} неподтверждённых байт в пути на поток, так что один залипший
 * адресат не заставит туннель буферизовать без границы.
 */
public final class Frames {

    public static final byte OPEN = 1;
    public static final byte DATA = 2;
    public static final byte CLOSE = 3;
    public static final byte CONFIG = 4;
    public static final byte EOF = 5;
    public static final byte WINDOW = 6;
    public static final byte PING = 7;
    public static final byte PONG = 8;
    public static final byte BYE = 9;
    public static final byte ACK = 10;

    private static final Charset UTF8 = StandardCharsets.UTF_8;
    private static final byte[] PING_BYTES = {PING};
    private static final byte[] PONG_BYTES = {PONG};
    private static final byte[] BYE_BYTES = {BYE};

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

    /**
     * Keepalive транспорта. Возвращает общий массив, потому что эти кадры неизменяемые,
     * однобайтовые и уходят по таймеру: выделять новый каждый раз — чистая трата.
     */
    public static byte[] ping() {
        return PING_BYTES;
    }

    public static byte[] pong() {
        return PONG_BYTES;
    }

    /** Транспорт закрывается осознанно, чтобы другая сторона сразу отпустила сессию. */
    public static byte[] bye() {
        return BYE_BYTES;
    }

    /**
     * Подтверждает все кадры вниз до {@code through}. Именно это позволяет серверу выбрасывать
     * кадры из буфера переотправки, и с этим же сверяется заявка возобновляющегося клиента.
     */
    public static byte[] ack(long through) {
        return ByteBuffer.allocate(1 + 8).put(ACK).putLong(through).array();
    }

    /** True для кадров уровня транспорта, с которыми транспорты разбираются сами. */
    public static boolean isCarrier(byte type) {
        return type == PING || type == PONG || type == BYE || type == ACK;
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
     * Разбирает одно сообщение. Бросает {@link IllegalArgumentException} на всё кривое,
     * включая кадр, оборвавшийся раньше времени; вызывающий считает это фатальной ошибкой
     * сессии, а не поводом угадывать, что отправитель имел в виду.
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
            case PING, PONG, BYE -> {
                return Frame.carrier(type);
            }
            case ACK -> {
                long through = b.getLong();
                if (through < 0) {
                    throw new IllegalArgumentException("negative ack sequence " + through);
                }
                return Frame.ack(through);
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
