package com.inyeqai.tunnel.common;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Границы кадров для транспорта, у которого их нет.
 *
 * <p>WebSocket доставляет по одному сообщению на отправку, поэтому там {@link Frames} обходится
 * без поля длины. HTTP-тело — обычный поток байт: те же кадры, идущие поверх него, обязаны
 * говорить, какой они длины.
 *
 * <pre>
 *   вверх        [length:4][frame bytes]              повторяется до конца потока
 *   вниз         [sequence:8][length:4][frame bytes]  повторяется до конца потока
 * </pre>
 *
 * <p>Номер есть только у потока вниз, потому что только поток вниз возобновляется кадр за
 * кадром: умерший ответ приходится заменять другим, который подхватит ровно там, где клиент
 * остановился, и ничем больше это место не определить. Вверх вместо этого идёт череда целых
 * запросов, так что номера пачки в запросе хватает, чтобы отличить повтор от новой работы.
 *
 * <p>Длина сверяется с {@link #MAX_FRAME} до того, как будет выделен хоть один байт, так что
 * рассинхронизированная или враждебная сторона не заставит читателя зарезервировать
 * произвольный буфер, заявив огромный кадр.
 */
public final class Framing {

    /**
     * Самый большой кадр, который принимается с провода. С запасом больше 16-килобайтных кусков
     * полезной нагрузки, которые нарезает мультиплексор; всё, что близко к пределу, означает, что
     * поток разъехался, а не настоящий трафик.
     */
    public static final int MAX_FRAME = 1024 * 1024;

    private Framing() {
    }

    /** Один кадр, прочитанный из нумерованного потока. */
    public record Sequenced(long seq, byte[] frame) {
    }

    /** Пишет один кадр с префиксом длины. Flush не делает: когда его делать, решает вызывающий. */
    public static void write(OutputStream out, byte[] frame) throws IOException {
        checkSize(frame);
        writeInt(out, frame.length);
        out.write(frame);
    }

    /** Пишет один кадр с его номером — для потока, который может понадобиться возобновить. */
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
     * Читает один кадр.
     *
     * @return кадр или null при чистом конце потока: конец ровно на границе кадра, когда ни одного
     *         байта заголовка прочитать не успели
     * @throws IOException при оборванном кадре или заголовке, негодной длине или сбое
     *                     ввода-вывода. Поток, оборвавшийся внутри кадра, — ошибка протокола, и
     *                     принимать её за конец кадра нельзя никогда: вверх это означало бы
     *                     применить часть пачки и отчитаться о ней целиком как о сделанной.
     */
    public static byte[] read(DataInputStream in) throws IOException {
        int first = in.read();
        if (first < 0) {
            return null; // сторона закрылась между кадрами — так поток и заканчивается
        }
        byte[] header = new byte[4];
        header[0] = (byte) first;
        in.readFully(header, 1, 3); // EOFException, то есть IOException, если заголовок обрезан
        int len = ByteBuffer.wrap(header).getInt();
        if (len < 0 || len > MAX_FRAME) {
            throw new IOException("bad frame length " + len);
        }
        byte[] frame = new byte[len];
        in.readFully(frame);
        return frame;
    }

    /**
     * Читает один кадр и его номер из нумерованного потока.
     *
     * @return кадр или null при чистом конце потока
     * @throws IOException при оборванном кадре или заголовке, негодном номере или длине — так же,
     *                     как {@link #read}, и по той же причине
     */
    public static Sequenced readSequenced(DataInputStream in) throws IOException {
        int first = in.read();
        if (first < 0) {
            return null; // ответ закончился между кадрами
        }
        byte[] header = new byte[8 + 4];
        header[0] = (byte) first;
        in.readFully(header, 1, header.length - 1);
        ByteBuffer b = ByteBuffer.wrap(header);
        long seq = b.getLong();
        if (seq < 1) {
            // Ноль клиент отправляет в смысле «я не держу ничего», так что настоящим номером
            // кадра он быть не может; всё, что меньше, — бессмыслица.
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

    /** Обёртка над потоком для {@link #read} или {@link #readSequenced}. */
    public static DataInputStream reader(InputStream in) {
        return new DataInputStream(in);
    }
}
