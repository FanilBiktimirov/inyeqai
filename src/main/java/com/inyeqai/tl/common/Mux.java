package com.inyeqai.tl.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Мультиплексирует много TCP-потоков поверх одного соединения — WebSocket или запасного
 * HTTP-транспорта, разницы отсюда не видно. Общий для обеих сторон: каждая
 * держит у себя карту {@code streamId -> Stream}, выкачивает байты из сокета кадрами DATA и
 * пишет входящие кадры DATA обратно в соответствующий сокет.
 *
 * <p>Главное в этом классе — многопоточность. Путь разбора кадров ({@code onOpen},
 * {@code onData}, {@code onEof}, {@code onClose}, {@code onWindow}) никогда не блокируется на
 * вводе-выводе сокета: он только раздаёт работу рабочим потокам, своим на каждый поток туннеля.
 * И дозвон до адресата, и запись в сокет живут вне этого пути, так что один недоступный хост
 * или один залипший получатель не могут застопорить все остальные потоки в туннеле.
 *
 * <p>У каждого направления каждого потока есть окно кредитов на {@link #WINDOW_BYTES} байт.
 * Отправитель блокируется, как только столько байт остаётся неподтверждёнными, а получатель
 * возвращает кредит по мере того, как выгребает байты в свой сокет. Без этого быстрый локальный
 * читатель при медленном туннеле раздувал бы очередь без границы.
 *
 * <p>Приёмник {@code sender} должен безопасно вызываться из многих потоков и не должен бросать
 * исключений; блокироваться ему можно — именно так обратное давление доходит до потоков-насосов.
 * И Spring-сервер, и JDK-клиент оборачивают свой путь отправки соответственно.
 */
public final class Mux {

    /** Открывает TCP-соединение до адресата; каждая сторона подставляет своё. */
    public interface Dialer {
        Socket dial(String host, int port) throws IOException;
    }

    /** Решает, можно ли по просьбе другой стороны дозваниваться до {@code host:port}. */
    public interface Policy {
        boolean allows(String host, int port);
    }

    private static final Logger log = LoggerFactory.getLogger(Mux.class);

    /** Сколько неподтверждённых байт отправитель может держать в пути на направление потока. */
    public static final int WINDOW_BYTES = 256 * 1024;

    private static final int CHUNK = 16 * 1024;
    /**
     * Жёсткий предел на байты, стоящие в очереди одного потока. Сторона, уважающая окно, до него
     * никогда не доберётся; сторона, игнорирующая окно, нарушает протокол — и мы роняем её поток,
     * а не буферизуем всё, что она присылает.
     */
    private static final int QUEUE_LIMIT = 2 * WINDOW_BYTES;
    /** Маркер в очереди: «сторона закончила писать»; стоит по порядку за ещё не отданными DATA. */
    private static final Object EOF_MARK = new Object();
    /** Сколько заблокированный отправитель ждёт, прежде чем проверить, жив ли ещё его поток. */
    private static final long WINDOW_POLL_MS = 200;

    private static final Policy ALLOW_ALL = (host, port) -> true;

    private final Consumer<byte[]> sender;
    private final boolean serverSide;
    private final Policy policy;
    private final ConcurrentHashMap<Integer, Stream> streams = new ConcurrentHashMap<>();
    private final AtomicInteger counter = new AtomicInteger(1);
    private final AtomicInteger threadSeq = new AtomicInteger();
    private final AtomicLong streamsOpened = new AtomicLong();
    private final AtomicLong bytesToPeer = new AtomicLong();
    private final AtomicLong bytesFromPeer = new AtomicLong();
    private final ExecutorService pool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "mux-" + threadSeq.incrementAndGet());
        t.setDaemon(true);
        return t;
    });

    private volatile boolean shuttingDown;

    public Mux(Consumer<byte[]> sender, boolean serverSide) {
        this(sender, serverSide, ALLOW_ALL);
    }

    public Mux(Consumer<byte[]> sender, boolean serverSide, Policy policy) {
        this.sender = sender;
        this.serverSide = serverSide;
        this.policy = policy == null ? ALLOW_ALL : policy;
    }

    /** Одно мультиплексированное TCP-соединение. */
    private static final class Stream {
        final int id;
        /** Куда идёт этот поток, в виде {@code host:port}; только для логов и статуса. */
        final String dst;
        final long openedAtNanos = System.nanoTime();
        final BlockingQueue<Object> inbound = new LinkedBlockingQueue<>();
        final AtomicInteger queued = new AtomicInteger();
        /** Кредит на байты, которые *мы* ещё можем отправить. */
        final Semaphore window = new Semaphore(WINDOW_BYTES);
        final AtomicBoolean sentEof = new AtomicBoolean();
        final AtomicBoolean recvEof = new AtomicBoolean();
        final AtomicBoolean dead = new AtomicBoolean();
        final AtomicLong sent = new AtomicLong();
        final AtomicLong received = new AtomicLong();
        final AtomicBoolean logged = new AtomicBoolean();
        volatile Socket socket;

        Stream(int id, String dst) {
            this.id = id;
            this.dst = dst;
        }

        long ageMillis() {
            return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - openedAtNanos);
        }
    }

    /** Новый id потока для соединения, принятого этой стороной. Старший бит помечает инициатора. */
    public int nextId() {
        int id = counter.getAndIncrement() & 0x7fffffff;
        return serverSide ? (id | 0x80000000) : id;
    }

    /**
     * Регистрирует сокет, который мы приняли на слушателе, до того как уйдёт его кадр OPEN.
     * Работает в паре с {@link #startOutbound(int)}:
     *
     * <pre>
     *   mux.prepareOutbound(id, socket, dst);
     *   send(Frames.open(id, host, port));
     *   mux.startOutbound(id);
     * </pre>
     *
     * <p>Именно регистрация заранее делает зазор вокруг OPEN безопасным. Другая сторона может
     * ответить CLOSE (отказала или не смогла дозвониться) ещё до того, как мы начали
     * перекладывать байты; раз поток уже в карте, этот CLOSE попадёт в него и сокет закроется,
     * а не придёт на неизвестный id, оставив соединение вызывающего висеть открытым навсегда.
     * А запуск потоков только после этого не даёт DATA обогнать свой же OPEN.
     */
    public void prepareOutbound(int streamId, Socket socket, String dst) {
        Stream s = new Stream(streamId, dst);
        s.socket = socket;
        if (streams.putIfAbsent(streamId, s) != null) {
            closeQuiet(socket);
            throw new IllegalStateException("duplicate stream id " + streamId);
        }
        if (shuttingDown) {
            streams.remove(streamId, s);
            closeQuiet(socket);
        }
    }

    /** Начинает перекладывать поток, зарегистрированный через {@link #prepareOutbound}. */
    public void startOutbound(int streamId) {
        Stream s = streams.get(streamId);
        if (s == null || s.dead.get() || shuttingDown) {
            return; // другая сторона уже отказала, или мы уходим
        }
        streamsOpened.incrementAndGet();
        log.debug("stream {} open, peer dials {}", label(streamId), s.dst);
        start(s);
    }

    /**
     * Другая сторона попросила нас дозвониться до host:port для этого потока. Регистрирует поток
     * сразу, чтобы DATA, пришедшие пока дозвон ещё в пути, встали в очередь, а не потерялись,
     * и только потом дозванивается — вне потока разбора кадров.
     */
    public void onOpen(int streamId, String host, int port, Dialer dialer) {
        Stream s = new Stream(streamId, host + ":" + port);
        if (streams.putIfAbsent(streamId, s) != null) {
            log.warn("peer reused stream id {}", label(streamId));
            sender.accept(Frames.close(streamId));
            return;
        }
        if (shuttingDown) {
            streams.remove(streamId, s);
            return;
        }
        if (!policy.allows(host, port)) {
            log.warn("denied stream {} to {} (not allowed)", label(streamId), s.dst);
            reject(s);
            return;
        }
        pool.execute(() -> {
            Socket sock;
            try {
                sock = dialer.dial(host, port);
            } catch (IOException e) {
                log.debug("stream {} could not reach {}: {}", label(streamId), s.dst, e.toString());
                reject(s);
                return;
            }
            s.socket = sock;
            if (s.dead.get() || shuttingDown) {
                closeQuiet(sock);
                return;
            }
            streamsOpened.incrementAndGet();
            log.debug("stream {} open, dialed {}", label(streamId), s.dst);
            start(s);
        });
    }

    /** Для потока пришли байты; ставим их в очередь к его пишущему потоку. */
    public void onData(int streamId, byte[] payload) {
        Stream s = streams.get(streamId);
        if (s == null || s.dead.get()) {
            return;
        }
        if (s.queued.addAndGet(payload.length) > QUEUE_LIMIT) {
            log.warn("stream {} exceeded the receive window, dropping it", label(streamId));
            fail(s);
            return;
        }
        s.inbound.add(payload);
    }

    /** Сторона закончила писать в этот поток; полузакрываем свой, когда отдадим остаток байт. */
    public void onEof(int streamId) {
        Stream s = streams.get(streamId);
        if (s != null && !s.dead.get()) {
            s.inbound.add(EOF_MARK);
        }
    }

    /** Другая сторона выдала нам кредит, чтобы отправить в этот поток ещё. */
    public void onWindow(int streamId, int credit) {
        Stream s = streams.get(streamId);
        if (s != null) {
            s.window.release(credit);
        }
    }

    /** Другая сторона снесла поток; убираем свою сторону, не отправляя CLOSE в ответ. */
    public void onClose(int streamId) {
        Stream s = streams.remove(streamId);
        if (s != null) {
            kill(s);
            logClosed(s, "closed by peer");
        }
    }

    public void closeAll() {
        shuttingDown = true;
        for (Integer id : new ArrayList<>(streams.keySet())) {
            Stream s = streams.remove(id);
            if (s != null) {
                kill(s);
                logClosed(s, "dropped with the TL");
            }
        }
        pool.shutdownNow();
    }

    /** Сколько потоков живо прямо сейчас. */
    public int openStreams() {
        return streams.size();
    }

    /** Сколько потоков этот мультиплексор пронёс с момента создания. */
    public long streamsOpened() {
        return streamsOpened.get();
    }

    /** Байты, прочитанные из локальных сокетов и отправленные в туннель. */
    public long bytesToPeer() {
        return bytesToPeer.get();
    }

    /** Байты, принятые из туннеля и записанные в локальные сокеты. */
    public long bytesFromPeer() {
        return bytesFromPeer.get();
    }

    /** Все живые потоки — для экрана статуса. */
    public List<StreamInfo> streams() {
        List<StreamInfo> out = new ArrayList<>();
        for (Stream s : streams.values()) {
            out.add(new StreamInfo(s.id, s.dst, s.ageMillis(), s.sent.get(), s.received.get()));
        }
        out.sort(Comparator.comparingLong(StreamInfo::ageMillis).reversed());
        return out;
    }

    private void start(Stream s) {
        pool.execute(() -> writerLoop(s));
        pool.execute(() -> pumpLoop(s));
    }

    /** Сообщает другой стороне, что поток так и не поднялся, и забывает его. */
    private void reject(Stream s) {
        streams.remove(s.id, s);
        s.dead.set(true);
        s.inbound.clear();
        sender.accept(Frames.close(s.id));
    }

    /** Разбор кадров -> сокет: выгребает очередь в сокет, возвращая кредит. */
    private void writerLoop(Stream s) {
        Socket socket = s.socket;
        try {
            OutputStream out = socket.getOutputStream();
            while (true) {
                Object item = s.inbound.take();
                if (item == EOF_MARK) {
                    s.recvEof.set(true);
                    shutdownOutputQuiet(socket);
                    finishIfDone(s);
                    return;
                }
                byte[] chunk = (byte[]) item;
                out.write(chunk);
                out.flush();
                s.queued.addAndGet(-chunk.length);
                s.received.addAndGet(chunk.length);
                bytesFromPeer.addAndGet(chunk.length);
                // Кредит возвращается только после того, как байты реально ушли из наших рук,
                // чтобы окно действительно отражало то, что адресат уже впитал.
                sender.accept(Frames.window(s.id, chunk.length));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException | RuntimeException e) {
            fail(s);
        }
    }

    /** Сокет -> отправка кадров: перекладывает байты наружу, уважая окно другой стороны. */
    private void pumpLoop(Stream s) {
        Socket socket = s.socket;
        byte[] buf = new byte[CHUNK];
        try {
            InputStream in = socket.getInputStream();
            int n;
            while ((n = in.read(buf)) >= 0) {
                if (n == 0) {
                    continue;
                }
                if (!acquireWindow(s, n)) {
                    return; // поток умер, или мы выключаемся
                }
                sender.accept(Frames.data(s.id, buf, 0, n));
                s.sent.addAndGet(n);
                bytesToPeer.addAndGet(n);
            }
            // Чистый EOF: объявляем его и даём другому направлению работать дальше.
            s.sentEof.set(true);
            sender.accept(Frames.eof(s.id));
            finishIfDone(s);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException | RuntimeException e) {
            fail(s);
        }
    }

    /**
     * Ждёт кредит на {@code n} байт. Опрашивает в цикле, чтобы поток, убитый пока его отправитель
     * заблокирован здесь, не завис: закрытие сокета разблокирует чтение, но ожидание кредита
     * иначе не разблокировало бы ничто.
     */
    private boolean acquireWindow(Stream s, int n) throws InterruptedException {
        while (!s.dead.get() && !shuttingDown) {
            if (s.window.tryAcquire(n, WINDOW_POLL_MS, TimeUnit.MILLISECONDS)) {
                return true;
            }
        }
        return false;
    }

    /** Оба направления увидели чистый EOF: поток закончен, CLOSE не нужен. */
    private void finishIfDone(Stream s) {
        if (s.sentEof.get() && s.recvEof.get() && streams.remove(s.id, s)) {
            s.dead.set(true);
            closeQuiet(s.socket);
            logClosed(s, "closed cleanly");
        }
    }

    /** На нашей стороне что-то сломалось: говорим об этом другой стороне и сносим поток. */
    private void fail(Stream s) {
        if (streams.remove(s.id, s)) {
            kill(s);
            sender.accept(Frames.close(s.id));
            logClosed(s, "failed");
        }
    }

    /**
     * По строке на каждый закончившийся поток, на уровне DEBUG: куда он шёл, сколько прожил и
     * сколько пронёс в каждую сторону. Этого в логах иначе нет совсем — без такой строки
     * работающий туннель и туннель, который тихо роняет потоки, выглядят одинаково.
     */
    private void logClosed(Stream s, String how) {
        if (s.logged.compareAndSet(false, true) && log.isDebugEnabled()) {
            log.debug("stream {} to {} {} after {} ms, sent {}, received {}",
                    label(s.id), s.dst, how, s.ageMillis(), bytes(s.sent.get()), bytes(s.received.get()));
        }
    }

    /** Id потоков читаются беззнаковыми; см. {@link StreamInfo#label()}. */
    private static String label(int streamId) {
        return Integer.toUnsignedString(streamId);
    }

    /**
     * Счётчики байт, которые человек прочитает с одного взгляда. Форматируются с
     * {@link Locale#ROOT} намеренно: локаль по умолчанию поставила бы в дробной части запятую на
     * русской машине и точку в контейнере, и один и тот же трафик читался бы по-разному
     * в зависимости от того, где процесс запустили.
     */
    public static String bytes(long n) {
        if (n < 1024) {
            return n + " B";
        }
        if (n < 1024 * 1024) {
            return String.format(Locale.ROOT, "%.1f KB", n / 1024.0);
        }
        if (n < 1024L * 1024 * 1024) {
            return String.format(Locale.ROOT, "%.1f MB", n / (1024.0 * 1024));
        }
        return String.format(Locale.ROOT, "%.2f GB", n / (1024.0 * 1024 * 1024));
    }

    private static void kill(Stream s) {
        s.dead.set(true);
        s.inbound.clear();
        closeQuiet(s.socket);
    }

    private static void closeQuiet(Socket s) {
        if (s == null) {
            return;
        }
        try {
            s.close();
        } catch (IOException ignored) {
        }
    }

    private static void shutdownOutputQuiet(Socket s) {
        try {
            if (!s.isClosed() && !s.isOutputShutdown()) {
                s.shutdownOutput();
            }
        } catch (IOException ignored) {
            // другой стороны может уже не быть; судьбу потока решает читающая сторона
        }
    }
}
