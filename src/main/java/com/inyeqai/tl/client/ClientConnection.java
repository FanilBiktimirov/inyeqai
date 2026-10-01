package com.inyeqai.tl.client;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inyeqai.tl.common.ForwardSpec;
import com.inyeqai.tl.common.Frame;
import com.inyeqai.tl.common.Frames;
import com.inyeqai.tl.common.Mux;
import com.inyeqai.tl.common.Reverse;

/**
 * Одна попытка подключения туннеля. Владеет WebSocket, {@link Mux}, локальными слушателями,
 * сериализованной очередью отправки (WebSocket из JDK запрещает перекрывающиеся отправки) и
 * расписанием keepalive. {@link #run()} блокируется, пока соединение не закончится.
 *
 * <p>Keepalive здесь не просто ping: ответ отслеживается, и туннель, до которого перестали
 * доходить pong, разбирается — чтобы вызывающий переподключился. Без этого соединение, убитое
 * незаметно — ноутбук ушёл в сон, запись в NAT протухла, шлюз перезапустили — выглядит здоровым
 * вечно: в сокет никто не пишет, а значит, никто и не заметит, что он сломан.
 */
final class ClientConnection implements WebSocket.Listener {

    private static final Logger log = LoggerFactory.getLogger(ClientConnection.class);
    private static final int DIAL_TIMEOUT_MS = 10_000;
    private static final byte[] PING = new byte[0];
    private static final byte[] STOP = new byte[0]; // другой объект, чем PING: сравнение по ссылке
    /**
     * Сколько кадров DATA разом можно держать в очереди отправки. Окна по потокам уже
     * ограничивают каждый поток в отдельности; это ограничивает сумму, чтобы много потоков не
     * сложились в неограниченную очередь.
     */
    private static final int MAX_INFLIGHT_DATA = 64;
    /** Сообщение такого размера значит, что пир рассинхронизировался, а не реальный трафик. */
    private static final int MAX_MESSAGE = 1024 * 1024;
    /** Pong можно и пропустить; но два периода keepalive тишины — значит, связи больше нет. */
    private static final int PONG_TIMEOUT_FACTOR = 2;

    private final String url;
    private final String auth;
    private final int keepaliveSec;
    private final List<ForwardSpec> locals;
    private final List<Reverse> reverses;
    private final Transport transport;

    private final LinkedBlockingQueue<byte[]> sendQ = new LinkedBlockingQueue<>();
    private final Semaphore dataSlots = new Semaphore(MAX_INFLIGHT_DATA);
    private final List<ServerSocket> listeners = new ArrayList<>();
    private final CountDownLatch closed = new CountDownLatch(1);
    private final AtomicBoolean down = new AtomicBoolean(false);
    private final ByteArrayOutputStream rx = new ByteArrayOutputStream();

    private volatile WebSocket ws;
    private volatile long lastPongNanos;
    private volatile long upSinceNanos;
    private Mux mux;
    private Thread sender;
    private ScheduledExecutorService keepalive;

    private final Mux.Dialer dialer = (host, port) -> {
        Socket s = new Socket();
        s.connect(new InetSocketAddress(host, port), DIAL_TIMEOUT_MS);
        return s;
    };

    ClientConnection(String url, String auth, int keepaliveSec,
                     List<ForwardSpec> locals, List<Reverse> reverses) {
        this(url, auth, keepaliveSec, locals, reverses, Transport.WEBSOCKET);
    }

    ClientConnection(String url, String auth, int keepaliveSec,
                     List<ForwardSpec> locals, List<Reverse> reverses, Transport transport) {
        this.url = url;
        this.auth = auth;
        this.keepaliveSec = keepaliveSec;
        this.locals = locals;
        this.reverses = reverses;
        this.transport = transport;
    }

    /**
     * Подключиться и блокироваться, пока транспорт не закроется или не отвалится с ошибкой.
     * Бросает исключение, если соединение вообще не удалось поднять, — вызывающий считает это
     * неудавшейся попыткой.
     *
     * <p>Оба транспорта приходят сюда как {@link WebSocket} и зовут этот класс обратно как
     * {@link WebSocket.Listener}; почему HTTP-транспорт сделан именно так — см. {@link HttpLink}.
     * Всё, что ниже этого метода, для обоих одинаково.
     */
    void run() throws Exception {
        if (transport == Transport.HTTP) {
            HttpLink.open(url, auth, this); // зовёт onOpen до того, как вернётся
        } else {
            HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15)).build();
            WebSocket.Builder b = http.newWebSocketBuilder().connectTimeout(Duration.ofSeconds(15));
            if (auth != null && !auth.isEmpty()) {
                b.header("X-TL-Auth", auth);
            }
            b.buildAsync(URI.create(url), this).join(); // завершится исключением на 401 и прочем
        }
        closed.await();
    }

    /** Разобрать это соединение и дать {@link #run()} вернуть управление. Нужно тестам. */
    void stop() {
        shutdown();
    }

    /** Сколько это соединение продержалось — для нарастающей задержки переподключения. */
    Duration uptime() {
        long since = upSinceNanos;
        return since == 0 ? Duration.ZERO : Duration.ofNanos(System.nanoTime() - since);
    }

    /** true с момента, как WebSocket открылся, и до того, как соединение разобрали. */
    boolean connected() {
        return ws == null || down.get();
    }

    /**
     * Давность самого свежего ответа на keepalive. Это единственное число, которое говорит, что
     * дальняя сторона действительно отвечает: WebSocket, чей пир исчез без TCP-close, остаётся
     * «подключённым» бесконечно, и выдают его только пропавшие pong.
     *
     * @return empty, когда keepalive выключен: тогда судить просто не по чему
     */
    Optional<Duration> sinceLastPong() {
        if (keepaliveSec <= 0) {
            return Optional.empty();
        }
        return Optional.of(Duration.ofNanos(System.nanoTime() - lastPongNanos));
    }

    /** Сколько pong может отсутствовать, прежде чем связь считается потерянной. */
    Duration pongDeadline() {
        return Duration.ofSeconds((long) keepaliveSec * PONG_TIMEOUT_FACTOR);
    }

    String url() {
        return url;
    }

    /** Каким транспортом шла эта попытка. Видно на health-эндпойнте, и это важно, когда
     * транспорт выбрал {@code --transport auto}, а не оператор. */
    Transport transport() {
        return transport;
    }

    List<ForwardSpec> locals() {
        return locals;
    }

    List<Reverse> reverses() {
        return reverses;
    }

    /** Счётчики мультиплексора или empty, пока соединение не открылось. */
    Optional<Mux> mux() {
        return Optional.ofNullable(mux);
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        this.ws = webSocket;
        this.mux = new Mux(this::enqueue, false);
        this.lastPongNanos = System.nanoTime();
        this.upSinceNanos = System.nanoTime();

        sender = new Thread(this::senderLoop, "ws-sender");
        sender.setDaemon(true);
        sender.start();

        if (!reverses.isEmpty()) {
            enqueue(Frames.config(reverses));
        }
        startLocalListeners();
        startKeepalive();

        log.info("TL up: {} over {}", url, transport.label());
        webSocket.request(Long.MAX_VALUE);
    }

    @Override
    public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
        if (rx.size() + data.remaining() > MAX_MESSAGE) {
            log.warn("peer sent an oversized message (> {} bytes), dropping the TL", MAX_MESSAGE);
            shutdown();
            return null;
        }
        byte[] part = new byte[data.remaining()];
        data.get(part);
        rx.write(part, 0, part.length);
        if (last) {
            byte[] msg = rx.toByteArray();
            rx.reset();
            dispatch(msg);
        }
        return null;
    }

    @Override
    public CompletionStage<?> onPong(WebSocket webSocket, ByteBuffer message) {
        lastPongNanos = System.nanoTime();
        return null;
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        shutdown(false);
        return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        log.warn("TL error: {}", error.toString());
        shutdown(false);
    }

    private void dispatch(byte[] msg) {
        Frame f;
        try {
            f = Frames.decode(msg);
        } catch (IllegalArgumentException e) {
            log.warn("bad frame from server ({}), dropping the TL", e.getMessage());
            shutdown();
            return;
        }
        switch (f.type) {
            case Frames.OPEN -> mux.onOpen(f.streamId, f.host, f.port, dialer);
            case Frames.DATA -> mux.onData(f.streamId, f.payload);
            case Frames.EOF -> mux.onEof(f.streamId);
            case Frames.WINDOW -> mux.onWindow(f.streamId, f.credit);
            case Frames.CLOSE -> mux.onClose(f.streamId);
            default -> log.warn("unexpected frame type {}", f.type);
        }
    }

    private void startLocalListeners() {
        for (ForwardSpec spec : locals) {
            try {
                ServerSocket ss = new ServerSocket();
                ss.setReuseAddress(true);
                ss.bind(new InetSocketAddress(spec.bindHost(), spec.bindPort()));
                listeners.add(ss);
                Thread t = new Thread(() -> acceptLoop(ss, spec), "local-" + spec.bindPort());
                t.setDaemon(true);
                t.start();
                log.info("local listener on {}:{} -> server {}:{}",
                        spec.bindHost(), spec.bindPort(), spec.dstHost(), spec.dstPort());
            } catch (Exception e) {
                log.error("cannot open local listener on {}:{} : {}",
                        spec.bindHost(), spec.bindPort(), e.toString());
            }
        }
    }

    private void acceptLoop(ServerSocket ss, ForwardSpec spec) {
        while (!ss.isClosed()) {
            Socket sock;
            try {
                sock = ss.accept();
            } catch (Exception e) {
                break;
            }
            int id = mux.nextId();
            // Сначала регистрируем, чтобы CLOSE в ответ на этот OPEN не пришёл на неизвестный
            // поток, и только потом отправляем OPEN — до того, как его сможет обогнать DATA.
            mux.prepareOutbound(id, sock, spec.dstHost() + ":" + spec.dstPort());
            enqueue(Frames.open(id, spec.dstHost(), spec.dstPort()));
            mux.startOutbound(id);
        }
    }

    private void startKeepalive() {
        if (keepaliveSec <= 0) {
            log.debug("keepalive disabled");
            return;
        }
        keepalive = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ws-keepalive");
            t.setDaemon(true);
            return t;
        });
        keepalive.scheduleAtFixedRate(this::keepaliveTick, keepaliveSec, keepaliveSec, TimeUnit.SECONDS);
    }

    private void keepaliveTick() {
        long silentNanos = System.nanoTime() - lastPongNanos;
        long limitNanos = TimeUnit.SECONDS.toNanos((long) keepaliveSec * PONG_TIMEOUT_FACTOR);
        if (silentNanos > limitNanos) {
            log.warn("no keepalive reply for {}s, dropping the TL",
                    TimeUnit.NANOSECONDS.toSeconds(silentNanos));
            shutdown();
            return;
        }
        enqueue(PING);
    }

    private void enqueue(byte[] bytes) {
        if (down.get()) {
            return;
        }
        // DATA ждёт свободного слота — так обратное давление туннеля доходит до качающих
        // потоков мультиплексора. Служебные кадры маленькие, и им нельзя стоять в очереди за
        // объёмными данными: застрявший кадр WINDOW застопорил бы тот самый поток, который
        // он и должен разблокировать.
        if (isData(bytes)) {
            try {
                while (!dataSlots.tryAcquire(1, 200, TimeUnit.MILLISECONDS)) {
                    if (down.get()) {
                        return;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (down.get()) {
                dataSlots.release();
                return;
            }
        }
        sendQ.offer(bytes);
    }

    private static boolean isData(byte[] bytes) {
        return bytes.length > 0 && bytes[0] == Frames.DATA;
    }

    private void senderLoop() {
        try {
            while (true) {
                byte[] item = sendQ.take();
                if (item == STOP) {
                    break;
                }
                if (item == PING) {
                    ws.sendPing(ByteBuffer.allocate(0)).get();
                    continue;
                }
                try {
                    ws.sendBinary(ByteBuffer.wrap(item), true).get();
                } finally {
                    if (isData(item)) {
                        dataSlots.release();
                    }
                }
            }
        } catch (Exception e) {
            shutdown();
        }
    }

    private void shutdown() {
        shutdown(true);
    }

    /**
     * @param graceful сначала отправить кадр закрытия. Имеет смысл, когда соединение
     *                 заканчиваем мы: сервер тогда сразу освободит обратные слушатели этой
     *                 сессии; бессмысленно, когда сокет и так уже мёртв.
     */
    private void shutdown(boolean graceful) {
        if (!down.compareAndSet(false, true)) {
            return;
        }
        if (keepalive != null) {
            keepalive.shutdownNow();
        }
        for (ServerSocket ss : listeners) {
            try {
                ss.close();
            } catch (Exception ignored) {
            }
        }
        if (mux != null) {
            mux.closeAll();
        }
        sendQ.offer(STOP);
        // Отпускаем всех, кто ждёт слот отправки: вычерпывать очередь больше никто не будет.
        dataSlots.release(MAX_INFLIGHT_DATA);
        WebSocket socket = ws;
        if (socket != null) {
            if (graceful) {
                // Ждём ограниченное время и всё равно обрываем: полумёртвый сокет закрытие
                // никогда не доведёт до конца, а именно этот случай тут и надо пережить.
                try {
                    socket.sendClose(WebSocket.NORMAL_CLOSURE, "bye")
                            .orTimeout(2, TimeUnit.SECONDS).exceptionally(t -> null).join();
                } catch (Exception ignored) {
                }
            }
            try {
                socket.abort();
            } catch (Exception ignored) {
            }
        }
        closed.countDown();
    }
}
