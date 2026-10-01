package com.inyeqai.tl.client;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inyeqai.tl.common.Frames;
import com.inyeqai.tl.common.Framing;

/**
 * Клиентская половина запасного HTTP-транспорта, выданная наружу как {@link WebSocket}.
 *
 * <p>Это зеркальное отражение того же приёма на сервере. {@link ClientConnection} держит всё,
 * что делает клиента клиентом — мультиплексор, локальные слушатели, очередь отправки,
 * keepalive-watchdog, картину мира health-эндпойнта, — и ничто из этого к WebSocket отношения не
 * имеет. Поэтому вместо второго клиента под HTTP этот класс реализует тот небольшой интерфейс,
 * которым {@code ClientConnection} на самом деле пользуется, и зовёт те же колбэки слушателя,
 * что позвала бы JDK. Логика соединения остаётся в одном месте для обоих транспортов.
 *
 * <p>Под капотом: один долгоживущий {@code GET} везёт кадры вниз, а кадры наверх собираются
 * пачками в короткие {@code POST}. HTTP/1.1 задан принудительно, а не согласован, потому что весь
 * смысл этого транспорта — выглядеть для всего, что стоит в середине, максимально обычным
 * трафиком.
 *
 * <p>Собственные кадры транспорта — {@code PING}, {@code PONG}, {@code BYE} и {@code ACK} —
 * здесь же и производятся, и потребляются, так что мультиплексор их вообще не видит, а
 * принесённые ими pong докладываются через {@link WebSocket.Listener#onPong}. Именно поэтому
 * проверка живости у клиента и вердикт {@code stale} на его health-эндпойнте значат на обоих
 * транспортах ровно одно и то же.
 *
 * <h2>Восстановление</h2>
 *
 * <p>Ни одна из половин HTTP-разговора, закончившись, не говорит, что с ней сделал другой конец,
 * поэтому каждое направление восстанавливается по-своему:
 *
 * <ul>
 *   <li>Закончившийся ответ <b>потока вниз</b> заменяется новым, который просит продолжить после
 *       последнего доехавшего номера кадра. Кадры нумерует сервер, и приходить они должны
 *       подряд; пропуск или повтор считается фатальным, а не терпимым, потому что и то и другое
 *       значит, что внутри потока байты пропали или удвоились, — а этого ничто выше этого слоя
 *       обнаружить не смогло бы.
 *   <li>Не прошедший POST <b>потока вверх</b> повторяется с тем же номером пачки, по которому
 *       сервер отличает повтор от новой работы. Повторять без этого значило бы рисковать
 *       применить одни и те же кадры дважды.
 * </ul>
 *
 * <p>Ради этого транспорт и существует. Прокси, ограничивающий, сколько может длиться ответ,
 * обрежет поток вниз по таймеру, и без восстановления каждый такой обрыв рвал бы все потоки в
 * туннеле; с восстановлением обрыв стоит одного круга, а потоки его даже не замечают.
 */
final class HttpLink implements WebSocket {

    private static final Logger log = LoggerFactory.getLogger(HttpLink.class);

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration UP_TIMEOUT = Duration.ofSeconds(30);
    /**
     * Сколько пытаться восстановиться, прежде чем сдаться и пересобрать туннель. Это только
     * страховка: как только сессия действительно необратимо потеряна, сервер отвечает 404 или
     * 410, и этот ответ заканчивает всё сразу. Держится заметно ниже дефолтного pong-таймаута
     * сервера, после которого восстанавливать было бы уже нечего.
     */
    private static final Duration RESUME_DEADLINE = Duration.ofSeconds(30);
    /**
     * Пауза между попытками восстановиться: достаточно большая, чтобы не крутиться впустую, и
     * достаточно маленькая, чтобы её не чувствовали.
     */
    private static final long RETRY_PAUSE_MS = 200;
    /**
     * Через сколько принятых кадров подтверждение отправляется даже когда наверх больше нечего
     * везти. Обычно подтверждения едут попутно с кадрами WINDOW, которые скачивание и так
     * производит; это на случай сессии, у которой поток вверх оказался молчаливым, — чтобы сервер
     * всё равно мог отпустить то, что держит для возможной переотправки.
     */
    private static final int ACK_EVERY_FRAMES = 64;
    /**
     * Сколько кадров DATA может лежать в очереди неотправленными — та же граница, которую
     * WebSocket-путь ставит своей очереди отправки. Отсюда на этом транспорте и берётся обратное
     * давление: отправка считается завершённой, едва кадр попал в очередь, поэтому давить назад
     * приходится самой очереди. Служебные кадры она не задерживает никогда — кадр WINDOW, ждущий
     * за объёмными данными, застопорил бы тот самый поток, ради разблокировки которого он и
     * существует.
     */
    private static final int MAX_INFLIGHT_DATA = 64;
    /**
     * Сколько байт может увезти один POST. Сборка в пачки — то, что держит пропускную способность
     * выше пола, заданного временем круга.
     */
    private static final int BATCH_BYTES = 256 * 1024;
    /** Сигнальное значение в очереди, завершающее цикл отправки наверх. */
    private static final byte[] POISON = new byte[0];

    private final HttpClient http;
    private final String base;
    private final String auth;
    private final String id;
    private final Listener listener;

    private final BlockingQueue<byte[]> up = new LinkedBlockingQueue<>();
    private final Semaphore dataSlots = new Semaphore(MAX_INFLIGHT_DATA);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean ended = new AtomicBoolean();

    /** Ответ, который читается сейчас; заменяется при каждом восстановлении. */
    private volatile InputStream down;
    /** Наибольший номер кадра, отданного мультиплексору. Пишет читатель, читает отправитель. */
    private volatile long receivedThrough;
    /**
     * Выставляется, когда сервер попрощался: это единственный конец, с которого не надо
     * восстанавливаться.
     */
    private volatile boolean serverSaidBye;
    /** До какого кадра читатель уже поставил подтверждение в очередь; только поток читателя. */
    private long ackQueuedThrough;
    /** Номера пачек, выданные серверу; только поток отправителя. */
    private long batchSeq;
    /** Наибольшее значение, уже подтверждённое серверу; только поток отправителя. */
    private long ackSentThrough;
    private int resumes;
    /** Завершается, когда прощание реально ушло, чтобы вызывающий мог его дождаться. */
    private volatile CompletableFuture<WebSocket> byeSent;

    private Thread reader;
    private Thread sender;

    /** Сбой, который повтором не вылечить: сессии нет или она не принимает то, что мы шлём. */
    private static final class Unrecoverable extends IOException {
        Unrecoverable(String message) {
            super(message);
        }
    }

    private HttpLink(HttpClient http, String base, String auth, String id,
                     Listener listener, InputStream down) {
        this.http = http;
        this.base = base;
        this.auth = auth;
        this.id = id;
        this.listener = listener;
        this.down = down;
    }

    /**
     * Открыть сессию и начать везти кадры. Возвращается, когда ответ с потоком вниз уже
     * установлен и {@link Listener#onOpen} уже позван.
     *
     * @param wsUrl тот же {@code ws://} или {@code wss://} URL, что берёт WebSocket-транспорт;
     *              здесь он отображается в {@code http}/{@code https}, так что один URL годится
     *              для обоих
     * @throws IOException если сессию открыть не удалось — вызывающий считает это неудавшейся
     *                     попыткой подключения и повторяет с нарастающей задержкой
     */
    static HttpLink open(String wsUrl, String auth, Listener listener) throws Exception {
        String base = httpBase(wsUrl);
        HttpClient http = HttpClient.newBuilder()
                // Намеренно без согласования версии повыше: этот транспорт существует для сетей,
                // которые плохо обходятся со всем необычным, а HTTP/1.1 — самое обычное, что есть.
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(CONNECT_TIMEOUT)
                .build();

        HttpResponse<String> connect = http.send(
                request(base + "/connect", auth).timeout(UP_TIMEOUT)
                        .POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
        if (connect.statusCode() != 200) {
            http.shutdownNow();
            throw new IOException("http transport: " + base + "/connect answered "
                    + connect.statusCode());
        }
        String id = connect.body().trim();
        if (id.isEmpty()) {
            http.shutdownNow();
            throw new IOException("http transport: server returned no session id");
        }

        // На потоке вниз нет таймаута запроса: он и должен стоять открытым всю жизнь туннеля,
        // а дедлайн здесь обрезал бы совершенно здоровый простаивающий поток.
        HttpResponse<InputStream> stream = http.send(
                request(base + "/down/" + id + "?from=0", auth).GET().build(),
                HttpResponse.BodyHandlers.ofInputStream());
        if (stream.statusCode() != 200) {
            closeQuiet(stream.body());
            http.shutdownNow();
            throw new IOException("http transport: " + base + "/down answered " + stream.statusCode());
        }

        HttpLink link = new HttpLink(http, base, auth, id, listener, stream.body());
        link.start();
        return link;
    }

    private void start() {
        sender = thread(this::senderLoop, "http-up");
        sender.start();
        // onOpen до старта читателя: он строит мультиплексор, который нужен входящим кадрам, и
        // кадр, пришедший раньше, попал бы на недостроенное соединение.
        listener.onOpen(this);
        reader = thread(this::readerLoop, "http-down");
        reader.start();
        log.info("http transport: session {} on {}", id, base);
    }

    /** Из {@code ws}/{@code wss}-URL — в зеркалящие его HTTP-эндпойнты. */
    static String httpBase(String wsUrl) {
        String u = wsUrl;
        if (u.startsWith("wss://")) {
            u = "https://" + u.substring("wss://".length());
        } else if (u.startsWith("ws://")) {
            u = "http://" + u.substring("ws://".length());
        }
        while (u.endsWith("/")) {
            u = u.substring(0, u.length() - 1);
        }
        return u + "/http";
    }

    private static HttpRequest.Builder request(String url, String auth) {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url));
        if (auth != null && !auth.isEmpty()) {
            b.header("X-TL-Auth", auth);
        }
        return b;
    }

    /**
     * Читать кадры столько, сколько живёт сессия, через сколько бы ответов это ни пришлось
     * сделать. Конец ответа — не конец сессии: для этого и есть {@link #resume}, и только когда
     * восстановиться не удалось, здесь сообщают, что соединение закрыто.
     */
    private void readerLoop() {
        while (!closed.get()) {
            try {
                drain(down);
            } catch (Exception e) {
                if (closed.get()) {
                    return;
                }
                log.debug("http transport: downstream ended ({})", e.toString());
            }
            if (closed.get()) {
                return;
            }
            if (serverSaidBye) {
                end("the server closed the TL");
                return;
            }
            if (!resume()) {
                return;
            }
        }
    }

    /** Прочитать один ответ до конца. Возвращается нормально, когда давать больше нечего. */
    private void drain(InputStream in) throws IOException {
        DataInputStream dis = Framing.reader(in);
        Framing.Sequenced next;
        while ((next = Framing.readSequenced(dis)) != null) {
            long expected = receivedThrough + 1;
            if (next.seq() != expected) {
                // Ни пропуск, ни повтор терпеть нельзя: первое значит, что из какого-то потока
                // пропали байты, второе — что они удвоились, и в обоих случаях всё, что выше
                // этого слоя, поехало бы дальше ни о чём не подозревая.
                throw new IOException("frame " + next.seq() + " arrived where " + expected
                        + " was due; the TL has to be rebuilt");
            }
            byte[] frame = next.frame();
            if (frame.length == 0) {
                throw new IOException("empty frame");
            }
            switch (frame[0]) {
                case Frames.PING -> offer(Frames.pong());
                case Frames.PONG -> listener.onPong(this, ByteBuffer.allocate(0));
                case Frames.BYE -> {
                    serverSaidBye = true;
                    return;
                }
                default -> listener.onBinary(this, ByteBuffer.wrap(frame), true);
            }
            // Только теперь: это число — обещание, что кадр обработан, и сервер на основании
            // этого обещания выбрасывает свою копию.
            receivedThrough = next.seq();
            if (receivedThrough - ackQueuedThrough >= ACK_EVERY_FRAMES) {
                ackQueuedThrough = receivedThrough;
                offer(Frames.ack(receivedThrough));
            }
        }
    }

    /**
     * Попросить новый ответ с потоком вниз, продолжающий после последнего кадра, который у нас
     * есть.
     *
     * @return true, когда ответ прицеплен; false, когда сессию восстановить нельзя — о закрытии
     *         соединения к этому моменту уже сообщено, и вызывающий пересоберёт туннель
     */
    private boolean resume() {
        closeQuiet(down);
        long deadline = System.nanoTime() + RESUME_DEADLINE.toNanos();
        boolean firstTry = true;
        while (!closed.get() && System.nanoTime() < deadline) {
            // Перед первой попыткой паузы нет. Обрезанный ответ здесь — обычное дело, и с
            // сервером чаще всего всё в порядке, так что восстановление должно стоить одного
            // круга, а не фиксированного ожидания; притормаживать есть смысл только с сервером,
            // который правда недоступен.
            if (!firstTry && !pause()) {
                return false;
            }
            firstTry = false;
            long from = receivedThrough;
            try {
                HttpResponse<InputStream> res = http.send(
                        request(base + "/down/" + id + "?from=" + from, auth).GET().build(),
                        HttpResponse.BodyHandlers.ofInputStream());
                int code = res.statusCode();
                if (code == 200) {
                    down = res.body();
                    resumes++;
                    log.info("http transport: session {} resumed after frame {} ({} so far)",
                            id, from, resumes);
                    return true;
                }
                closeQuiet(res.body());
                if (code == 409) {
                    continue; // предыдущий ответ ещё доигрывается; спрашиваем снова
                }
                end("the server will not resume this session (" + code + ")");
                return false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (IOException e) {
                log.debug("http transport: cannot reach the server to resume ({})", e.toString());
            }
        }
        if (closed.get()) {
            return false;
        }
        end("could not resume within " + RESUME_DEADLINE.toSeconds() + "s");
        return false;
    }

    /** @return false, если транспорт пропал, пока мы выжидали паузу */
    private boolean pause() {
        try {
            Thread.sleep(RETRY_PAUSE_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        return !closed.get();
    }

    private void senderLoop() {
        List<byte[]> batch = new ArrayList<>();
        try {
            while (true) {
                batch.clear();
                byte[] first = up.take();
                if (first == POISON) {
                    return;
                }
                batch.add(first);
                int total = first.length;
                boolean ending = false;
                // Забираем всё, что уже ждёт: один запрос на много кадров — то, что не даёт
                // пропускной способности упасть до пола «один круг на кадр».
                while (total < BATCH_BYTES) {
                    byte[] next = up.poll();
                    if (next == null) {
                        break;
                    }
                    if (next == POISON) {
                        ending = true;
                        break;
                    }
                    batch.add(next);
                    total += next.length;
                }
                // Подтверждение едет попутно с тем, что и так идёт наверх: девять байт, и именно
                // они позволяют серверу перестать держать кадры для переотправки.
                long through = receivedThrough;
                if (through > ackSentThrough) {
                    batch.add(0, Frames.ack(through));
                }
                boolean goodbye = contains(batch, Frames.BYE);
                try {
                    post(++batchSeq, batch, UP_TIMEOUT);
                    ackSentThrough = through;
                    if (goodbye) {
                        CompletableFuture<WebSocket> f = byeSent;
                        if (f != null) {
                            f.complete(this);
                        }
                    }
                } finally {
                    // Кредит возвращается, когда кадры действительно ушли, — чтобы граница
                    // значила то, что обещает.
                    dataSlots.release(countData(batch));
                }
                if (ending) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            if (!closed.get()) {
                fail(e);
            }
        }
    }

    /**
     * Отправить пачку кадров одним телом запроса, повторяя с тем же номером пачки, пока сервер
     * её не примет или не скажет, что не примет никогда.
     *
     * <p>Безопасным повтор делает именно номер пачки. Не прошедший POST не говорит, применил его
     * сервер или нет, поэтому простой повтор мог бы применить те же кадры дважды; а с номером
     * сервер на повтор отвечает, но не применяет его.
     */
    private void post(long batch, List<byte[]> frames, Duration timeout)
            throws IOException, InterruptedException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        for (byte[] frame : frames) {
            Framing.write(body, frame);
        }
        byte[] bytes = body.toByteArray();
        long deadline = System.nanoTime() + RESUME_DEADLINE.toNanos();
        int attempt = 0;
        while (true) {
            attempt++;
            try {
                HttpResponse<Void> res = http.send(
                        request(base + "/up/" + id + "?batch=" + batch, auth).timeout(timeout)
                                .header("Content-Type", "application/octet-stream")
                                .POST(HttpRequest.BodyPublishers.ofByteArray(bytes)).build(),
                        HttpResponse.BodyHandlers.discarding());
                int code = res.statusCode();
                if (code >= 200 && code < 300) {
                    if (attempt > 1) {
                        log.info("http transport: batch {} went through on attempt {}", batch, attempt);
                    }
                    return;
                }
                if (code < 500) {
                    // В первую очередь 404 и 410: сессии нет или эту пачку она не возьмёт.
                    // Повтор тут ничего не меняет.
                    throw new Unrecoverable("upstream batch " + batch + " answered " + code);
                }
                log.debug("http transport: batch {} answered {}, trying again", batch, code);
            } catch (Unrecoverable e) {
                throw e;
            } catch (IOException e) {
                log.debug("http transport: batch {} did not get through ({})", batch, e.toString());
            }
            if (closed.get() || System.nanoTime() > deadline) {
                throw new IOException("upstream batch " + batch + " never got through");
            }
            Thread.sleep(RETRY_PAUSE_MS);
        }
    }

    private static boolean contains(List<byte[]> frames, byte type) {
        for (byte[] frame : frames) {
            if (frame.length > 0 && frame[0] == type) {
                return true;
            }
        }
        return false;
    }

    private static int countData(List<byte[]> frames) {
        int n = 0;
        for (byte[] frame : frames) {
            if (isData(frame)) {
                n++;
            }
        }
        return n;
    }

    private static boolean isData(byte[] frame) {
        return frame.length > 0 && frame[0] == Frames.DATA;
    }

    /** Поставить кадр в очередь на следующий запрос. Служебные кадры ждать не заставляют. */
    private void offer(byte[] frame) {
        if (!closed.get()) {
            up.offer(frame);
        }
    }

    /**
     * Дождаться разрешения поставить в очередь ещё один кадр DATA. Опрашивает, а не блокируется
     * наглухо, чтобы транспорт, разобранный пока отправитель здесь припаркован, не держал поток
     * вечно.
     *
     * @return false, если транспорт пропал за время ожидания; кадр тогда выбрасывается:
     *         транспорта нет, и его потоки умирают вместе с ним
     */
    private boolean acquireDataSlot() {
        try {
            while (!closed.get()) {
                if (dataSlots.tryAcquire(200, TimeUnit.MILLISECONDS)) {
                    return true;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    /**
     * Транспорт закончился окончательно. Сообщается как закрытие, чтобы вызывающий
     * переподключился.
     */
    private void end(String reason) {
        if (ended.compareAndSet(false, true)) {
            log.warn("http transport: {}", reason);
            listener.onClose(this, WebSocket.NORMAL_CLOSURE, reason);
        }
    }

    private void fail(Throwable t) {
        if (ended.compareAndSet(false, true)) {
            listener.onError(this, t);
        }
    }

    @Override
    public CompletableFuture<WebSocket> sendBinary(ByteBuffer data, boolean last) {
        byte[] frame = new byte[data.remaining()];
        data.get(frame);
        if (isData(frame) && !acquireDataSlot()) {
            return CompletableFuture.completedFuture(this);
        }
        offer(frame);
        // Завершаем, едва кадр попал в очередь, а не когда он уже ушёл POST-ом: ожидание здесь
        // давало бы копиться только одному кадру за раз и сводило бы каждую пачку к одному
        // кадру, превращая пропускную способность в один круг на 16 КБ.
        return CompletableFuture.completedFuture(this);
    }

    @Override
    public CompletableFuture<WebSocket> sendPing(ByteBuffer message) {
        offer(Frames.ping());
        return CompletableFuture.completedFuture(this);
    }

    @Override
    public CompletableFuture<WebSocket> sendClose(int statusCode, String reason) {
        // В очередь, как и всё остальное, — чтобы номера пачек оставались в руках одного потока,
        // а возвращённый future говорил, когда прощание реально ушло. Вызывающий даёт ему немного
        // времени, прежде чем обрывать: прощание этого стоит, потому что сразу освобождает
        // обратные слушатели на сервере, но не больше — транспорт всё равно уходит.
        CompletableFuture<WebSocket> f = new CompletableFuture<>();
        byeSent = f;
        offer(Frames.bye());
        return f;
    }

    @Override
    public void abort() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        up.clear();
        up.offer(POISON);
        // Отпускаем всех, кто припаркован в ожидании слота под кадр DATA: вычерпывать уже некому.
        dataSlots.release(MAX_INFLIGHT_DATA);
        // Закрытие потока вниз — это то, что разблокирует читателя, припаркованного на чтении.
        closeQuiet(down);
        if (sender != null) {
            sender.interrupt();
        }
        if (reader != null && reader != Thread.currentThread()) {
            reader.interrupt();
        }
        try {
            http.shutdownNow();
        } catch (Exception ignored) {
            // сделать с этим уже нечего; транспорт так и так потерян
        }
    }

    @Override
    public boolean isInputClosed() {
        return closed.get();
    }

    @Override
    public boolean isOutputClosed() {
        return closed.get();
    }

    @Override
    public void request(long n) {
        // Кадры читаются с той скоростью, с какой приходят; сообщать о спросе нечего.
    }

    @Override
    public String getSubprotocol() {
        return "";
    }

    @Override
    public CompletableFuture<WebSocket> sendText(CharSequence data, boolean last) {
        throw new UnsupportedOperationException("the TL protocol is binary only");
    }

    @Override
    public CompletableFuture<WebSocket> sendPong(ByteBuffer message) {
        // Pong — это ответы на кадры PING, и их ставит в очередь сам читатель; напрямую в
        // клиенте их никто не запрашивает.
        throw new UnsupportedOperationException("pongs are answered by the transport");
    }

    private static Thread thread(Runnable body, String name) {
        Thread t = new Thread(body, name);
        t.setDaemon(true);
        return t;
    }

    private static void closeQuiet(InputStream in) {
        try {
            if (in != null) {
                in.close();
            }
        } catch (IOException ignored) {
        }
    }
}
