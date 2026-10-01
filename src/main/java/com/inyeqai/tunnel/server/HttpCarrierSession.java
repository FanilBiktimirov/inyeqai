package com.inyeqai.tunnel.server;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.PingMessage;
import org.springframework.web.socket.WebSocketExtension;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;

import com.inyeqai.tunnel.common.Frames;
import com.inyeqai.tunnel.common.Framing;

/**
 * HTTP-транспорт, переодетый в {@link WebSocketSession}.
 *
 * <p>В этом весь трюк запасного транспорта. Всё, что делает сессию туннеля сессией —
 * мультиплексор, обратные слушатели, список разрешённых адресов, keepalive-отстрел замолчавших
 * сессий, который освобождает порты, занятые исчезнувшим клиентом, страница {@code /status} —
 * живёт в {@link TunnelWebSocketHandler} и относится к сессиям, а не к WebSocket'ам. Вместо
 * того чтобы отращивать для HTTP вторую копию всего этого, HTTP-транспорт притворяется перед
 * этим обработчиком обычной сессией: {@link HttpTunnelController} вызывает те же колбэки
 * {@code afterConnectionEstablished} / {@code handleMessage} / {@code afterConnectionClosed},
 * что вызвал бы контейнер. Обработчик не тронут и не может разойтись со второй реализацией,
 * потому что второй реализации нет.
 *
 * <p>Различаются два транспорта кадрированием, тем, как решается живость, и временем жизни — и
 * эти различия заканчиваются здесь:
 * <ul>
 *   <li>Исходящие кадры складываются в очередь и пишутся в тот ответ потока вниз, который
 *       сейчас подключён, каждый с префиксом длины ({@link Framing}), потому что у HTTP-тела
 *       нет границ сообщений.
 *   <li>{@link PingMessage} от отстрела замолчавших сессий в обработчике превращается в кадр
 *       {@code PING}, а кадр {@code PONG}, который на него отвечает, докладывается обработчику
 *       обратно как {@code PongMessage}. Поэтому единственная мера живости сессии, которая есть
 *       у обработчика — давность последнего pong — значит на обоих транспортах ровно одно и то
 *       же.
 *   <li>Сессия <b>переживает свой ответ потока вниз</b>. Ответ прокси рубят по своим
 *       соображениям; сессию — нет. См. {@link #pumpTo}.
 * </ul>
 *
 * <h2>Восстановление сессии</h2>
 *
 * <p>Каждый кадр, идущий вниз, получает номер и хранится, пока клиент не подтвердит его через
 * {@code ACK}. Когда ответ умирает, клиент возвращается и просит продолжить после последнего
 * номера, который у него есть, и всё, что идёт после, пишется заново в новый ответ.
 *
 * <p>Безопасным, а не просто правдоподобным, это делает буфер. Кадр мог быть записан в ответ,
 * который уже был мёртв, и по самой записи этого никак не видно, так что «отправлен» не может
 * значить «доставлен»: значить это может только {@code ACK}. Без буфера восстановленная сессия
 * тихо пропустила бы эти кадры, а для байтового потока это порча данных, которую не заметит ни
 * одна из сторон. Буфер ограничен ({@link #RESUME_BUFFER_BYTES}), и когда он полон, писатель
 * ждёт, а не растёт: ни одному клиенту не позволено заставить сервер держать для него данные
 * без предела.
 */
final class HttpCarrierSession implements WebSocketSession {

    private static final Logger log = LoggerFactory.getLogger(HttpCarrierSession.class);

    /**
     * Сколько кадров может ждать писателя потока вниз. Собственные окна мультиплексора на
     * каждый поток ограничивают поток по отдельности; это ограничивает общую сумму, так что
     * клиент, который перестал читать, не может заставить сервер буферизовать без предела. Это
     * же — бюджет, на котором сессия переживает обрыв: всё, что накопится здесь, пока ни один
     * ответ не подключён.
     */
    private static final int QUEUE_FRAMES = 256;
    /** Сколько отправитель ждёт места в очереди, прежде чем сессию спишут в утиль. */
    private static final long OFFER_TIMEOUT_MS = 30_000;
    /**
     * Сколько записанных, но ещё не подтверждённых байт одна сессия может держать для
     * переотправки. Дальше этого писатель ждёт подтверждений. Клиент, который вообще читает,
     * подтверждает за один круг, так что при обычной работе до предела дело не доходит;
     * доходит, когда клиент не читает ничего, — а это ровно тот случай, когда сервер и должен
     * перестать для него что-то производить.
     */
    private static final int RESUME_BUFFER_BYTES = 2 * 1024 * 1024;
    /**
     * Сколько писатель ждёт подтверждения, прежде чем поставить на сессии крест. Сосед, который
     * забирает кадры и никогда их не подтверждает, сломан так, что ожиданием это не лечится.
     */
    private static final long ACK_TIMEOUT_MS = 60_000;
    /** Сколько новый поток вниз ждёт, пока предыдущий отпустит слот, прежде чем ответить 409. */
    private static final long ATTACH_WAIT_MS = 5_000;
    /** Маркер в очереди, который завершает цикл писателя потока вниз. */
    private static final byte[] POISON = new byte[0];

    private final String id;
    private final URI uri;
    private final InetSocketAddress remote;
    private final InetSocketAddress local;
    private final Closer onClose;

    private final BlockingQueue<byte[]> outbound = new ArrayBlockingQueue<>(QUEUE_FRAMES);
    private final Map<String, Object> attributes = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    /** Одновременно только один ответ потока вниз; новый ждёт, пока старый отпустит слот. */
    private final Semaphore downSlot = new Semaphore(1);
    private volatile Thread pumpThread;
    private final AtomicLong attachments = new AtomicLong();

    /** Охраняет буфер переотправки и счётчики номеров кадров. */
    private final ReentrantLock resume = new ReentrantLock();
    private final Condition acknowledged = resume.newCondition();
    private final Deque<Pending> unacked = new ArrayDeque<>();
    private long nextSeq = 1;
    private long ackedThrough;
    private int unackedBytes;

    /**
     * Выстраивает в одну линию сторону потока вверх и охраняет {@link #lastBatch}. Каждый POST
     * несёт пачку кадров, и клиент отправляет их по одному за раз, но в HTTP ничто этого не
     * гарантирует: два запроса, пришедшие наперегонки, отдали бы мультиплексору кадры не по
     * порядку, а для байтового потока порядок — это всё.
     */
    private final ReentrantLock inbound = new ReentrantLock();
    private long lastBatch;

    private volatile int textLimit = 64 * 1024;
    private volatile int binaryLimit = Framing.MAX_FRAME;

    /** Кадр потока вниз, который записан, но ещё не подтверждён. */
    private record Pending(long seq, byte[] frame) {
    }

    /** Ему сообщают один раз, когда сессия кончилась: владелец забывает её и дёргает обработчик. */
    interface Closer {
        void closed(HttpCarrierSession session, CloseStatus status);
    }

    /** Бросается, когда клиент просит продолжить с точки, которую эта сессия выдать не может. */
    static final class CannotResume extends IOException {
        CannotResume(String message) {
            super(message);
        }
    }

    HttpCarrierSession(String id, URI uri, InetSocketAddress remote, InetSocketAddress local,
                       Closer onClose) {
        this.id = id;
        this.uri = uri;
        this.remote = remote;
        this.local = local;
        this.onClose = onClose;
    }

    /**
     * Занять слот потока вниз, вытеснив писателя, который ещё не заметил, что его ответ мёртв.
     * Вытеснение здесь и есть смысл: обычно клиент узнаёт об обрыве ответа задолго до того, как
     * у сервера свалится запись, и без этого его первую попытку вернуться отшил бы труп.
     * Вытеснением ничего не теряется: неподтверждённый кадр переотправляется в новом ответе
     * независимо от того, успел старый писатель его отдать или нет.
     *
     * @return false, если предыдущий писатель не отпустил слот вовремя; вызывающий отвечает 409,
     *         и клиент пробует ещё раз чуть позже
     */
    boolean attachDown() {
        Thread holder = pumpThread;
        if (holder != null) {
            holder.interrupt(); // выдёргивает его из ожидания кадра
        }
        try {
            if (!downSlot.tryAcquire(ATTACH_WAIT_MS, TimeUnit.MILLISECONDS)) {
                log.warn("downstream of {} is still held by a writer that will not let go", id);
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        return true;
    }

    /** Можно ли ещё выдать кадры начиная с {@code fromSeq + 1}. */
    boolean canResumeFrom(long fromSeq) {
        resume.lock();
        try {
            return fromSeq >= ackedThrough && fromSeq <= nextSeq - 1;
        } finally {
            resume.unlock();
        }
    }

    /** Сколько раз эту сессию приходилось восстанавливать — для {@code /status}. */
    long resumes() {
        return Math.max(0, attachments.get() - 1);
    }

    /** Неподтверждённые байты, которые держим на случай переотправки — для {@code /status}. */
    int unackedBytes() {
        resume.lock();
        try {
            return unackedBytes;
        } finally {
            resume.unlock();
        }
    }

    /** Замок потока вверх; держится, пока пачку кадров отдают обработчику. */
    ReentrantLock inboundLock() {
        return inbound;
    }

    /**
     * Решить, что делать с пачкой потока вверх. Вызывать с захваченным {@link #inboundLock}.
     *
     * @return true — применять; false — это повтор уже применённой пачки: клиент не мог знать,
     *         потерялся ли наш ответ по пути назад, поэтому спросил снова, а применить те же
     *         кадры дважды значит продублировать байты внутри потока
     * @throws CannotResume если пачку пропустили: кадров не хватает, и из того, что пришло, уже
     *                      не собрать никакое последующее состояние
     */
    boolean beginBatch(long batch) throws CannotResume {
        if (batch <= lastBatch) {
            return false;
        }
        if (batch != lastBatch + 1) {
            throw new CannotResume("upstream batch " + batch + " arrived with "
                    + (batch - lastBatch - 1) + " missing before it");
        }
        return true;
    }

    /** Отметить пачку применённой. Вызывать с захваченным {@link #inboundLock}. */
    void batchApplied(long batch) {
        lastBatch = batch;
    }

    /** Клиент подтверждает, что держит все кадры потока вниз до {@code through} включительно. */
    void onAck(long through) {
        resume.lock();
        try {
            if (through >= nextSeq) {
                // Он заявляет кадры, которых мы никогда не писали. Что-то сильно поехало;
                // продолжать значило бы поверить и в следующее его заявление.
                log.warn("client {} acknowledged frame {} but only {} were sent, closing",
                        id, through, nextSeq - 1);
                // Закрывать, держа этот замок, безопасно: он реентрантный, и закрытие не берёт
                // ничего, что ждало бы поток, которому этот замок может понадобиться.
                close(CloseStatus.PROTOCOL_ERROR);
                return;
            }
            if (through <= ackedThrough) {
                return;
            }
            ackedThrough = through;
            while (!unacked.isEmpty() && unacked.peekFirst().seq() <= through) {
                unackedBytes -= unacked.removeFirst().frame().length;
            }
            acknowledged.signalAll();
        } finally {
            resume.unlock();
        }
    }

    /** Поставить кадр в очередь на ответ потока вниз, как это делает {@link #sendMessage}. */
    void enqueue(byte[] frame) throws IOException {
        if (closed.get()) {
            return;
        }
        try {
            if (!outbound.offer(frame, OFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                // Здесь сессию заканчиваем, а не просто сообщаем о проблеме. Вызывающий
                // логирует неудачную отправку и идёт дальше — для WebSocket'а это верно, там
                // сорвавшаяся отправка означает сокет, который контейнер и так закроет. Этот
                // транспорт не закрывает никто, кроме нас, а кадр, тихо выпавший из байтового
                // потока, — это порча данных, которую соседу нечем обнаружить.
                log.warn("client {} is not draining the tunnel, closing the session", id);
                close(CloseStatus.SESSION_NOT_RELIABLE);
                throw new IOException("client is not draining the tunnel, gave up on session " + id);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while queueing a frame for " + id, e);
        }
    }

    /**
     * Писать кадры в ответ потока вниз, пока тот не кончится. Блокирует вызывающий поток на всё
     * время жизни этого ответа, а это не то же самое, что время жизни сессии.
     *
     * <p>Начинает с того, что переотправляет всё, что клиент не подтвердил, а потом переходит к
     * новым кадрам. Первым в очередь ставится {@code PING}, чтобы заголовки ответа ушли сразу и
     * сказали клиенту, что транспорт рабочий, а не оставляли его догадываться по тишине. Каждая
     * пачка флашится, потому что туннель, который ждёт, пока наполнится буфер, добавлял бы
     * задержку к каждому запросу и мог бы застопорить обмен, где одна сторона ждёт ответа.
     *
     * <p>Когда метод возвращается, сессия остаётся живой, а слот — отпущенным. В этом вся
     * разница, которую вносит восстановление: кончившийся ответ — это то, из чего надо
     * выкарабкаться, и только отстрел замолчавших сессий по keepalive — который спрашивает,
     * отвечает ли ещё <em>клиент</em>, а не открыт ли ещё конкретный ответ — решает, что сессия
     * кончилась.
     */
    void pumpTo(OutputStream out, long fromSeq) {
        // Поток из пула может нести прерывание, адресованное писателю, который пользовался им
        // до нас; иначе этот флаг прибил бы наш ответ ещё до того, как он что-то отправил.
        Thread.interrupted();
        pumpThread = Thread.currentThread();
        long attachment = attachments.incrementAndGet();
        List<byte[]> batch = new ArrayList<>();
        try {
            List<Pending> replay = replayFrom(fromSeq);
            if (attachment > 1) {
                log.info("downstream of {} resumed from frame {} ({} frame(s) to send again, "
                                + "resumed {} time(s) so far)",
                        id, fromSeq, replay.size(), resumes());
            }
            // Если он потеряется — не беда: приветствие это не трафик туннеля.
            outbound.offer(Frames.ping());
            for (Pending p : replay) {
                Framing.write(out, p.seq(), p.frame());
            }
            out.flush();

            while (true) {
                batch.clear();
                batch.add(outbound.take());
                outbound.drainTo(batch); // всё остальное, что уже ждёт, — одним флашем

                int upToPoison = batch.size();
                for (int i = 0; i < batch.size(); i++) {
                    if (batch.get(i) == POISON) {
                        upToPoison = i;
                        break;
                    }
                }
                // Записать всю пачку в буфер, прежде чем писать хоть один кадр. Кадр, вынутый
                // из очереди, в безопасности только когда он уже в буфере переотправки: если
                // писать сначала, то сбой на середине пачки потерял бы кадры, которые ещё на
                // руках, а поскольку номера им так и не выдали, пропуск был бы невидим обеим
                // сторонам.
                List<Pending> pending = new ArrayList<>(upToPoison);
                for (int i = 0; i < upToPoison; i++) {
                    pending.add(reserve(batch.get(i)));
                }
                for (Pending p : pending) {
                    Framing.write(out, p.seq(), p.frame());
                }
                out.flush();
                if (upToPoison < batch.size()) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            // Нас сменяет более новый поток вниз, или сессия уходит.
            log.debug("downstream of {} handed over", id);
        } catch (CannotResume e) {
            log.warn("cannot resume {}: {}", id, e.getMessage());
            close(CloseStatus.SERVER_ERROR);
        } catch (IOException e) {
            // Ответа больше нет. Сессия — есть: мы ждём, что клиент вернётся и попросит
            // продолжить, и пока он этого не сделал или не перестал отвечать на keepalive, его
            // потоки и обратные слушатели остаются точно такими, какими были.
            log.debug("downstream of {} ended: {}", id, e.toString());
        } finally {
            pumpThread = null;
            // Не оставляем флаг выставленным на потоке из пула — его унаследовала бы следующая
            // задача.
            Thread.interrupted();
            downSlot.release();
        }
    }

    /**
     * Подтвердить всё до {@code fromSeq} включительно и вернуть то, что ещё надо записать
     * заново. Сама просьба клиента продолжить с какой-то точки — доказательство, что всё до неё
     * у него есть, так что она заодно работает подтверждением.
     */
    private List<Pending> replayFrom(long fromSeq) throws CannotResume {
        resume.lock();
        try {
            if (fromSeq < ackedThrough) {
                throw new CannotResume("client asks to continue from frame " + fromSeq
                        + ", which it already confirmed past (" + ackedThrough + ")");
            }
            if (fromSeq > nextSeq - 1) {
                throw new CannotResume("client asks to continue from frame " + fromSeq
                        + ", but only " + (nextSeq - 1) + " were ever sent");
            }
            ackedThrough = fromSeq;
            while (!unacked.isEmpty() && unacked.peekFirst().seq() <= fromSeq) {
                unackedBytes -= unacked.removeFirst().frame().length;
            }
            return new ArrayList<>(unacked);
        } finally {
            resume.unlock();
        }
    }

    /**
     * Взять для кадра следующий номер и отложить кадр на случай переотправки. Когда буфер
     * полон — ждёт, и это ровно то обратное давление, которого заслуживает клиент, не читающий
     * ничего; кадр попадает в буфер до записи, так что кадр, потерянный на сорвавшейся записи,
     * всё равно остаётся кадром, который мы можем отправить снова.
     */
    private Pending reserve(byte[] frame) throws IOException, InterruptedException {
        resume.lock();
        try {
            long waitNanos = TimeUnit.MILLISECONDS.toNanos(ACK_TIMEOUT_MS);
            // В пустой буфер всегда влезает ещё один кадр, так что кадр, который больше всего
            // бюджета, не может заклинить сессию.
            while (!unacked.isEmpty() && unackedBytes + frame.length > RESUME_BUFFER_BYTES) {
                if (closed.get()) {
                    throw new IOException("session " + id + " closed while waiting to send");
                }
                if (waitNanos <= 0) {
                    log.warn("client {} is reading but never acknowledging, closing the session", id);
                    close(CloseStatus.SESSION_NOT_RELIABLE);
                    throw new IOException("no acknowledgements from " + id);
                }
                waitNanos = acknowledged.awaitNanos(waitNanos);
            }
            Pending pending = new Pending(nextSeq++, frame);
            unacked.addLast(pending);
            unackedBytes += frame.length;
            return pending;
        } finally {
            resume.unlock();
        }
    }

    @Override
    public void sendMessage(WebSocketMessage<?> message) throws IOException {
        if (message instanceof BinaryMessage binary) {
            ByteBuffer payload = binary.getPayload().duplicate();
            byte[] frame = new byte[payload.remaining()];
            payload.get(frame);
            enqueue(frame);
        } else if (message instanceof PingMessage) {
            // keepalive обработчика, переведённый в то, что способно проехать в HTTP-теле.
            enqueue(Frames.ping());
        } else {
            log.debug("ignoring a {} on the HTTP carrier {}", message.getClass().getSimpleName(), id);
        }
    }

    @Override
    public boolean isOpen() {
        return !closed.get();
    }

    @Override
    public void close() {
        close(CloseStatus.NORMAL);
    }

    @Override
    public void close(CloseStatus status) {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        // Сначала чистим: в полной очереди не нашлось бы места для маркера, а то, что в ней
        // осталось, всё равно уже никогда не будет записано.
        outbound.clear();
        outbound.offer(POISON);
        resume.lock();
        try {
            unacked.clear();
            unackedBytes = 0;
            acknowledged.signalAll(); // отпускаем всех, кто ждёт подтверждения
        } finally {
            resume.unlock();
        }
        onClose.closed(this, status);
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public HttpHeaders getHandshakeHeaders() {
        return HttpHeaders.EMPTY;
    }

    @Override
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    @Override
    public Principal getPrincipal() {
        return null;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return local;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return remote;
    }

    @Override
    public String getAcceptedProtocol() {
        return null;
    }

    @Override
    public void setTextMessageSizeLimit(int limit) {
        this.textLimit = limit;
    }

    @Override
    public int getTextMessageSizeLimit() {
        return textLimit;
    }

    @Override
    public void setBinaryMessageSizeLimit(int limit) {
        this.binaryLimit = limit;
    }

    @Override
    public int getBinaryMessageSizeLimit() {
        return binaryLimit;
    }

    @Override
    public List<WebSocketExtension> getExtensions() {
        return List.of();
    }
}
