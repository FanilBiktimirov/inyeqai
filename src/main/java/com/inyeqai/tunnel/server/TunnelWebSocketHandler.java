package com.inyeqai.tunnel.server;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import jakarta.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.PingMessage;
import org.springframework.web.socket.PongMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.BinaryWebSocketHandler;

import com.inyeqai.tunnel.common.Frame;
import com.inyeqai.tunnel.common.Frames;
import com.inyeqai.tunnel.common.Mux;
import com.inyeqai.tunnel.common.Reverse;

/**
 * Серверный конец туннеля. Одна {@link Session} на каждое WebSocket-подключение держит
 * {@link Mux} и все обратные слушатели, которые это подключение открыло. Отправки
 * сериализуются на локе своей сессии, потому что в {@code WebSocketSession} нельзя писать
 * параллельно.
 *
 * <p>Сервер пингует своих клиентов и закрывает сессии, от которых перестали приходить
 * pong'и. Это не косметика: клиент, исчезнувший без TCP-закрытия (ноутбук ушёл в сон,
 * истекла запись в NAT), иначе навсегда удерживал бы свои обратные слушатели, и тот же
 * клиент, переподключившись, никогда не смог бы занять эти порты заново.
 */
@Component
public class TunnelWebSocketHandler extends BinaryWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(TunnelWebSocketHandler.class);
    private static final int MAX_MESSAGE = 256 * 1024;
    private static final int DIAL_TIMEOUT_MS = 10_000;

    private final TunnelProperties props;
    private final AddressPolicy policy;
    private final ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();
    /** Какая сессия держит каждый обратный порт — чтобы можно было выселить залипшего держателя. */
    private final ConcurrentHashMap<Integer, Session> reversePorts = new ConcurrentHashMap<>();
    private final ScheduledExecutorService keepalive;

    private final Mux.Dialer dialer = (host, port) -> {
        Socket s = new Socket();
        s.connect(new InetSocketAddress(host, port), DIAL_TIMEOUT_MS);
        return s;
    };

    public TunnelWebSocketHandler(TunnelProperties props) {
        this.props = props;
        this.policy = new AddressPolicy(props.getAllow());
        if (policy.unrestricted()) {
            log.info("no tunnel.allow list: clients may reach any address this server can");
        } else {
            log.info("restricting clients to {} allowed address pattern(s)", props.getAllow().size());
        }
        long periodMs = props.getKeepalive().toMillis();
        if (periodMs > 0) {
            keepalive = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "tunnel-keepalive");
                t.setDaemon(true);
                return t;
            });
            keepalive.scheduleAtFixedRate(this::pingAndReap, periodMs, periodMs, TimeUnit.MILLISECONDS);
        } else {
            keepalive = null;
            log.info("server keepalive disabled; dead sessions are only noticed by TCP");
        }
    }

    /** Состояние одного подключения. */
    private final class Session {
        private final WebSocketSession ws;
        private final ReentrantLock sendLock = new ReentrantLock();
        private final List<ServerSocket> listeners = new CopyOnWriteArrayList<>();
        private final AtomicLong lastPongNanos = new AtomicLong(System.nanoTime());
        private final AtomicBoolean configured = new AtomicBoolean();
        private final long connectedAtNanos = System.nanoTime();
        private final Mux mux;

        Session(WebSocketSession ws) {
            this.ws = ws;
            this.mux = new Mux(this::send, true, policy::allowsDial);
        }

        SessionSnapshot snapshot() {
            List<Integer> ports = new ArrayList<>();
            for (ServerSocket ss : listeners) {
                ports.add(ss.getLocalPort());
            }
            // Восстановить можно только HTTP-транспорт, так что и сказать тут есть что только
            // ему. Спросить его напрямую лучше, чем выдумывать общее понятие состояния
            // транспорта для вопроса, на который один из транспортов вообще не отвечает, а
            // туннель, восстанавливающийся каждые пару секунд, — ровно та тихая деградация,
            // ради которой /status и существует.
            long resumes = 0;
            int holding = 0;
            if (ws instanceof HttpCarrierSession carrier) {
                resumes = carrier.resumes();
                holding = carrier.unackedBytes();
            }
            return new SessionSnapshot(
                    ws.getId(),
                    ws.getRemoteAddress() == null ? "?" : ws.getRemoteAddress().toString(),
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - connectedAtNanos),
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastPongNanos.get()),
                    mux.openStreams(),
                    mux.streamsOpened(),
                    mux.bytesToPeer(),
                    mux.bytesFromPeer(),
                    ports,
                    mux.streams(),
                    resumes,
                    holding);
        }

        void send(byte[] bytes) {
            sendLock.lock();
            try {
                if (ws.isOpen()) {
                    ws.sendMessage(new BinaryMessage(bytes));
                }
            } catch (Exception e) {
                log.debug("send failed on {}: {}", ws.getId(), e.toString());
            } finally {
                sendLock.unlock();
            }
        }

        /**
         * Пинг без ожидания лока на отправку. Если отправка уже идёт, значит в этого клиента
         * прямо сейчас пишут, и пропустить один пинг ничего не стоит; а вот заблокироваться
         * здесь стоило бы дорого: один клиент, переставший читать, задержал бы единственный
         * keepalive-поток, а с ним — пинги и отстрел всех остальных сессий.
         */
        void ping() {
            boolean held = false;
            try {
                held = sendLock.tryLock(100, TimeUnit.MILLISECONDS);
                if (!held) {
                    log.debug("skipping ping on {}: a send is in flight", ws.getId());
                    return;
                }
                if (ws.isOpen()) {
                    ws.sendMessage(new PingMessage(ByteBuffer.allocate(0)));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.debug("ping failed on {}: {}", ws.getId(), e.toString());
            } finally {
                if (held) {
                    sendLock.unlock();
                }
            }
        }

        boolean staleFor(long nanos) {
            return System.nanoTime() - lastPongNanos.get() > nanos;
        }

        void closeListeners() {
            for (ServerSocket ss : listeners) {
                reversePorts.remove(ss.getLocalPort(), this);
                closeQuiet(ss);
            }
            listeners.clear();
        }
    }

    /** Живой срез всех сессий, со свежайшим ответом на keepalive. Для {@code GET /status}. */
    public List<SessionSnapshot> snapshot() {
        List<SessionSnapshot> out = new ArrayList<>();
        for (Session s : sessions.values()) {
            out.add(s.snapshot());
        }
        out.sort(Comparator.comparing(SessionSnapshot::id));
        return out;
    }

    @PreDestroy
    void stopKeepalive() {
        if (keepalive != null) {
            keepalive.shutdownNow();
        }
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        session.setBinaryMessageSizeLimit(MAX_MESSAGE);
        sessions.put(session.getId(), new Session(session));
        log.info("tunnel client connected: {}", session.getId());
    }

    @Override
    protected void handleBinaryMessage(WebSocketSession session, BinaryMessage message) {
        Session ctx = sessions.get(session.getId());
        if (ctx == null) {
            return;
        }
        ByteBuffer bb = message.getPayload();
        byte[] b = new byte[bb.remaining()];
        bb.get(b);

        Frame f;
        try {
            f = Frames.decode(b);
        } catch (IllegalArgumentException e) {
            log.warn("bad frame on {} ({}), closing", session.getId(), e.getMessage());
            closeQuiet(session, CloseStatus.BAD_DATA);
            return;
        }
        switch (f.type) {
            case Frames.CONFIG -> configure(ctx, f.reverses);
            case Frames.OPEN -> ctx.mux.onOpen(f.streamId, f.host, f.port, dialer);
            case Frames.DATA -> ctx.mux.onData(f.streamId, f.payload);
            case Frames.EOF -> ctx.mux.onEof(f.streamId);
            case Frames.WINDOW -> ctx.mux.onWindow(f.streamId, f.credit);
            case Frames.CLOSE -> ctx.mux.onClose(f.streamId);
            default -> log.warn("unexpected frame type {} on {}", f.type, session.getId());
        }
    }

    @Override
    protected void handlePongMessage(WebSocketSession session, PongMessage message) {
        Session ctx = sessions.get(session.getId());
        if (ctx != null) {
            ctx.lastPongNanos.set(System.nanoTime());
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        Session ctx = sessions.remove(session.getId());
        if (ctx != null) {
            ctx.closeListeners();
            ctx.mux.closeAll();
        }
        log.info("tunnel client disconnected: {} ({})", session.getId(), status);
    }

    /** Пингануть каждую сессию и закрыть те, которые перестали отвечать. */
    private void pingAndReap() {
        long timeoutNanos = props.getPongTimeout().toNanos();
        for (Session ctx : sessions.values()) {
            try {
                if (ctx.staleFor(timeoutNanos)) {
                    log.warn("no pong from {} for {}s, closing the session",
                            ctx.ws.getId(), props.getPongTimeout().toSeconds());
                    // Слушатели сбрасываем сразу, не дожидаясь колбэка о закрытии, — чтобы
                    // клиент, переподключившийся тут же, нашёл свои порты свободными.
                    ctx.closeListeners();
                    closeQuiet(ctx.ws, CloseStatus.SESSION_NOT_RELIABLE);
                } else {
                    ctx.ping();
                }
            } catch (Exception e) {
                log.debug("keepalive failed for {}: {}", ctx.ws.getId(), e.toString());
            }
        }
    }

    private void configure(Session ctx, List<Reverse> reverses) {
        if (!ctx.configured.compareAndSet(false, true)) {
            log.warn("ignoring a repeated CONFIG on {}", ctx.ws.getId());
            return;
        }
        for (Reverse r : reverses) {
            String bind = r.bindHost() == null || r.bindHost().isEmpty() ? "0.0.0.0" : r.bindHost();
            if (!policy.allowsReverse(bind, r.serverPort())) {
                log.error("denied reverse listener on {}:{} (not allowed)", bind, r.serverPort());
                continue;
            }
            openReverseListener(ctx, r, bind);
        }
    }

    private void openReverseListener(Session ctx, Reverse r, String bind) {
        evictStaleHolder(ctx, r.serverPort());
        try {
            ServerSocket ss = new ServerSocket();
            ss.setReuseAddress(true);
            ss.bind(new InetSocketAddress(bind, r.serverPort()));
            ctx.listeners.add(ss);
            reversePorts.put(r.serverPort(), ctx);
            Thread t = new Thread(() -> acceptLoop(ctx, ss, r), "reverse-" + r.serverPort());
            t.setDaemon(true);
            t.start();
            log.info("reverse listener on {}:{} -> client {}:{}",
                    bind, r.serverPort(), r.clientHost(), r.clientPort());
        } catch (Exception e) {
            log.error("cannot open reverse listener on port {}: {}", r.serverPort(), e.toString());
        }
    }

    /**
     * Освободить обратный порт, который всё ещё держит сессия, пропустившая раунд keepalive.
     * Это обычный случай после того, как хост клиента ушёл в сон: старая сессия мертва, но её
     * сокет ещё не отвалился, и без этого переподключающийся клиент не сможет занять порт
     * заново.
     */
    private void evictStaleHolder(Session incoming, int port) {
        Session holder = reversePorts.get(port);
        if (holder == null || holder == incoming) {
            return;
        }
        long graceNanos = props.getKeepalive().toNanos();
        if (graceNanos <= 0 || !holder.staleFor(graceNanos)) {
            return; // порт принадлежит живому клиенту; пусть bind упадёт и об этом скажет
        }
        log.warn("port {} was held by unresponsive session {}, closing it for {}",
                port, holder.ws.getId(), incoming.ws.getId());
        holder.closeListeners();
        closeQuiet(holder.ws, CloseStatus.SESSION_NOT_RELIABLE);
    }

    private void acceptLoop(Session ctx, ServerSocket ss, Reverse r) {
        while (!ss.isClosed()) {
            Socket sock;
            try {
                sock = ss.accept();
            } catch (Exception e) {
                break; // слушатель закрылся вместе с сессией
            }
            int id = ctx.mux.nextId();
            // Сначала регистрируем, чтобы CLOSE в ответ на этот OPEN не пришёл для неизвестного
            // потока, и только потом отправляем OPEN — до того, как его обгонит любой DATA.
            ctx.mux.prepareOutbound(id, sock, r.clientHost() + ":" + r.clientPort());
            ctx.send(Frames.open(id, r.clientHost(), r.clientPort()));
            ctx.mux.startOutbound(id);
        }
    }

    private static void closeQuiet(ServerSocket ss) {
        try {
            ss.close();
        } catch (Exception ignored) {
        }
    }

    private static void closeQuiet(WebSocketSession session, CloseStatus status) {
        try {
            session.close(status);
        } catch (Exception ignored) {
        }
    }
}
