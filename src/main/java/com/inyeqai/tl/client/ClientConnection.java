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
 * One attempt at a tunnel connection. Owns the WebSocket, a {@link Mux}, the local
 * listeners, a serialized send queue (the JDK WebSocket forbids overlapping sends), and
 * the keepalive schedule. {@link #run()} blocks until the connection ends.
 *
 * <p>Keepalive is not just a ping: the reply is tracked, and a tunnel whose pongs stop
 * arriving is torn down so the caller reconnects. Without that a connection killed
 * invisibly &mdash; laptop suspended, NAT entry expired, gateway restarted &mdash; looks
 * healthy forever, because nothing is written to notice the broken socket.
 */
final class ClientConnection implements WebSocket.Listener {

    private static final Logger log = LoggerFactory.getLogger(ClientConnection.class);
    private static final int DIAL_TIMEOUT_MS = 10_000;
    private static final byte[] PING = new byte[0];
    private static final byte[] STOP = new byte[0]; // distinct identity from PING
    /**
     * DATA frames allowed in the send queue at once. Per-stream windows already bound each
     * stream; this caps the total so many streams cannot add up to an unbounded queue.
     */
    private static final int MAX_INFLIGHT_DATA = 64;
    /** A single message this large means a desynchronised peer, not real traffic. */
    private static final int MAX_MESSAGE = 1024 * 1024;
    /** Pongs may be missed; two keepalive periods of silence means the link is gone. */
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
     * Connect and block until the carrier closes or errors. Throws if it cannot be
     * established at all, which the caller treats as a failed attempt.
     *
     * <p>Both transports arrive here as a {@link WebSocket} and call back into this class as
     * a {@link WebSocket.Listener}; see {@link HttpLink} for why the HTTP one is shaped that
     * way. Everything below this method is the same for either.
     */
    void run() throws Exception {
        if (transport == Transport.HTTP) {
            HttpLink.open(url, auth, this); // calls onOpen before it returns
        } else {
            HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15)).build();
            WebSocket.Builder b = http.newWebSocketBuilder().connectTimeout(Duration.ofSeconds(15));
            if (auth != null && !auth.isEmpty()) {
                b.header("X-TL-Auth", auth);
            }
            b.buildAsync(URI.create(url), this).join(); // completes exceptionally on 401 etc.
        }
        closed.await();
    }

    /** Tear down this connection and let {@link #run()} return. Used by tests. */
    void stop() {
        shutdown();
    }

    /** How long this connection stayed up, for the caller's reconnect backoff. */
    Duration uptime() {
        long since = upSinceNanos;
        return since == 0 ? Duration.ZERO : Duration.ofNanos(System.nanoTime() - since);
    }

    /** True once the WebSocket is open and until the connection is torn down. */
    boolean connected() {
        return ws != null && !down.get();
    }

    /**
     * Age of the newest keepalive reply. This is the one number that says the far end is
     * really answering: a WebSocket whose peer has vanished without a TCP close stays
     * "connected" indefinitely, and only the missing pongs give it away.
     *
     * @return empty when keepalive is off, because then there is nothing to go on
     */
    Optional<Duration> sinceLastPong() {
        if (keepaliveSec <= 0) {
            return Optional.empty();
        }
        return Optional.of(Duration.ofNanos(System.nanoTime() - lastPongNanos));
    }

    /** How long a pong may be missing before the link counts as gone. */
    Duration pongDeadline() {
        return Duration.ofSeconds((long) keepaliveSec * PONG_TIMEOUT_FACTOR);
    }

    String url() {
        return url;
    }

    /** Which carrier this attempt used. Shown by the health endpoint, which matters when
     * {@code --transport auto} picked it rather than the operator. */
    Transport transport() {
        return transport;
    }

    List<ForwardSpec> locals() {
        return locals;
    }

    List<Reverse> reverses() {
        return reverses;
    }

    /** The multiplexer's counters, or empty before the connection opened. */
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
            // Register first so a CLOSE answering this OPEN cannot arrive for an unknown
            // stream, then send OPEN before any DATA can overtake it.
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
        // DATA waits for a slot, which is how the tunnel's backpressure reaches the mux
        // pump threads. Control frames are small and must never queue behind bulk data:
        // a WINDOW frame stuck in line would stall the very stream it is meant to unblock.
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
     * @param graceful send a close frame first. Worth it when we are the ones ending the
     *                 connection, since it makes the server free this session's reverse
     *                 listeners at once; pointless when the socket is already gone.
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
        // Release anything blocked waiting for a send slot now that nothing will drain.
        dataSlots.release(MAX_INFLIGHT_DATA);
        WebSocket socket = ws;
        if (socket != null) {
            if (graceful) {
                // Bounded wait, then abort regardless: a half-dead socket never completes
                // the close, and that is exactly the case this path has to survive.
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
