package com.laptopkit.tunnel.server;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

import com.laptopkit.tunnel.common.Frames;
import com.laptopkit.tunnel.common.Framing;

/**
 * An HTTP carrier dressed as a {@link WebSocketSession}.
 *
 * <p>This is the whole trick behind the fallback transport. Everything that makes a tunnel
 * session a session &mdash; the mux, the reverse listeners, the allow list, the keepalive
 * reaper that frees ports held by a vanished client, the {@code /status} view &mdash; lives in
 * {@link TunnelWebSocketHandler} and is about sessions, not about WebSockets. Rather than
 * grow a second copy of it for HTTP, the HTTP transport presents itself to that handler as
 * an ordinary session: {@link HttpTunnelController} calls the same
 * {@code afterConnectionEstablished} / {@code handleMessage} / {@code afterConnectionClosed}
 * callbacks the container would. The handler is untouched and cannot drift out of step with
 * a second implementation, because there is no second implementation.
 *
 * <p>What the two carriers do differ in is framing and liveness, and that difference stops
 * here:
 * <ul>
 *   <li>Outgoing frames are queued and written to the open downstream response, each one
 *       length-prefixed ({@link Framing}) because an HTTP body has no message boundaries.
 *   <li>A {@link PingMessage} from the handler's reaper becomes a {@code PING} frame, and
 *       the {@code PONG} frame that answers it is reported back to the handler as a
 *       {@code PongMessage}. The handler's one measure of a live session &mdash; the age of
 *       the last pong &mdash; therefore means exactly the same thing on both transports.
 * </ul>
 *
 * <p>The session deliberately outlives any single HTTP request: upstream frames arrive as a
 * series of short POSTs, so the session cannot be tied to one of them. It is tied to the
 * downstream response instead, which is the carrier's real lifeline.
 */
final class HttpCarrierSession implements WebSocketSession {

    private static final Logger log = LoggerFactory.getLogger(HttpCarrierSession.class);

    /**
     * Frames that may wait for the downstream writer. The mux's own per-stream windows
     * bound each stream; this bounds the total, so a client that has stopped reading cannot
     * make the server buffer without limit.
     */
    private static final int QUEUE_FRAMES = 256;
    /** How long a sender waits for queue space before the session is written off. */
    private static final long OFFER_TIMEOUT_MS = 30_000;
    /** Queue sentinel that ends the downstream writer loop. */
    private static final byte[] POISON = new byte[0];

    private final String id;
    private final URI uri;
    private final InetSocketAddress remote;
    private final InetSocketAddress local;
    private final Closer onClose;

    private final BlockingQueue<byte[]> outbound = new ArrayBlockingQueue<>(QUEUE_FRAMES);
    private final Map<String, Object> attributes = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean downAttached = new AtomicBoolean();
    /**
     * Serializes the upstream side. Each POST carries a batch of frames and the client
     * sends one POST at a time, but nothing in HTTP guarantees that: two requests racing
     * would hand the mux frames out of order, and for a byte stream order is everything.
     */
    private final ReentrantLock inbound = new ReentrantLock();

    private volatile int textLimit = 64 * 1024;
    private volatile int binaryLimit = Framing.MAX_FRAME;

    /** Told once when a session ends, so its owner can forget it and notify the handler. */
    interface Closer {
        void closed(HttpCarrierSession session, CloseStatus status);
    }

    HttpCarrierSession(String id, URI uri, InetSocketAddress remote, InetSocketAddress local,
                       Closer onClose) {
        this.id = id;
        this.uri = uri;
        this.remote = remote;
        this.local = local;
        this.onClose = onClose;
    }

    /** Claim the one downstream slot. False means a response is already streaming. */
    boolean attachDown() {
        return downAttached.compareAndSet(false, true);
    }

    /** The upstream lock; held while a batch of frames is handed to the handler. */
    ReentrantLock inboundLock() {
        return inbound;
    }

    /** Queue a frame for the downstream response, as {@link #sendMessage} does. */
    void enqueue(byte[] frame) throws IOException {
        if (closed.get()) {
            return;
        }
        try {
            if (!outbound.offer(frame, OFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                // End the session here rather than only reporting it. The caller logs a failed
                // send and carries on, which is right for a WebSocket, where a send that fails
                // means a socket the container will close anyway. Nothing closes this carrier
                // but us, and a frame quietly dropped from a byte stream is corruption the
                // peer has no way to detect.
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
     * Write queued frames to the downstream response until the session ends. Blocks the
     * calling thread for the life of the session.
     *
     * <p>Starts with a {@code PING}: it pushes the response headers out immediately, so the
     * client knows the carrier is usable rather than waiting for the first real traffic, and
     * it starts the keepalive exchange from a known point. Each batch is flushed, because a
     * tunnel that waits for a buffer to fill would add latency to every request and could
     * stall an exchange where one side is waiting to hear back before it writes again.
     */
    void pumpTo(OutputStream out) {
        List<byte[]> batch = new ArrayList<>();
        try {
            Framing.write(out, Frames.ping());
            out.flush();
            while (true) {
                batch.clear();
                batch.add(outbound.take());
                outbound.drainTo(batch); // whatever else is already waiting, in one flush
                boolean ending = false;
                for (byte[] frame : batch) {
                    if (frame == POISON) {
                        ending = true;
                        break;
                    }
                    Framing.write(out, frame);
                }
                out.flush();
                if (ending) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            log.debug("downstream of {} ended: {}", id, e.toString());
        } finally {
            // The downstream response is the carrier: when it ends, the session is over.
            close(CloseStatus.SESSION_NOT_RELIABLE);
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
            // The handler's keepalive, translated into something an HTTP body can carry.
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
        // Clear first: a full queue would have no room for the sentinel, and nothing left
        // in it will ever be written now anyway.
        outbound.clear();
        outbound.offer(POISON);
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
