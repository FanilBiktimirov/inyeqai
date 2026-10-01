package com.laptopkit.tunnel.server;

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
 * <p>What the two carriers differ in is framing, liveness and lifetime, and those differences
 * stop here:
 * <ul>
 *   <li>Outgoing frames are queued and written to whichever downstream response is attached,
 *       each one length-prefixed ({@link Framing}) because an HTTP body has no message
 *       boundaries.
 *   <li>A {@link PingMessage} from the handler's reaper becomes a {@code PING} frame, and
 *       the {@code PONG} frame that answers it is reported back to the handler as a
 *       {@code PongMessage}. The handler's one measure of a live session &mdash; the age of
 *       the last pong &mdash; therefore means exactly the same thing on both transports.
 *   <li>A session <b>survives its downstream response</b>. The response is a thing proxies cut
 *       for reasons of their own; the session is not. See {@link #pumpTo}.
 * </ul>
 *
 * <h2>Resumption</h2>
 *
 * <p>Every downstream frame gets a sequence number and is kept until the client confirms it
 * with an {@code ACK}. When a response dies, the client comes back asking to continue after
 * the last sequence it holds, and everything after that is written again on the new response.
 *
 * <p>The buffer is what makes this safe rather than merely plausible. A frame might have been
 * written into a response that was already dead, and nothing about the write says so, so
 * "sent" cannot mean "delivered": only an {@code ACK} can. Without the buffer a resumed
 * session would quietly skip those frames, which for a byte stream is corruption that neither
 * end can detect. It is bounded ({@link #RESUME_BUFFER_BYTES}) and, when full, the writer
 * waits rather than growing: no client gets to make the server hold data for it without limit.
 */
final class HttpCarrierSession implements WebSocketSession {

    private static final Logger log = LoggerFactory.getLogger(HttpCarrierSession.class);

    /**
     * Frames that may wait for a downstream writer. The mux's own per-stream windows bound each
     * stream; this bounds the total, so a client that has stopped reading cannot make the server
     * buffer without limit. It is also the budget a session has to survive a break on: whatever
     * piles up here while no response is attached.
     */
    private static final int QUEUE_FRAMES = 256;
    /** How long a sender waits for queue space before the session is written off. */
    private static final long OFFER_TIMEOUT_MS = 30_000;
    /**
     * Bytes written but not yet acknowledged that one session may hold for a retransmit. Beyond
     * this the writer waits for acknowledgements. A client that is reading at all acknowledges
     * within a round trip, so in ordinary use this is never reached; it is reached when a
     * client reads nothing, which is exactly when the server should stop producing for it.
     */
    private static final int RESUME_BUFFER_BYTES = 2 * 1024 * 1024;
    /**
     * How long the writer waits for an acknowledgement before giving up on the session. A peer
     * taking frames and never confirming them is broken in a way waiting cannot fix.
     */
    private static final long ACK_TIMEOUT_MS = 60_000;
    /** How long a new downstream waits for the previous one to let go before answering 409. */
    private static final long ATTACH_WAIT_MS = 5_000;
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
    /** One downstream response at a time; a new one has to wait for the old to let go. */
    private final Semaphore downSlot = new Semaphore(1);
    private volatile Thread pumpThread;
    private final AtomicLong attachments = new AtomicLong();

    /** Guards the retransmit buffer and the sequence counters. */
    private final ReentrantLock resume = new ReentrantLock();
    private final Condition acknowledged = resume.newCondition();
    private final Deque<Pending> unacked = new ArrayDeque<>();
    private long nextSeq = 1;
    private long ackedThrough;
    private int unackedBytes;

    /**
     * Serializes the upstream side and guards {@link #lastBatch}. Each POST carries a batch of
     * frames and the client sends one at a time, but nothing in HTTP guarantees that: two
     * requests racing would hand the mux frames out of order, and for a byte stream order is
     * everything.
     */
    private final ReentrantLock inbound = new ReentrantLock();
    private long lastBatch;

    private volatile int textLimit = 64 * 1024;
    private volatile int binaryLimit = Framing.MAX_FRAME;

    /** A downstream frame that has been written but not yet confirmed. */
    private record Pending(long seq, byte[] frame) {
    }

    /** Told once when a session ends, so its owner can forget it and notify the handler. */
    interface Closer {
        void closed(HttpCarrierSession session, CloseStatus status);
    }

    /** Thrown when a client asks to continue from a point this session cannot produce. */
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
     * Claim the downstream slot, displacing a writer that has not yet noticed its response is
     * dead. Displacing is the point: in the usual case the client knows the response broke long
     * before the server's write fails, and without this its first attempt to come back would be
     * refused by a corpse. Nothing is lost by displacing, because an unacknowledged frame is
     * retransmitted on the new response whether the old writer managed to send it or not.
     *
     * @return false if the previous writer would not let go in time; the caller answers 409 and
     *         the client tries again shortly
     */
    boolean attachDown() {
        Thread holder = pumpThread;
        if (holder != null) {
            holder.interrupt(); // wakes it out of waiting for a frame
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

    /** Whether frames from {@code fromSeq + 1} onwards can still be produced. */
    boolean canResumeFrom(long fromSeq) {
        resume.lock();
        try {
            return fromSeq >= ackedThrough && fromSeq <= nextSeq - 1;
        } finally {
            resume.unlock();
        }
    }

    /** How many times this session has had to be resumed, for {@code /status}. */
    long resumes() {
        return Math.max(0, attachments.get() - 1);
    }

    /** Unconfirmed bytes held for a possible retransmit, for {@code /status}. */
    int unackedBytes() {
        resume.lock();
        try {
            return unackedBytes;
        } finally {
            resume.unlock();
        }
    }

    /** The upstream lock; held while a batch of frames is handed to the handler. */
    ReentrantLock inboundLock() {
        return inbound;
    }

    /**
     * Decide what to do with an upstream batch. Must be called with {@link #inboundLock} held.
     *
     * @return true to apply it, false if it is a retry of a batch already applied &mdash; the
     *         client could not tell whether our answer was lost on the way back, so it asked
     *         again, and applying the same frames twice would duplicate bytes in a stream
     * @throws CannotResume if a batch was skipped: frames are missing and no later state can
     *                      be reconstructed from what arrived
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

    /** Record a batch as applied. Must be called with {@link #inboundLock} held. */
    void batchApplied(long batch) {
        lastBatch = batch;
    }

    /** The client confirming it holds every downstream frame through {@code through}. */
    void onAck(long through) {
        resume.lock();
        try {
            if (through >= nextSeq) {
                // It claims frames we never wrote. Something is badly confused; carrying on
                // would mean trusting its next claim too.
                log.warn("client {} acknowledged frame {} but only {} were sent, closing",
                        id, through, nextSeq - 1);
                // Safe to close while holding this: the lock is reentrant and closing takes
                // nothing that waits on a thread which might want it.
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
     * Write frames to a downstream response until it ends. Blocks the calling thread for as
     * long as that response lives, which is not the same as the life of the session.
     *
     * <p>Starts by replaying everything the client has not confirmed, then carries on with new
     * frames. A {@code PING} is queued first so the response headers go out immediately,
     * telling the client the carrier is usable rather than leaving it to guess from silence.
     * Each batch is flushed, because a tunnel that waits for a buffer to fill would add latency
     * to every request and could stall an exchange where one side is waiting to hear back.
     *
     * <p>When this returns the session is left alive and the slot released. That is the whole
     * difference resumption makes: a response ending is a thing to recover from, and only the
     * keepalive reaper &mdash; which asks whether the <em>client</em> is still answering, not
     * whether a particular response is still open &mdash; decides that a session is over.
     */
    void pumpTo(OutputStream out, long fromSeq) {
        // A pooled thread may carry an interrupt meant for the writer that used it last; the
        // flag would otherwise end this response before it sent anything.
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
            // Harmless if it is dropped: a greeting is not tunnel traffic.
            outbound.offer(Frames.ping());
            for (Pending p : replay) {
                Framing.write(out, p.seq(), p.frame());
            }
            out.flush();

            while (true) {
                batch.clear();
                batch.add(outbound.take());
                outbound.drainTo(batch); // whatever else is already waiting, in one flush

                int upToPoison = batch.size();
                for (int i = 0; i < batch.size(); i++) {
                    if (batch.get(i) == POISON) {
                        upToPoison = i;
                        break;
                    }
                }
                // Record the whole batch before writing any of it. A frame taken out of the
                // queue is only safe once it is in the retransmit buffer: write first and a
                // failure part-way through the batch would drop the frames still in hand, and
                // because they were never numbered the gap would be invisible to both ends.
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
            // A newer downstream is taking over, or the session is going away.
            log.debug("downstream of {} handed over", id);
        } catch (CannotResume e) {
            log.warn("cannot resume {}: {}", id, e.getMessage());
            close(CloseStatus.SERVER_ERROR);
        } catch (IOException e) {
            // The response is gone. The session is not: the client is expected to come back
            // and ask to continue, and until it does, or stops answering keepalives, its
            // streams and reverse listeners stay exactly as they are.
            log.debug("downstream of {} ended: {}", id, e.toString());
        } finally {
            pumpThread = null;
            // Do not leave the flag set on a pooled thread, where the next task would inherit it.
            Thread.interrupted();
            downSlot.release();
        }
    }

    /**
     * Confirm everything through {@code fromSeq} and return what still has to be written again.
     * The client asking to continue from a point is itself proof that it holds everything up to
     * it, so this doubles as an acknowledgement.
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
     * Take the next sequence number for a frame and keep the frame for a possible retransmit.
     * Waits when the buffer is full, which is the backpressure a client that reads nothing
     * deserves; the frame is recorded before it is written, so a frame lost in a failed write
     * is still a frame we can send again.
     */
    private Pending reserve(byte[] frame) throws IOException, InterruptedException {
        resume.lock();
        try {
            long waitNanos = TimeUnit.MILLISECONDS.toNanos(ACK_TIMEOUT_MS);
            // An empty buffer always takes one more frame, so a frame larger than the whole
            // budget can never wedge the session.
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
        resume.lock();
        try {
            unacked.clear();
            unackedBytes = 0;
            acknowledged.signalAll(); // let go of anything waiting to be acknowledged
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
