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
 * The client half of the HTTP fallback transport, presented as a {@link WebSocket}.
 *
 * <p>It is the mirror image of the trick on the server. {@link ClientConnection} holds
 * everything that makes the client a client &mdash; the mux, the local listeners, the send
 * queue, the keepalive watchdog, the health endpoint's view of the world &mdash; and none of
 * that is about WebSockets. So instead of a second client for HTTP, this class implements the
 * small interface {@code ClientConnection} actually uses and feeds the same listener
 * callbacks the JDK would. The connection logic stays in one place for both transports.
 *
 * <p>Underneath: one long-lived {@code GET} carries frames down, and frames going up are
 * batched into short {@code POST}s. HTTP/1.1 is forced rather than negotiated, because the
 * whole point of this transport is to look like the most ordinary traffic possible to
 * whatever is in the middle.
 *
 * <p>The carrier's own {@code PING}, {@code PONG}, {@code BYE} and {@code ACK} frames are
 * produced and consumed here, so the mux never sees them, and the pongs they carry are reported
 * through {@link WebSocket.Listener#onPong}. That is what keeps the client's liveness check, and
 * the {@code stale} verdict on its health endpoint, meaning exactly the same thing on both
 * transports.
 *
 * <h2>Resumption</h2>
 *
 * <p>Neither half of an HTTP conversation ending tells you what the other end did with it, so
 * each direction recovers differently:
 *
 * <ul>
 *   <li>A <b>downstream</b> response that ends is replaced by a new one asking to continue after
 *       the last frame number that arrived. Frames are numbered by the server and must arrive
 *       consecutively; a gap or a repeat is treated as fatal rather than tolerated, because
 *       either one means bytes missing or doubled inside a stream, which nothing above this
 *       layer could detect.
 *   <li>An <b>upstream</b> POST that fails is retried with the same batch number, which the
 *       server uses to tell a retry from new work. Retrying without that would risk applying the
 *       same frames twice.
 * </ul>
 *
 * <p>This is what the whole transport is for. A proxy that caps how long a response may last
 * will cut the downstream on a timer, and without resumption every cut would tear down every
 * stream in the tunnel; with it the cut costs one round trip and the streams never notice.
 */
final class HttpLink implements WebSocket {

    private static final Logger log = LoggerFactory.getLogger(HttpLink.class);

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration UP_TIMEOUT = Duration.ofSeconds(30);
    /**
     * How long to keep trying to resume before giving up and rebuilding the tunnel. It is only a
     * backstop: the server answers 404 or 410 the moment the session is truly unrecoverable, and
     * that answer ends things at once. Kept well under a server's default pong timeout, past
     * which there would be no session left to resume anyway.
     */
    private static final Duration RESUME_DEADLINE = Duration.ofSeconds(30);
    /** Pause between attempts to resume, long enough not to spin, short enough not to be felt. */
    private static final long RETRY_PAUSE_MS = 200;
    /**
     * Received frames after which an acknowledgement is sent even if nothing else is going up.
     * Acknowledgements normally ride along with the WINDOW frames a download produces anyway;
     * this covers a session whose upstream happens to be silent, so the server can still let go
     * of what it is holding for a possible retransmit.
     */
    private static final int ACK_EVERY_FRAMES = 64;
    /**
     * DATA frames that may be queued unsent, the same bound the WebSocket path puts on its
     * own send queue. This is where backpressure comes from on this transport: a send is
     * reported complete as soon as the frame is queued, so the queue itself has to push back.
     * Control frames are never held up by it &mdash; a WINDOW frame waiting behind bulk data
     * would stall the very stream it exists to unblock.
     */
    private static final int MAX_INFLIGHT_DATA = 64;
    /** Bytes one POST may carry. Batching is what keeps throughput off the round-trip floor. */
    private static final int BATCH_BYTES = 256 * 1024;
    /** Queue sentinel that ends the upstream sender loop. */
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

    /** The response being read now; replaced on every resume. */
    private volatile InputStream down;
    /** Highest frame number handed to the mux. Written by the reader, read by the sender. */
    private volatile long receivedThrough;
    /** Set when the server says goodbye, which is the one ending not to resume from. */
    private volatile boolean serverSaidBye;
    /** Last value the reader queued an acknowledgement for; reader thread only. */
    private long ackQueuedThrough;
    /** Batch numbers handed to the server; sender thread only. */
    private long batchSeq;
    /** Highest value already acknowledged to the server; sender thread only. */
    private long ackSentThrough;
    private int resumes;
    /** Completed once a goodbye has actually gone out, so a caller can wait for it. */
    private volatile CompletableFuture<WebSocket> byeSent;

    private Thread reader;
    private Thread sender;

    /** A failure no retry can mend: the session is gone, or refuses what we are sending. */
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
     * Open a session and start carrying frames. Returns once the downstream response is
     * established, having already called {@link Listener#onOpen}.
     *
     * @param wsUrl the same {@code ws://} or {@code wss://} URL the WebSocket transport uses;
     *              mapped to {@code http}/{@code https} here, so one URL serves both
     * @throws IOException if the session cannot be opened, which the caller treats as a
     *                     failed connection attempt and retries with backoff
     */
    static HttpLink open(String wsUrl, String auth, Listener listener) throws Exception {
        String base = httpBase(wsUrl);
        HttpClient http = HttpClient.newBuilder()
                // Deliberately not negotiated up: this transport exists for networks that
                // mishandle anything unusual, and HTTP/1.1 is the least unusual thing there is.
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

        // No request timeout on the downstream: it is meant to stay open for the life of the
        // tunnel, and a deadline here would cut a perfectly healthy idle one.
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
        // onOpen before the reader starts: it builds the mux that incoming frames need, and a
        // frame arriving first would land on a half-built connection.
        listener.onOpen(this);
        reader = thread(this::readerLoop, "http-down");
        reader.start();
        log.info("http transport: session {} on {}", id, base);
    }

    /** {@code ws}/{@code wss} URL to the HTTP endpoints that mirror it. */
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
     * Read frames for as long as the session lasts, across however many responses that takes.
     * A response ending is not the session ending: that is what {@link #resume} is for, and only
     * when resuming fails does this report the connection as closed.
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

    /** Read one response to its end. Returns normally when it has nothing more to give. */
    private void drain(InputStream in) throws IOException {
        DataInputStream dis = Framing.reader(in);
        Framing.Sequenced next;
        while ((next = Framing.readSequenced(dis)) != null) {
            long expected = receivedThrough + 1;
            if (next.seq() != expected) {
                // Neither a gap nor a repeat may be tolerated: one means bytes are missing from
                // some stream and the other means they are doubled, and in both cases everything
                // above this layer would carry on none the wiser.
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
            // Only now: this number is a promise that the frame has been dealt with, and the
            // server drops its copy on the strength of it.
            receivedThrough = next.seq();
            if (receivedThrough - ackQueuedThrough >= ACK_EVERY_FRAMES) {
                ackQueuedThrough = receivedThrough;
                offer(Frames.ack(receivedThrough));
            }
        }
    }

    /**
     * Ask for a new downstream response continuing after the last frame we hold.
     *
     * @return true once one is attached; false when the session cannot be resumed, having
     *         already reported the connection closed so the caller rebuilds the tunnel
     */
    private boolean resume() {
        closeQuiet(down);
        long deadline = System.nanoTime() + RESUME_DEADLINE.toNanos();
        boolean firstTry = true;
        while (!closed.get() && System.nanoTime() < deadline) {
            // No pause before the first attempt. A cut response is the ordinary case here and
            // the server is usually perfectly fine, so recovery should cost one round trip
            // rather than a fixed wait; only a server that is actually unreachable gets paced.
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
                    continue; // the previous response is still being wound up; ask again
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

    /** @return false if the link went away while pausing */
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
                // Take whatever else is already waiting: one request for many frames is what
                // keeps throughput off the one-round-trip-per-frame floor.
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
                // Ride an acknowledgement along with whatever is going up anyway; it is nine
                // bytes and it is what lets the server stop holding frames for a retransmit.
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
                    // Credit is returned once the frames are really gone, so the bound means
                    // what it says.
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
     * Send a batch of frames as one request body, retrying the same batch number until the
     * server accepts it or says it never will.
     *
     * <p>The batch number is what makes retrying safe. A POST that fails does not say whether
     * the server applied it, so a plain retry could apply the same frames twice; numbered, the
     * server answers a repeat without acting on it.
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
                    // 404 and 410 in particular: the session is gone, or will not take this
                    // batch. Nothing about trying again changes that.
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

    /** Queue a frame for the next request. Control frames are never made to wait. */
    private void offer(byte[] frame) {
        if (!closed.get()) {
            up.offer(frame);
        }
    }

    /**
     * Wait for permission to queue one more DATA frame. Polls rather than blocking outright,
     * so a link torn down while a sender is parked here does not keep the thread forever.
     *
     * @return false if the link went away while waiting, in which case the frame is dropped:
     *         the carrier is gone and its streams die with it
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

    /** The carrier ended for good. Reported as a close, so the caller reconnects. */
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
        // Complete as soon as the frame is queued, rather than once it has been POSTed:
        // waiting here would let only one frame accumulate at a time and reduce every batch
        // to a single frame, turning throughput into one round trip per 16 KB.
        return CompletableFuture.completedFuture(this);
    }

    @Override
    public CompletableFuture<WebSocket> sendPing(ByteBuffer message) {
        offer(Frames.ping());
        return CompletableFuture.completedFuture(this);
    }

    @Override
    public CompletableFuture<WebSocket> sendClose(int statusCode, String reason) {
        // Queued like anything else, so batch numbers stay in one thread's hands, and the
        // returned future says when it actually left. The caller gives it a moment before
        // aborting; a goodbye is worth that much because it frees the server's reverse
        // listeners at once, but no more than that, since the link is going away regardless.
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
        // Let go of anything parked waiting to queue a DATA frame; nothing will drain now.
        dataSlots.release(MAX_INFLIGHT_DATA);
        // Closing the downstream is what unblocks the reader, which is parked on a read.
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
            // nothing left to do about it; the link is gone either way
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
        // Frames are read as fast as they arrive; there is no demand to signal.
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
        // Pongs are answers to PING frames and are queued by the reader itself; nothing in
        // the client asks for one directly.
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
