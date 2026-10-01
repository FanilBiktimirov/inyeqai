package com.laptopkit.tunnel.client;

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

import com.laptopkit.tunnel.common.Frames;
import com.laptopkit.tunnel.common.Framing;

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
 * <p>The carrier's own {@code PING}/{@code PONG}/{@code BYE} frames are produced and consumed
 * here, so the mux never sees them, and the pongs they carry are reported through
 * {@link WebSocket.Listener#onPong}. That is what keeps the client's liveness check, and the
 * {@code stale} verdict on its health endpoint, meaning exactly the same thing on both
 * transports.
 */
final class HttpLink implements WebSocket {

    private static final Logger log = LoggerFactory.getLogger(HttpLink.class);

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration UP_TIMEOUT = Duration.ofSeconds(30);
    /** A goodbye is worth a moment, not a wait: the link is going away either way. */
    private static final Duration BYE_TIMEOUT = Duration.ofSeconds(2);
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
    private final InputStream down;

    private final BlockingQueue<byte[]> up = new LinkedBlockingQueue<>();
    private final Semaphore dataSlots = new Semaphore(MAX_INFLIGHT_DATA);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean ended = new AtomicBoolean();

    private Thread reader;
    private Thread sender;

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
                request(base + "/down/" + id, auth).GET().build(),
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
            b.header("X-Tunnel-Auth", auth);
        }
        return b;
    }

    private void readerLoop() {
        try {
            DataInputStream in = Framing.reader(down);
            byte[] frame;
            while ((frame = Framing.read(in)) != null) {
                if (frame.length == 0) {
                    throw new IOException("empty frame");
                }
                switch (frame[0]) {
                    case Frames.PING -> offer(Frames.pong());
                    case Frames.PONG -> listener.onPong(this, ByteBuffer.allocate(0));
                    case Frames.BYE -> {
                        end("the server closed the tunnel");
                        return;
                    }
                    default -> listener.onBinary(this, ByteBuffer.wrap(frame), true);
                }
            }
            end("the server closed the downstream");
        } catch (Exception e) {
            if (!closed.get()) {
                fail(e);
            }
        }
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
                try {
                    post(batch, UP_TIMEOUT);
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

    /** Send a batch of frames as one request body. */
    private void post(List<byte[]> frames, Duration timeout) throws IOException, InterruptedException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        for (byte[] frame : frames) {
            Framing.write(body, frame);
        }
        HttpResponse<Void> res = http.send(
                request(base + "/up/" + id, auth).timeout(timeout)
                        .header("Content-Type", "application/octet-stream")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body.toByteArray())).build(),
                HttpResponse.BodyHandlers.discarding());
        int code = res.statusCode();
        if (code < 200 || code >= 300) {
            // 404 in particular means the server has already forgotten this session, so
            // there is nothing to retry: the tunnel has to be rebuilt.
            throw new IOException("upstream POST answered " + code);
        }
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

    /** The carrier ended cleanly. Reported as a close, so the caller reconnects. */
    private void end(String reason) {
        if (ended.compareAndSet(false, true)) {
            log.debug("http transport: {}", reason);
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
        // Sent inline, ahead of the queue: it frees the server's reverse listeners at once,
        // which is the only reason to bother saying goodbye at all.
        try {
            post(List.of(Frames.bye()), BYE_TIMEOUT);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.debug("http transport: goodbye not delivered ({})", e.toString());
        }
        return CompletableFuture.completedFuture(this);
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
        throw new UnsupportedOperationException("the tunnel protocol is binary only");
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
