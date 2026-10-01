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
 * Multiplexes many TCP streams over one WebSocket. Shared by both ends: each side keeps a
 * {@code streamId -> Stream} map, pumps socket bytes out as DATA frames, and writes
 * incoming DATA frames back to the matching socket.
 *
 * <p>Threading is the point of this class. The frame-dispatch path ({@code onOpen},
 * {@code onData}, {@code onEof}, {@code onClose}, {@code onWindow}) never blocks on socket
 * I/O: it only hands work to per-stream threads. Dialing a destination and writing to a
 * socket both happen off that path, so one unreachable host or one stalled reader cannot
 * stall every other stream sharing the tunnel.
 *
 * <p>Each direction of each stream has a {@link #WINDOW_BYTES} credit window. A sender
 * blocks once that many bytes are unacknowledged, and the receiver returns credit as it
 * drains bytes into its socket. Without this a fast local reader feeding a slow tunnel
 * would buffer without bound.
 *
 * <p>The {@code sender} sink must be safe to call from many threads and must not throw; it
 * may block, which is how backpressure reaches the pump threads. Both the Spring server
 * and the JDK client wrap their send path accordingly.
 */
public final class Mux {

    /** Opens a TCP connection to a destination; supplied per side. */
    public interface Dialer {
        Socket dial(String host, int port) throws IOException;
    }

    /** Decides whether the peer may have us dial {@code host:port}. */
    public interface Policy {
        boolean allows(String host, int port);
    }

    private static final Logger log = LoggerFactory.getLogger(Mux.class);

    /** Unacknowledged bytes a sender may have in flight per stream direction. */
    public static final int WINDOW_BYTES = 256 * 1024;

    private static final int CHUNK = 16 * 1024;
    /**
     * Hard cap on bytes queued for one stream. A peer honouring the window never reaches
     * it; one ignoring the window is a protocol violation, and we drop its stream rather
     * than buffer whatever it sends.
     */
    private static final int QUEUE_LIMIT = 2 * WINDOW_BYTES;
    /** Queue sentinel for "peer is done writing", kept in order behind pending DATA. */
    private static final Object EOF_MARK = new Object();
    /** How long a blocked sender waits before rechecking that its stream is still alive. */
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

    /** One multiplexed TCP connection. */
    private static final class Stream {
        final int id;
        /** Where this stream goes, as {@code host:port}; for logs and status only. */
        final String dst;
        final long openedAtNanos = System.nanoTime();
        final BlockingQueue<Object> inbound = new LinkedBlockingQueue<>();
        final AtomicInteger queued = new AtomicInteger();
        /** Credit for bytes *we* may still send. */
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

    /** Fresh stream id for a connection this side accepted. Top bit tags the originator. */
    public int nextId() {
        int id = counter.getAndIncrement() & 0x7fffffff;
        return serverSide ? (id | 0x80000000) : id;
    }

    /**
     * Register a socket we accepted on a listener, before its OPEN frame goes out. Pair it
     * with {@link #startOutbound(int)}:
     *
     * <pre>
     *   mux.prepareOutbound(id, socket);
     *   send(Frames.open(id, host, port));
     *   mux.startOutbound(id);
     * </pre>
     *
     * <p>Registering first is what makes the gap around the OPEN safe. The peer may answer
     * with CLOSE (it refused, or could not dial) before we have started relaying; with the
     * stream already in the map that CLOSE lands on it and the socket is closed, instead of
     * arriving for an unknown id and leaving the caller's connection hanging open forever.
     * Starting the threads only afterwards keeps DATA from overtaking its own OPEN.
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

    /** Begin relaying a stream registered by {@link #prepareOutbound}. */
    public void startOutbound(int streamId) {
        Stream s = streams.get(streamId);
        if (s == null || s.dead.get() || shuttingDown) {
            return; // the peer already refused it, or we are going away
        }
        streamsOpened.incrementAndGet();
        log.debug("stream {} open, peer dials {}", label(streamId), s.dst);
        start(s);
    }

    /**
     * Peer asked us to dial host:port for this stream. Registers the stream immediately so
     * DATA arriving while the dial is still in flight is queued rather than dropped, then
     * dials off the dispatch thread.
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

    /** Bytes arrived for a stream; queue them for its writer thread. */
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

    /** Peer is done writing this stream; half-close our side once pending bytes are out. */
    public void onEof(int streamId) {
        Stream s = streams.get(streamId);
        if (s != null && !s.dead.get()) {
            s.inbound.add(EOF_MARK);
        }
    }

    /** Peer granted us credit to send more on this stream. */
    public void onWindow(int streamId, int credit) {
        Stream s = streams.get(streamId);
        if (s != null) {
            s.window.release(credit);
        }
    }

    /** Peer tore a stream down; drop our side without echoing a CLOSE back. */
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

    /** Live streams right now. */
    public int openStreams() {
        return streams.size();
    }

    /** Streams this mux has carried since it was created. */
    public long streamsOpened() {
        return streamsOpened.get();
    }

    /** Bytes read from local sockets and sent into the tunnel. */
    public long bytesToPeer() {
        return bytesToPeer.get();
    }

    /** Bytes received from the tunnel and written to local sockets. */
    public long bytesFromPeer() {
        return bytesFromPeer.get();
    }

    /** Every live stream, for the status view. */
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

    /** Tell the peer this stream never came up, and forget it. */
    private void reject(Stream s) {
        streams.remove(s.id, s);
        s.dead.set(true);
        s.inbound.clear();
        sender.accept(Frames.close(s.id));
    }

    /** Frame-dispatch -> socket: drain the queue into the socket, returning credit. */
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
                // Credit is returned only once the bytes have actually left our hands, so
                // the window really tracks what the destination has absorbed.
                sender.accept(Frames.window(s.id, chunk.length));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException | RuntimeException e) {
            fail(s);
        }
    }

    /** Socket -> frame-sender: relay bytes out, respecting the peer's window. */
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
                    return; // stream died or we are shutting down
                }
                sender.accept(Frames.data(s.id, buf, 0, n));
                s.sent.addAndGet(n);
                bytesToPeer.addAndGet(n);
            }
            // Clean EOF: announce it and let the other direction keep running.
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
     * Wait for {@code n} bytes of credit. Polls so that a stream killed while its sender
     * is blocked here does not linger: closing the socket unblocks a read, but nothing
     * would otherwise unblock a wait for credit.
     */
    private boolean acquireWindow(Stream s, int n) throws InterruptedException {
        while (!s.dead.get() && !shuttingDown) {
            if (s.window.tryAcquire(n, WINDOW_POLL_MS, TimeUnit.MILLISECONDS)) {
                return true;
            }
        }
        return false;
    }

    /** Both directions saw a clean EOF: the stream is finished, no CLOSE needed. */
    private void finishIfDone(Stream s) {
        if (s.sentEof.get() && s.recvEof.get() && streams.remove(s.id, s)) {
            s.dead.set(true);
            closeQuiet(s.socket);
            logClosed(s, "closed cleanly");
        }
    }

    /** Something broke on our side: tell the peer and tear the stream down. */
    private void fail(Stream s) {
        if (streams.remove(s.id, s)) {
            kill(s);
            sender.accept(Frames.close(s.id));
            logClosed(s, "failed");
        }
    }

    /**
     * One line per finished stream, at DEBUG: where it went, how long it lived and how much
     * it carried each way. This is the view the logs otherwise lack entirely &mdash; without
     * it a working tunnel and a tunnel quietly dropping streams look identical.
     */
    private void logClosed(Stream s, String how) {
        if (s.logged.compareAndSet(false, true) && log.isDebugEnabled()) {
            log.debug("stream {} to {} {} after {} ms, sent {}, received {}",
                    label(s.id), s.dst, how, s.ageMillis(), bytes(s.sent.get()), bytes(s.received.get()));
        }
    }

    /** Stream ids read unsigned; see {@link StreamInfo#label()}. */
    private static String label(int streamId) {
        return Integer.toUnsignedString(streamId);
    }

    /**
     * Byte counts a human can read at a glance. Formatted with {@link Locale#ROOT} on
     * purpose: the default locale would put a comma in the decimals on a Russian machine and
     * a dot in a container, so the same traffic would read differently depending on where
     * the process happens to run.
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
            // the peer may already be gone; the read side decides the stream's fate
        }
    }
}
