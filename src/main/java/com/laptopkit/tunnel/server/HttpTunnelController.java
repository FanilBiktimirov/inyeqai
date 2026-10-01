package com.laptopkit.tunnel.server;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import jakarta.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.PongMessage;
import org.springframework.web.socket.WebSocketMessage;

import com.laptopkit.tunnel.common.Frames;
import com.laptopkit.tunnel.common.Framing;

/**
 * The fallback transport: the same tunnel over plain HTTP, for networks where the WebSocket
 * upgrade never arrives. Proxies that strip {@code Upgrade}, TLS inspection that mangles the
 * handshake and gateways that simply answer 400 all leave the WebSocket carrier unusable
 * while ordinary requests keep working.
 *
 * <pre>
 *   POST {path}/http/connect            open a session, returns its id
 *   GET  {path}/http/down/{id}?from=N   a long-lived response carrying frames server -&gt; client,
 *                                       continuing after frame N (0 = from the beginning)
 *   POST {path}/http/up/{id}?batch=N    batch N of frames, client -&gt; server; repeated
 * </pre>
 *
 * <p>The shape is asymmetric on purpose. Downstream is one streaming response, because the
 * server must be able to push at any moment. Upstream is a series of short POSTs rather than
 * one long request body: an intermediary that buffers a request body before forwarding it
 * would deadlock a streaming upload, and a body-size cap on the way in &mdash; the chain this
 * tunnel was written for has squid's {@code REQUEST_BODY_MAX} at 500 KB &mdash; would kill it
 * outright. Short bounded POSTs survive both.
 *
 * <p>Sessions outlive individual requests, so none of this is tied to a request thread;
 * every session detail is handled by {@link TunnelWebSocketHandler} through
 * {@link HttpCarrierSession}, which explains the arrangement.
 *
 * <p>Both directions can be resumed, and they have to be resumed differently, because a broken
 * response and a broken request leave different questions open:
 *
 * <ul>
 *   <li><b>Downstream</b> ends without saying how much of it arrived. So frames are numbered and
 *       kept until acknowledged, and {@code from=N} asks for everything after the last one the
 *       client holds. See {@link HttpCarrierSession#pumpTo}.
 *   <li><b>Upstream</b> fails without saying whether the server applied it. So each POST carries
 *       a batch number: the same number again is a retry to answer but not apply, and a number
 *       that skips one means frames are missing and the session cannot continue. Retrying
 *       without this would duplicate bytes inside a stream, which is corruption neither end can
 *       detect.
 * </ul>
 *
 * <p>The status codes are what the client steers by: 409 means wait and ask again, because
 * another response is still being wound up, while 404 and 410 mean the session is beyond saving
 * and the tunnel has to be built anew.
 */
@RestController
@ConditionalOnProperty(name = "tunnel.http-fallback", matchIfMissing = true)
@RequestMapping("${tunnel.path:/tunnel}/http")
class HttpTunnelController {

    private static final Logger log = LoggerFactory.getLogger(HttpTunnelController.class);

    /**
     * Cap on one upstream POST. The client batches well below this; a body bigger than this
     * means a confused or hostile sender, not real traffic.
     */
    private static final int MAX_UP_BYTES = 8 * 1024 * 1024;

    private final TunnelWebSocketHandler handler;
    private final SharedSecret secret;
    private final ConcurrentHashMap<String, HttpCarrierSession> carriers = new ConcurrentHashMap<>();

    HttpTunnelController(TunnelWebSocketHandler handler, SharedSecret secret) {
        this.handler = handler;
        this.secret = secret;
        log.info("HTTP fallback transport enabled; set tunnel.http-fallback=false to remove it");
    }

    @PostMapping("/connect")
    ResponseEntity<String> connect(
            @RequestHeader(name = "X-Tunnel-Auth", required = false) String token,
            HttpServletRequest request) {
        if (!secret.accepts(token)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("unauthorized\n");
        }
        // Prefixed so /status and the logs say which carrier a session came in on.
        String id = "http-" + UUID.randomUUID();
        HttpCarrierSession carrier = new HttpCarrierSession(
                id, URI.create(request.getRequestURL().toString()),
                new InetSocketAddress(request.getRemoteAddr(), request.getRemotePort()),
                new InetSocketAddress(request.getLocalAddr(), request.getLocalPort()),
                this::release);
        carriers.put(id, carrier);
        handler.afterConnectionEstablished(carrier);
        // A session whose client never opens the downstream is reaped by the handler's own
        // keepalive pass: without a downstream no pong can arrive, so it goes stale like any
        // other silent client.
        return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(id + "\n");
    }

    @GetMapping("/down/{id}")
    ResponseEntity<StreamingResponseBody> down(
            @PathVariable String id,
            @RequestParam(name = "from", defaultValue = "0") long from,
            @RequestHeader(name = "X-Tunnel-Auth", required = false) String token) {
        if (!secret.accepts(token)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        HttpCarrierSession carrier = carriers.get(id);
        if (carrier == null) {
            return ResponseEntity.notFound().build();
        }
        if (from < 0) {
            return ResponseEntity.badRequest().build();
        }
        // Checked before the response is committed, so an impossible request gets a status the
        // client can act on instead of a stream that dies on its first frame.
        if (!carrier.canResumeFrom(from)) {
            log.warn("cannot resume {} from frame {}, ending the session", id, from);
            carrier.close(CloseStatus.SERVER_ERROR);
            return ResponseEntity.status(HttpStatus.GONE).build();
        }
        if (!carrier.attachDown()) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }
        log.debug("downstream attached for {} from frame {}", id, from);
        StreamingResponseBody body = out -> carrier.pumpTo(out, from);
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header("Cache-Control", "no-store")
                // Asks nginx not to buffer this response. Buffering would hold frames back
                // until some buffer filled, which for a tunnel means a stall, not a delay.
                .header("X-Accel-Buffering", "no")
                .body(body);
    }

    @PostMapping("/up/{id}")
    ResponseEntity<String> up(
            @PathVariable String id,
            // Taken as optional and checked below, so that a request missing it is still
            // refused for the right reason: nothing gets past the token check, and a caller
            // without one learns nothing about what it got wrong.
            @RequestParam(name = "batch", defaultValue = "-1") long batch,
            @RequestHeader(name = "X-Tunnel-Auth", required = false) String token,
            InputStream body) throws IOException {
        if (!secret.accepts(token)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("unauthorized\n");
        }
        if (batch < 1) {
            return ResponseEntity.badRequest().body("missing or bad batch number\n");
        }
        HttpCarrierSession carrier = carriers.get(id);
        if (carrier == null) {
            // The client must not keep posting into a session that no longer exists; 404
            // tells it to drop this carrier and reconnect.
            return ResponseEntity.notFound().build();
        }
        ReentrantLock lock = carrier.inboundLock();
        lock.lock();
        try {
            if (!carrier.beginBatch(batch)) {
                // A retry of a batch already applied. Answering without applying it again is the
                // whole point: the client had no way to tell that our last answer was lost, and
                // the same frames twice would duplicate bytes inside a stream.
                log.debug("upstream batch {} on {} was already applied", batch, id);
                return ResponseEntity.noContent().build();
            }
            deliver(carrier, body);
            // Marked only now: a batch that failed half-way through cannot be retried, so the
            // session is ended below instead and the client builds a new one.
            carrier.batchApplied(batch);
        } catch (HttpCarrierSession.CannotResume e) {
            log.warn("ending {}: {}", id, e.getMessage());
            carrier.close(CloseStatus.SERVER_ERROR);
            return ResponseEntity.status(HttpStatus.GONE).body(e.getMessage() + "\n");
        } catch (IOException e) {
            log.debug("bad upstream batch on {}: {}", id, e.toString());
            carrier.close(CloseStatus.BAD_DATA);
            return ResponseEntity.badRequest().body("bad frame batch\n");
        } finally {
            lock.unlock();
        }
        return ResponseEntity.noContent().build();
    }

    /**
     * Hand one POST's worth of frames to the session, in the order they were sent. Order is
     * the whole point of the lock around this call: these frames carry byte streams.
     */
    private void deliver(HttpCarrierSession carrier, InputStream body) throws IOException {
        DataInputStream in = Framing.reader(body);
        int total = 0;
        byte[] frame;
        while ((frame = Framing.read(in)) != null) {
            total += frame.length;
            if (total > MAX_UP_BYTES) {
                throw new IOException("upstream batch over " + MAX_UP_BYTES + " bytes");
            }
            if (frame.length == 0) {
                throw new IOException("empty frame");
            }
            if (!handleCarrierFrame(carrier, frame)) {
                // Everything else is tunnel traffic and goes to the handler untouched, as
                // if the container had delivered a WebSocket message.
                dispatch(carrier, new BinaryMessage(frame));
            }
        }
    }

    /**
     * Hand one message to the session handler. Its callback may throw anything, and anything
     * it throws leaves this session half-way through a byte stream: there is no sensible way
     * to carry on, so the batch fails and the session goes with it.
     */
    private void dispatch(HttpCarrierSession carrier, WebSocketMessage<?> message) throws IOException {
        try {
            handler.handleMessage(carrier, message);
        } catch (Exception e) {
            throw new IOException("the session handler failed on " + carrier.getId(), e);
        }
    }

    /**
     * Deal with the carrier's own frames here, so neither the handler nor the mux ever sees
     * one.
     *
     * @return true when the frame was the carrier's business
     */
    private boolean handleCarrierFrame(HttpCarrierSession carrier, byte[] frame) throws IOException {
        switch (frame[0]) {
            case Frames.PING -> carrier.enqueue(Frames.pong());
            // Lets the server drop what it was holding in case this client had to come back for
            // it; see HttpCarrierSession for why "written" is not "delivered" here.
            case Frames.ACK -> carrier.onAck(Frames.decode(frame).ackThrough);
            // Reported as a pong so the handler's liveness tracking, and the "last pong"
            // line on /status, mean the same thing on both transports.
            case Frames.PONG -> dispatch(carrier, new PongMessage());
            case Frames.BYE -> {
                log.debug("client said goodbye on {}", carrier.getId());
                carrier.close(CloseStatus.NORMAL);
            }
            default -> {
                return false;
            }
        }
        return true;
    }

    /**
     * Called exactly once per session, from {@link HttpCarrierSession#close}, whichever side
     * ended it: the client's {@code BYE}, the downstream response breaking, or the handler's
     * own reaper closing a session that stopped answering.
     */
    private void release(HttpCarrierSession carrier, CloseStatus status) {
        carriers.remove(carrier.getId(), carrier);
        // Closes the mux and frees the reverse listeners, the same callback the container
        // would make for a WebSocket.
        handler.afterConnectionClosed(carrier, status);
    }
}
