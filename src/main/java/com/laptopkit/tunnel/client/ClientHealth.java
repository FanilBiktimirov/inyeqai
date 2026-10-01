package com.laptopkit.tunnel.client;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.laptopkit.tunnel.common.ForwardSpec;
import com.laptopkit.tunnel.common.Mux;
import com.laptopkit.tunnel.common.Reverse;
import com.laptopkit.tunnel.common.StreamInfo;

/**
 * Health-эндпойнт клиента — на HTTP-сервере из самой JDK, чтобы клиент сохранил свою форму
 * «никаких зависимостей кроме JDK».
 *
 * <p>Он намеренно не отвечает на вопрос «процесс жив» — это всё, что может сказать обычная
 * liveness-проба, и здесь это не стоит ничего: клиент туннеля, чей пир исчез без TCP-close,
 * держит свой сокет, держит открытыми слушатели и продолжает принимать соединения, которые уже
 * не может довезти. Важно другое: отвечает ли ещё дальняя сторона, — и честное доказательство
 * этому — давность последнего ответа на keepalive.
 *
 * <pre>
 *   GET /healthz   200, когда туннель пригоден, 503, когда нет
 *   GET /status    тот же вердикт вместе с числами за ним, обычным текстом
 * </pre>
 *
 * <p>С {@code --keepalive 0s} никаких pong нет, а значит, нет и такого доказательства. Тогда
 * эндпойнт сообщает {@code unknown} и всё равно отвечает 200: не называть связь здоровой —
 * правильно, но так же правильно и не называть её сломанной на основании проверки, которую сам
 * оператор и выключил. Причина печатается всегда.
 */
final class ClientHealth implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ClientHealth.class);

    /** К чему эндпойнт пришёл и почему. */
    enum Verdict {
        /** Соединение есть, и пир отвечает на keepalive. */
        UP(200),
        /** Соединение есть, но keepalive выключен — присутствие пира ничем не подтверждено. */
        UNKNOWN(200),
        /** Соединение есть, но пир перестал отвечать: связь вот-вот разорвут. */
        STALE(503),
        /** Соединения нет вообще; клиент между попытками переподключения. */
        DOWN(503);

        final int code;

        Verdict(int code) {
            this.code = code;
        }
    }

    private final HttpServer http;
    private final Supplier<ClientConnection> current;

    /**
     * @param current живое соединение или null, пока идёт переподключение; читается на каждый
     *                запрос, чтобы эндпойнт следовал за клиентом через переподключения
     */
    ClientHealth(String bindHost, int port, Supplier<ClientConnection> current) throws IOException {
        this.current = current;
        this.http = HttpServer.create(new InetSocketAddress(bindHost, port), 0);
        http.createContext("/healthz", this::healthz);
        http.createContext("/status", this::status);
        http.setExecutor(Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "health");
            t.setDaemon(true);
            return t;
        }));
        http.start();
        log.info("health endpoint on {}:{} (/healthz, /status)", bindHost, http.getAddress().getPort());
    }

    int port() {
        return http.getAddress().getPort();
    }

    @Override
    public void close() {
        http.stop(0);
    }

    private void healthz(HttpExchange ex) throws IOException {
        Verdict v = verdict(current.get());
        respond(ex, v.code, v.name().toLowerCase(Locale.ROOT) + "\n");
    }

    private void status(HttpExchange ex) throws IOException {
        ClientConnection c = current.get();
        respond(ex, verdict(c).code, render(c));
    }

    /**
     * Весь вердикт в одном месте. Порядок важен: пропавшее соединение — это DOWN, что бы ни
     * говорили счётчики, а протухший pong важнее того, что соединение формально есть.
     */
    private static Verdict verdict(ClientConnection c) {
        if (c == null || !c.connected()) {
            return Verdict.DOWN;
        }
        Optional<Duration> pong = c.sinceLastPong();
        if (pong.isEmpty()) {
            return Verdict.UNKNOWN;
        }
        return pong.get().compareTo(c.pongDeadline()) > 0 ? Verdict.STALE : Verdict.UP;
    }

    private String render(ClientConnection c) {
        StringBuilder sb = new StringBuilder();
        Verdict v = verdict(c);
        sb.append("tunnel client: ").append(v.name().toLowerCase(Locale.ROOT)).append('\n');

        if (c == null || !c.connected()) {
            sb.append("reason: no connection to the server, reconnecting\n");
            return sb.toString();
        }
        sb.append("server: ").append(c.url()).append('\n');
        // Какой транспорт в деле: с --transport auto его выбрал клиент, а не оператор, и «http»
        // здесь — наглядный признак того, что WebSocket не выжил.
        sb.append("transport: ").append(c.transport().label()).append('\n');
        sb.append("connected for ").append(duration(c.uptime())).append('\n');

        Optional<Duration> pong = c.sinceLastPong();
        if (pong.isEmpty()) {
            sb.append("last pong: keepalive is off, nothing proves the server is answering\n");
        } else {
            sb.append("last pong: ").append(duration(pong.get()))
                    .append(" ago (dropped after ").append(duration(c.pongDeadline())).append(")\n");
            if (v == Verdict.STALE) {
                sb.append("reason: the server stopped answering keepalives\n");
            }
        }

        for (ForwardSpec f : c.locals()) {
            sb.append("forward ").append(f.bindHost()).append(':').append(f.bindPort())
                    .append(" -> server dials ").append(f.dstHost()).append(':').append(f.dstPort()).append('\n');
        }
        for (Reverse r : c.reverses()) {
            sb.append("reverse server:").append(r.serverPort())
                    .append(" -> we dial ").append(r.clientHost()).append(':').append(r.clientPort()).append('\n');
        }

        c.mux().ifPresent(m -> {
            sb.append("streams open ").append(m.openStreams())
                    .append(", carried ").append(m.streamsOpened())
                    .append(", sent ").append(Mux.bytes(m.bytesToPeer()))
                    .append(", received ").append(Mux.bytes(m.bytesFromPeer())).append('\n');
            List<StreamInfo> live = m.streams();
            for (StreamInfo s : live) {
                sb.append(String.format(Locale.ROOT,
                        "  %-10s %-28s open %8s  sent %-10s received %s%n",
                        s.label(), s.dst(), duration(Duration.ofMillis(s.ageMillis())),
                        Mux.bytes(s.sent()), Mux.bytes(s.received())));
            }
        });
        return sb.toString();
    }

    private static void respond(HttpExchange ex, int code, String body) throws IOException {
        byte[] out = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
        // Никакого кеширования: закешированный ответ health хуже, чем никакого.
        ex.getResponseHeaders().add("Cache-Control", "no-store");
        ex.sendResponseHeaders(code, out.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(out);
        }
    }

    /** Компактные человекочитаемые длительности — как в status на сервере. */
    static String duration(Duration d) {
        long millis = d.toMillis();
        if (millis < 1000) {
            return millis + "ms";
        }
        long seconds = millis / 1000;
        if (seconds < 60) {
            return String.format(Locale.ROOT, "%.1fs", millis / 1000.0);
        }
        if (seconds < 3600) {
            return String.format(Locale.ROOT, "%dm%02ds", seconds / 60, seconds % 60);
        }
        return String.format(Locale.ROOT, "%dh%02dm", seconds / 3600, (seconds % 3600) / 60);
    }
}
