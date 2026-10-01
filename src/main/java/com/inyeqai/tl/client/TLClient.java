package com.inyeqai.tl.client;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Точка входа клиента: вечный цикл — подключиться, обслуживать, пока связь не оборвётся,
 * выждать нарастающую задержку, переподключиться.
 *
 * <p>Куда подключаться и что пробрасывать приходит из {@link ClientSetup}, собранного из полей
 * {@code TLConfig}. Командную строку здесь никто не читает.
 */
public final class TLClient {

    private static final Logger log = LoggerFactory.getLogger(TLClient.class);

    private static final long MIN_BACKOFF_MS = 1_000;
    private static final long MAX_BACKOFF_MS = 5 * 60_000;
    /**
     * Соединение, продержавшееся столько, считается здоровым — нарастающая задержка начинается
     * заново.
     */
    private static final Duration SETTLED = Duration.ofSeconds(5);

    private TLClient() {
    }

    /**
     * Работать, пока не остановят: подключиться, везти трафик, выждать нарастающую задержку,
     * переподключиться. Всё нужное приходит в {@code setup} — разбирать нечего, и в run
     * configuration нечего испортить.
     */
    public static void run(ClientSetup setup) throws Exception {
        Transport transport = setup.transport() == null ? Transport.WEBSOCKET : setup.transport();
        log.info("connecting to {} over {} ({} local, {} reverse forward(s))", setup.url(),
                setup.transport() == null
                        ? "websocket, falling back to http if it does not hold"
                        : transport.label(),
                setup.locals().size(), setup.reverses().size());

        // Health-эндпойнт живёт дольше любого отдельного соединения: он должен продолжать
        // отвечать, пока клиент между попытками переподключения, — а именно тогда пробе
        // честный «down» нужнее всего.
        AtomicReference<ClientConnection> live = new AtomicReference<>();
        if (setup.healthPort() > 0) {
            try {
                new ClientHealth(setup.healthHost(), setup.healthPort(), live::get);
            } catch (IOException e) {
                log.debug("cannot open the health endpoint on {}:{}: {}",
                        setup.healthHost(), setup.healthPort(), e.toString());
                return;
            } catch (NoClassDefFoundError e) {
                // В рантайме, обрезанном через jlink, может не оказаться jdk.httpserver.
                // Говорим, какого модуля не хватает: сам по себе NoClassDefFoundError называет
                // класс, который никто не свяжет с health-эндпойнтом.
                log.debug("the health endpoint needs the jdk.httpserver module, "
                        + "which this Java runtime does not have ({})", e.getMessage());
                return;
            }
        }

        int attempt = 0;
        // Выход отсюда — через прерывание: клиент, которого попросили остановиться, должен
        // остановиться, а не переподключаться. В обычной работе этот поток никто не прерывает,
        // и цикл крутится вечно.
        while (!Thread.currentThread().isInterrupted()) {
            Duration uptime;
            String reason;
            ClientConnection conn = new ClientConnection(setup.url(), setup.auth(),
                    setup.keepaliveSeconds(), setup.locals(), setup.reverses(), transport);
            live.set(conn);
            try {
                conn.run();
                uptime = conn.uptime();
                reason = "connection closed after " + uptime.toSeconds() + "s";
            } catch (InterruptedException e) {
                conn.stop();
                live.set(null);
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                uptime = conn.uptime();
                reason = "connect failed (" + rootCause(e) + ")";
            }
            // Нарастающую задержку сбрасывает только соединение, которое правда продержалось;
            // связь, падающая на каждой попытке, должна продолжать выжидать, а не долбить
            // сервер.
            boolean settled = uptime.compareTo(SETTLED) >= 0;
            attempt = settled ? 0 : attempt;
            long delayMs = backoff(attempt);
            attempt++;
            // Обнуляем до сна, чтобы проба во время нарастающей задержки видела «down», а не
            // труп только что умершего соединения.
            live.set(null);
            if (setup.transport() == null && !settled) {
                // Попытка, которая так и не продержалась, не говорит, какой транспорт виноват,
                // поэтому следующую попытку получает другой. Именно это проводит клиента через
                // сеть, которая отказывает в апгрейде до WebSocket — или соглашается на него, а
                // потом съедает кадры, — и никому не приходится это замечать и передавать
                // --transport руками. Транспорт, который продержался, оставляем: он доказал,
                // что работает.
                transport = transport.other();
                log.warn("{}, trying the {} transport, reconnecting in {} ms",
                        reason, transport.label(), delayMs);
            } else {
                log.warn("{}, reconnecting in {} ms", reason, delayMs);
            }
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * Включить логирование по каждому потоку. В клиентском режиме Spring нет, а значит, нет ни
     * {@code logging.level.*}, который можно выставить, ни работающего logback.xml — уровень
     * двигается прямо на бэкенде логирования. Прикрыто проверкой типа, чтобы неожиданный бэкенд
     * выродился в предупреждение, а не в ClassCastException на старте.
     */
    public static void enableVerbose() {
        org.slf4j.Logger pkg = LoggerFactory.getLogger("com.inyeqai.tl");
        if (pkg instanceof ch.qos.logback.classic.Logger logback) {
            logback.setLevel(ch.qos.logback.classic.Level.DEBUG);
            log.info("verbose logging on: every stream will be logged as it opens and closes");
        } else {
            log.warn("--verbose: logging backend is {}, cannot raise the level",
                    pkg.getClass().getName());
        }
    }

    /** Экспоненциальная нарастающая задержка: {@code MIN << attempt} с потолком {@code MAX}. */
    static long backoff(int attempt) {
        if (attempt >= 20) { // 1s << 20 уже больше потолка; заодно сдвиг не переполнится
            return MAX_BACKOFF_MS;
        }
        return Math.min(MIN_BACKOFF_MS << attempt, MAX_BACKOFF_MS);
    }

    /** Если в URL нет пути, дописываем эндпойнт по умолчанию, чтобы работал голый host:port. */
    static String normalizeUrl(String url) {
        String u = url;
        if (!u.startsWith("ws://") && !u.startsWith("wss://")) {
            // принимаем и http/https и отображаем в ws-схему
            if (u.startsWith("https://")) {
                u = "wss://" + u.substring("https://".length());
            } else if (u.startsWith("http://")) {
                u = "ws://" + u.substring("http://".length());
            } else {
                u = "wss://" + u;
            }
        }
        URI parsed = URI.create(u);
        String path = parsed.getPath();
        if (path == null || path.isEmpty() || path.equals("/")) {
            u = stripTrailingSlash(u) + "/tl";
        }
        return u;
    }

    private static String stripTrailingSlash(String s) {
        return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
    }

    private static String rootCause(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) {
            c = c.getCause();
        }
        return c.toString();
    }
}
