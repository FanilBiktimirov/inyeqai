package com.inyeqai.tl;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import com.inyeqai.tl.InyeqaiApplication;
import com.inyeqai.tl.client.Transport;

/**
 * Все параметры запуска, полями. Единственный файл, который надо править перед нажатием Run.
 *
 * <p>В этой ветке туннель вообще не принимает аргументов командной строки: нечего заводить
 * в run configuration и нечего встраивать между брейкпоинтом и значением, на которое он
 * смотрит. Выставить {@link #MODE}, поправить что ещё важно — и запустить
 * {@link InyeqaiApplication} прямо из редактора.
 *
 * <p>Поля намеренно не {@code final}. Константу компилятор заинлайнит, и из работающего
 * отладчика её уже не поменять; а эти — поменять можно, так что значение правится на
 * брейкпоинте до строки, которая его читает.
 *
 * <p>Исключение — {@link #MODE}, {@link #AUTH}, {@link #CLIENT_URL} и {@link #FORWARDS}: их
 * можно задать переменными окружения {@code TLCFG_<имя поля>}. Так настройки запуска в
 * {@code .idea/runConfigurations} выбирают сценарий («локально» или «клиент к стенду»), не
 * трогая этот файл, а секрет стенда остаётся в {@code .idea}, которая в git не попадает.
 * Переменные читаются один раз, при загрузке класса; дальше это обычные поля.
 *
 * <p>Порты по умолчанию высокие, и это не случайно. Обычные подозреваемые — 3128, 3129, 3130,
 * 8080, 8090 — заняты живой цепочкой, под которую туннель и писался, и отладочный запуск,
 * тихо утащивший один из них, сломал бы что-то настоящее, делая вид, что всё работает.
 */
public final class TLConfig {

    /** Что поднимать. */
    public enum Mode {
        /** Только публичная сторона. */
        SERVER,
        /** Только вход в туннель; сервер уже должен работать по {@link #CLIENT_URL}. */
        CLIENT,
        /**
         * И то и другое, в одной JVM. Смысл режима: один процесс, оба конца, так что
         * брейкпоинт в {@code Mux} или {@code HttpLink} ловит ту сторону, которая дошла до
         * него первой, и весь обмен кадрами виден в одном отладчике.
         */
        BOTH,
        /** Дёрнуть health-эндпойнт и выйти — так же, как это делает HEALTHCHECK контейнера. */
        HEALTHCHECK
    }

    public static Mode MODE = Mode.valueOf(env("MODE", "BOTH").toUpperCase(Locale.ROOT));

    /** Логировать каждый поток на открытии и закрытии, с обеих сторон. При отладке — полезно. */
    public static boolean VERBOSE = true;

    // ─── сервер ────────────────────────────────────────────────────────────────────────────

    public static int SERVER_PORT = 18080;

    /** Где слушает сервер. Пусто — значит на всех интерфейсах. */
    public static String SERVER_HOST = "127.0.0.1";

    /**
     * Общий секрет, который должен предъявить клиент. Пусто — проверка выключена. Секрет стенда
     * сюда не пишется: ветка уходит на GitHub. Его отдаёт настройка запуска через
     * {@code TLCFG_AUTH}.
     */
    public static String AUTH = env("AUTH", "TL:debug");

    /** Путь, на который смонтирован WebSocket-эндпойнт; запасной HTTP-транспорт живёт под ним. */
    public static String PATH = "/tl";

    /**
     * Куда клиентам можно дозваниваться — регулярками по {@code host:port} и
     * {@code R:bind:port}. Пусто — значит куда угодно, докуда достаёт сервер: при отладке
     * удобно, для чего-либо публичного — неверно.
     */
    public static List<String> ALLOW = List.of();
    public static Duration SERVER_KEEPALIVE = Duration.ofSeconds(25);
    public static Duration PONG_TIMEOUT = Duration.ofSeconds(75);
    public static boolean HTTP_FALLBACK = true;
    public static boolean STATUS = true;

    // ─── клиент ────────────────────────────────────────────────────────────────────────────

    /**
     * К какому серверу подключаться. В режиме {@link Mode#BOTH} игнорируется: адрес там
     * собирается из {@link #SERVER_PORT} и {@link #PATH}, чтобы порт не держать в двух местах.
     *
     * <p>А вот в режиме {@link Mode#CLIENT} держать приходится, и это ловушка: порт и путь
     * здесь свои, так что правка {@link #SERVER_PORT} сюда молча не доедет. Если клиент не
     * достучался до сервера, которого вы только что переставили на другой порт, — смотреть
     * надо сюда.
     */
    public static String CLIENT_URL = env("CLIENT_URL", "ws://127.0.0.1:18080/tl");

    /**
     * Чем везти кадры. {@code null} — авто: начать с WebSocket и свалиться на HTTP, если
     * попытка не удержалась. Прибить гвоздями, чтобы намеренно воспроизвести один транспорт.
     */
    public static Transport TRANSPORT = null;

    /** Как часто клиент пингует. Ноль выключает его keepalive, вместе с pong'ами. */
    public static int CLIENT_KEEPALIVE_SECONDS = 25;

    /**
     * Пробросы в синтаксисе {@code [bind:]port:host:port}, с {@code R:} для
     * обратного. Хранятся текстом, потому что так они написаны везде ещё — в README, в
     * compose-файле, в обычной командной строке — и плохой проброс громко падает на старте.
     *
     * <p>Значение по умолчанию замыкает петлю, которой не нужно ничего постороннего:
     * подключение к {@code 127.0.0.1:18128} уходит в туннель и выходит обратно на собственный
     * HTTP-порт сервера, так что {@code curl http://127.0.0.1:18128/healthz} прогоняет весь
     * путь целиком.
     */
    public static List<String> FORWARDS = envList("FORWARDS", List.of(
            "18128:127.0.0.1:18080"
            // , "R:18130:127.0.0.1:18080"   // обратный: слушает сервер, дозванивается клиент
    ));

    /** Где слушает собственный health-эндпойнт клиента. Ноль — выключить. */
    public static int HEALTH_PORT = 19000;

    public static String HEALTH_HOST = "127.0.0.1";

    public static String HEALTHCHECK_URL = "http://127.0.0.1:19000/healthz";

    private TLConfig() {
    }

    public static String clientUrl() {
        if (MODE == Mode.BOTH) {
            return "ws://127.0.0.1:" + SERVER_PORT + PATH;
        }
        return CLIENT_URL;
    }

    /** Значение из {@code TLCFG_<name>}, если оно задано, иначе {@code fallback}. */
    private static String env(String name, String fallback) {
        String v = System.getenv("TLCFG_" + name);
        return v == null || v.isBlank() ? fallback : v.trim();
    }

    /** То же для списка: элементы через запятую. */
    private static List<String> envList(String name, List<String> fallback) {
        String v = System.getenv("TLCFG_" + name);
        if (v == null || v.isBlank()) {
            return fallback;
        }
        return Arrays.stream(v.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }
}
