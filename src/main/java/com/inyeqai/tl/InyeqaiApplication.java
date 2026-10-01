package com.inyeqai.tl;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MapPropertySource;

import com.inyeqai.tl.client.ClientSetup;
import com.inyeqai.tl.client.TLClient;

/**
 * Одна точка входа, без аргументов. Что запускается и как — решают поля в
 * {@link TLConfig}; этот класс только превращает их в работающий сервер, работающий
 * клиент или и то и другое сразу.
 *
 * <p>В этом весь смысл ветки. Туннель неудобно отлаживать через командную строку:
 * интересные поломки живут в кадрах между двумя концами, а добраться до них раньше означало
 * две run configuration, два терминала и аккуратную пару списков аргументов. Здесь — одна
 * зелёная стрелка, а в {@link TLConfig.Mode#BOTH} ещё и один процесс с обоими концами
 * внутри.
 */
@SpringBootApplication
public class InyeqaiApplication {

    public static void main(String[] args) throws Exception {
        switch (TLConfig.MODE) {
            case SERVER -> {
                startServer();
                Thread.currentThread().join(); // живым его держат потоки Spring; ждём здесь
            }
            case CLIENT -> runClient();
            case BOTH -> runBoth();
            case HEALTHCHECK -> healthcheck();
        }
    }

    /**
     * Сначала сервер, потом клиент в этом же потоке. Старт сервера синхронный, так что к
     * моменту подключения клиента есть к чему подключаться.
     *
     * <p>Клиент на главном потоке — намеренно: так стек, который отладчик показывает на
     * брейкпоинте, остаётся коротким и узнаваемым, а не растущим из потока пула.
     */
    private static void runBoth() throws Exception {
        ConfigurableApplicationContext context = startServer();
        try {
            runClient();
        } finally {
            context.close();
        }
    }

    /**
     * Поднять сервер, передав поля как свойства Spring — так Spring и настраивается.
     *
     * <p>Свойства кладутся в начало окружения, а не отдаются
     * {@code SpringApplicationBuilder.properties}, который регистрирует их как
     * <em>default</em>-свойства — самый низкий приоритет, какой есть, ниже
     * {@code application.yml}. Сделай так — и каждое поле отсюда молча перебьётся из yml:
     * сервер поднялся бы на 8080 вместо {@link TLConfig#SERVER_PORT}, а клиент всю жизнь
     * безуспешно стучался бы в порт, который никто не слушает. Старая командная строка этого
     * избегала случайно — аргументы командной строки приоритетнее yml.
     */
    private static ConfigurableApplicationContext startServer() {
        Map<String, Object> props = new HashMap<>();
        props.put("server.port", TLConfig.SERVER_PORT);
        if (!TLConfig.SERVER_HOST.isBlank()) {
            props.put("server.address", TLConfig.SERVER_HOST);
        }
        props.put("TL.auth", TLConfig.AUTH);
        props.put("TL.path", TLConfig.PATH);
        props.put("TL.status", TLConfig.STATUS);
        props.put("TL.http-fallback", TLConfig.HTTP_FALLBACK);
        props.put("TL.keepalive", TLConfig.SERVER_KEEPALIVE);
        props.put("TL.pong-timeout", TLConfig.PONG_TIMEOUT);
        // Список Spring привязывает по индексу — так же, как раньше повторяющийся флаг.
        List<String> allow = TLConfig.ALLOW;
        for (int i = 0; i < allow.size(); i++) {
            props.put("TL.allow[" + i + "]", allow.get(i));
        }
        if (TLConfig.VERBOSE) {
            props.put("logging.level.com.inyeqai.TL", "DEBUG");
        }
        return new SpringApplicationBuilder(com.inyeqai.tl.InyeqaiApplication.class)
                .initializers(context -> context.getEnvironment().getPropertySources()
                        .addFirst(new MapPropertySource("TLConfig", props)))
                .run();
    }

    /** Отдать клиенту его параметры, уже разрешённые. Блокируется, пока процесс не остановят. */
    private static void runClient() throws Exception {
        ClientSetup setup;
        try {
            setup = ClientSetup.of(
                    TLConfig.clientUrl(),
                    TLConfig.AUTH,
                    TLConfig.CLIENT_KEEPALIVE_SECONDS,
                    TLConfig.FORWARDS,
                    TLConfig.TRANSPORT,
                    TLConfig.HEALTH_HOST,
                    TLConfig.HEALTH_PORT);
            if (TLConfig.VERBOSE) {
                TLClient.enableVerbose();
            }
        } catch (IllegalArgumentException e) {
            // Плохое поле должно сказать, какое именно, и сказать до того, как что-то
            // начнёт слушать.
            System.err.println("TLConfig: " + e.getMessage());
            return;
        }
        TLClient.run(setup);
    }

    /**
     * Дёрнуть health-эндпойнт и выйти с 0 или 1, чтобы этим мог пользоваться HEALTHCHECK
     * контейнера.
     *
     * <p>Живёт в jar'е потому, что в runtime-образе нет ни curl, ни wget, а тащить один из
     * них только чтобы задать локальному порту вопрос — обмен хуже, чем переиспользовать
     * HTTP-клиент, который в runtime и так есть.
     */
    private static void healthcheck() {
        String url = TLConfig.HEALTHCHECK_URL;
        try {
            HttpResponse<String> res = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(5)).build()
                    .send(HttpRequest.newBuilder(URI.create(url))
                                    .timeout(Duration.ofSeconds(5)).GET().build(),
                            HttpResponse.BodyHandlers.ofString());
            // Печатаем в любом случае: docker эту выдачу сохраняет, а "stale" против "down" —
            // это разница между замолчавшим пиром и полным отсутствием подключения.
            System.out.print(res.body());
            System.exit(res.statusCode() >= 200 && res.statusCode() < 300 ? 0 : 1);
        } catch (Exception e) {
            System.out.println("unreachable: " + e);
            System.exit(1);
        }
    }
}
