package com.laptopkit.tunnel;

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

import com.laptopkit.tunnel.client.ClientSetup;
import com.laptopkit.tunnel.client.TunnelClient;

/**
 * Одна точка входа, без аргументов. Что запускается и как — решают поля в
 * {@link TunnelConfig}; этот класс только превращает их в работающий сервер, работающий
 * клиент или и то и другое сразу.
 *
 * <p>В этом весь смысл ветки. Туннель неудобно отлаживать через командную строку:
 * интересные поломки живут в кадрах между двумя концами, а добраться до них раньше означало
 * две run configuration, два терминала и аккуратную пару списков аргументов. Здесь — одна
 * зелёная стрелка, а в {@link TunnelConfig.Mode#BOTH} ещё и один процесс с обоими концами
 * внутри.
 */
@SpringBootApplication
public class TunnelApplication {

    public static void main(String[] args) throws Exception {
        switch (TunnelConfig.MODE) {
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
     * сервер поднялся бы на 8080 вместо {@link TunnelConfig#SERVER_PORT}, а клиент всю жизнь
     * безуспешно стучался бы в порт, который никто не слушает. Старая командная строка этого
     * избегала случайно — аргументы командной строки приоритетнее yml.
     */
    private static ConfigurableApplicationContext startServer() {
        Map<String, Object> props = new HashMap<>();
        props.put("server.port", TunnelConfig.SERVER_PORT);
        if (!TunnelConfig.SERVER_HOST.isBlank()) {
            props.put("server.address", TunnelConfig.SERVER_HOST);
        }
        props.put("tunnel.auth", TunnelConfig.AUTH);
        props.put("tunnel.path", TunnelConfig.PATH);
        props.put("tunnel.status", TunnelConfig.STATUS);
        props.put("tunnel.http-fallback", TunnelConfig.HTTP_FALLBACK);
        props.put("tunnel.keepalive", TunnelConfig.SERVER_KEEPALIVE);
        props.put("tunnel.pong-timeout", TunnelConfig.PONG_TIMEOUT);
        // Список Spring привязывает по индексу — так же, как раньше повторяющийся флаг.
        List<String> allow = TunnelConfig.ALLOW;
        for (int i = 0; i < allow.size(); i++) {
            props.put("tunnel.allow[" + i + "]", allow.get(i));
        }
        if (TunnelConfig.VERBOSE) {
            props.put("logging.level.com.laptopkit.tunnel", "DEBUG");
        }
        return new SpringApplicationBuilder(TunnelApplication.class)
                .initializers(context -> context.getEnvironment().getPropertySources()
                        .addFirst(new MapPropertySource("tunnelConfig", props)))
                .run();
    }

    /** Отдать клиенту его параметры, уже разрешённые. Блокируется, пока процесс не остановят. */
    private static void runClient() throws Exception {
        ClientSetup setup;
        try {
            setup = ClientSetup.of(
                    TunnelConfig.clientUrl(),
                    TunnelConfig.AUTH,
                    TunnelConfig.CLIENT_KEEPALIVE_SECONDS,
                    TunnelConfig.FORWARDS,
                    TunnelConfig.TRANSPORT,
                    TunnelConfig.HEALTH_HOST,
                    TunnelConfig.HEALTH_PORT);
            if (TunnelConfig.VERBOSE) {
                TunnelClient.enableVerbose();
            }
        } catch (IllegalArgumentException e) {
            // Плохое поле должно сказать, какое именно, и сказать до того, как что-то
            // начнёт слушать.
            System.err.println("TunnelConfig: " + e.getMessage());
            return;
        }
        TunnelClient.run(setup);
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
        String url = TunnelConfig.HEALTHCHECK_URL;
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
