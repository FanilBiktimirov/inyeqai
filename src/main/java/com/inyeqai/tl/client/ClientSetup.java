package com.inyeqai.tl.client;

import java.util.ArrayList;
import java.util.List;

import com.inyeqai.tl.common.ForwardSpec;
import com.inyeqai.tl.common.Reverse;

/**
 * Всё, что клиенту нужно для работы, уже разобранное: URL нормализован, пробросы разложены на два
 * вида, транспорт выбран.
 *
 * <p>Он существует, чтобы ничему ниже по коду не приходилось знать, откуда взялись значения.
 * {@code TLConfig} — то, что правит человек; а это — то, что отдают клиенту, и тест может
 * собрать такой объект напрямую, не трогая глобальное состояние.
 *
 * @param transport какой транспорт использовать, или null для auto: начать с WebSocket и
 *                  чередовать после каждой попытки, которая не продержалась
 * @param healthPort где слушает health-эндпойнт клиента; ноль — не слушает нигде
 */
public record ClientSetup(
        String url,
        String auth,
        int keepaliveSeconds,
        List<ForwardSpec> locals,
        List<Reverse> reverses,
        Transport transport,
        String healthHost,
        int healthPort) {

    public ClientSetup {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("no server URL: set TLConfig.CLIENT_URL");
        }
        if (locals.isEmpty() && reverses.isEmpty()) {
            throw new IllegalArgumentException("no forwards: set TLConfig.FORWARDS");
        }
        if (keepaliveSeconds < 0) {
            throw new IllegalArgumentException("negative keepalive: " + keepaliveSeconds);
        }
        if (healthPort < 0 || healthPort > 65535) {
            throw new IllegalArgumentException("health port out of range: " + healthPort);
        }
        locals = List.copyOf(locals);
        reverses = List.copyOf(reverses);
    }

    /** Разложить строки пробросов в стиле chisel на локальные и обратные. */
    public static ClientSetup of(String url, String auth, int keepaliveSeconds,
                                 List<String> forwards, Transport transport,
                                 String healthHost, int healthPort) {
        List<ForwardSpec> locals = new ArrayList<>();
        List<Reverse> reverses = new ArrayList<>();
        for (String spec : forwards) {
            ForwardSpec parsed = ForwardSpec.parse(spec);
            if (parsed.reverse()) {
                reverses.add(parsed.toReverse());
            } else {
                locals.add(parsed);
            }
        }
        return new ClientSetup(TLClient.normalizeUrl(url), auth, keepaliveSeconds,
                locals, reverses, transport, healthHost, healthPort);
    }
}
