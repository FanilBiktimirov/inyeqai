package com.inyeqai.tl.server;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * Токен {@code --auth} и единственное место, которое решает, годится ли предъявленный.
 * И рукопожатие WebSocket, и {@code GET /status} идут через здесь, так что они не могут
 * разъехаться в разные представления о том, что считается авторизованным.
 *
 * <p>Пустой настроенный токен означает, что проверки нет вообще. Это намеренный режим только
 * для dev, громко описанный в README, и он действует одинаково: у открытого сервера страница
 * статуса тоже открыта, а не живёт неожиданным исключением из одного из двух правил.
 */
final class SharedSecret {

    private final byte[] expected;

    SharedSecret(String expected) {
        this.expected = (expected == null ? "" : expected).getBytes(StandardCharsets.UTF_8);
    }

    boolean open() {
        return expected.length == 0;
    }

    /** Сравнение за постоянное время, чтобы неверный токен нельзя было угадать байт за байтом. */
    boolean accepts(String presented) {
        if (open()) {
            return true;
        }
        if (presented == null) {
            return false;
        }
        return MessageDigest.isEqual(expected, presented.getBytes(StandardCharsets.UTF_8));
    }
}
