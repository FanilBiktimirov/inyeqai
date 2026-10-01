package com.laptopkit.tunnel.server;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Куда клиентам разрешено ходить — в том же виде, что и списки адресов в {@code --authfile}
 * у chisel: регулярные выражения, которые матчатся против {@code host:port} для прямых потоков
 * и против {@code R:bind:port} для обратных слушателей.
 *
 * <p>Пустой список разрешает всё — так себя вёл сервер и раньше; по этой же причине на
 * публичном адресе он безопасен только когда есть и токен, и список.
 */
final class AddressPolicy {

    private final List<Pattern> patterns;

    AddressPolicy(List<String> raw) {
        List<Pattern> compiled = new ArrayList<>();
        if (raw != null) {
            for (String p : raw) {
                if (p == null || p.isBlank()) {
                    continue;
                }
                try {
                    compiled.add(Pattern.compile(p.trim()));
                } catch (PatternSyntaxException e) {
                    throw new IllegalArgumentException(
                            "tunnel.allow: bad regular expression '" + p + "': " + e.getDescription(), e);
                }
            }
        }
        this.patterns = List.copyOf(compiled);
    }

    boolean unrestricted() {
        return patterns.isEmpty();
    }

    /** Адрес назначения прямого потока, в виде {@code host:port}. */
    boolean allowsDial(String host, int port) {
        return matches(host + ":" + port);
    }

    /** Обратный слушатель в виде {@code R:bind:port} — нотация как у chisel. */
    boolean allowsReverse(String bindHost, int port) {
        return matches("R:" + bindHost + ":" + port);
    }

    private boolean matches(String addr) {
        if (patterns.isEmpty()) {
            return true;
        }
        for (Pattern p : patterns) {
            if (p.matcher(addr).find()) {
                return true;
            }
        }
        return false;
    }
}
