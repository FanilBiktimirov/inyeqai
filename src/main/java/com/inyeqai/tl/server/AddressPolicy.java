package com.inyeqai.tl.server;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Which destinations clients may reach: regular expressions matched against
 * {@code host:port} for forward streams and {@code R:bind:port} for reverse listeners.
 *
 * <p>An empty list allows everything, which keeps the previous behaviour; it is also the
 * reason the server is only safe on a public address with both a token and a list.
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
                            "tl.allow: bad regular expression '" + p + "': " + e.getDescription(), e);
                }
            }
        }
        this.patterns = List.copyOf(compiled);
    }

    boolean unrestricted() {
        return patterns.isEmpty();
    }

    /** A forward stream's destination, as {@code host:port}. */
    boolean allowsDial(String host, int port) {
        return matches(host + ":" + port);
    }

    /** A reverse listener, as {@code R:bind:port}. */
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
