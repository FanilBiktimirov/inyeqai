package com.inyeqai.tl.common;

import java.util.ArrayList;
import java.util.List;

/**
 * A port-forward rule parsed from the command line, in chisel's syntax.
 *
 * <ul>
 *   <li>Local  {@code [bind:]port:dsthost:dstport} &mdash; the client listens, the
 *       server dials the destination. {@code reverse == false}; {@code bindHost:bindPort}
 *       is the client-side listener, {@code dstHost:dstPort} the server-side target.</li>
 *   <li>Reverse {@code R:[bind:]port:dsthost:dstport} &mdash; the server listens, the
 *       client dials the destination. {@code reverse == true}; {@code bindHost:bindPort}
 *       is the server-side listener, {@code dstHost:dstPort} the client-side target.</li>
 * </ul>
 *
 * <p>Shorter forms fill in from the back, as chisel does: {@code 3000} is
 * {@code 3000:127.0.0.1:3000} and {@code example.com:3000} is
 * {@code 3000:example.com:3000}. IPv6 literals are written in brackets
 * ({@code [::1]:3000:[::1]:80}); the brackets are stripped from the stored host so it can
 * be handed straight to {@code InetSocketAddress}.
 */
public record ForwardSpec(boolean reverse, String bindHost, int bindPort, String dstHost, int dstPort) {

    private static final String DEFAULT_DST_HOST = "127.0.0.1";

    public static ForwardSpec parse(String raw) {
        String s = raw;
        boolean reverse = false;
        if (s.startsWith("R:")) {
            reverse = true;
            s = s.substring(2);
        }
        List<String> p = split(s, raw);
        String defaultBind = reverse ? "0.0.0.0" : "127.0.0.1";
        return switch (p.size()) {
            // 3000                 -> listen 3000, dial 127.0.0.1:3000
            case 1 -> {
                int port = port(p.get(0), raw);
                yield new ForwardSpec(reverse, defaultBind, port, DEFAULT_DST_HOST, port);
            }
            // example.com:3000     -> listen 3000, dial example.com:3000
            case 2 -> {
                int port = port(p.get(1), raw);
                yield new ForwardSpec(reverse, defaultBind, port, host(p.get(0), raw), port);
            }
            // 3128:127.0.0.1:3129  -> listen 3128, dial 127.0.0.1:3129
            case 3 -> new ForwardSpec(reverse, defaultBind, port(p.get(0), raw),
                    host(p.get(1), raw), port(p.get(2), raw));
            // 0.0.0.0:3128:host:3129
            case 4 -> new ForwardSpec(reverse, host(p.get(0), raw), port(p.get(1), raw),
                    host(p.get(2), raw), port(p.get(3), raw));
            default -> throw new IllegalArgumentException("bad forward spec: " + raw
                    + " (expected [bind:]port:host:port, optionally prefixed with R:)");
        };
    }

    public Reverse toReverse() {
        if (!reverse) {
            throw new IllegalStateException("not a reverse spec: " + this);
        }
        return new Reverse(bindHost, bindPort, dstHost, dstPort);
    }

    /** Split on ':' while keeping bracketed IPv6 literals in one piece. */
    private static List<String> split(String s, String raw) {
        List<String> out = new ArrayList<>(4);
        StringBuilder cur = new StringBuilder();
        boolean inBrackets = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '[') {
                if (inBrackets) {
                    throw new IllegalArgumentException("nested '[' in forward spec: " + raw);
                }
                inBrackets = true;
                cur.append(c);
            } else if (c == ']') {
                if (!inBrackets) {
                    throw new IllegalArgumentException("unmatched ']' in forward spec: " + raw);
                }
                inBrackets = false;
                cur.append(c);
            } else if (c == ':' && !inBrackets) {
                out.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        if (inBrackets) {
            throw new IllegalArgumentException("unmatched '[' in forward spec: " + raw);
        }
        out.add(cur.toString());
        if (out.isEmpty() || out.size() > 4) {
            throw new IllegalArgumentException("bad forward spec: " + raw
                    + " (expected [bind:]port:host:port, optionally prefixed with R:)");
        }
        return out;
    }

    private static String host(String v, String raw) {
        String h = v.trim();
        if (h.length() > 1 && h.charAt(0) == '[' && h.charAt(h.length() - 1) == ']') {
            h = h.substring(1, h.length() - 1);
        }
        if (h.isEmpty()) {
            throw new IllegalArgumentException("empty host in forward spec: " + raw);
        }
        return h;
    }

    private static int port(String v, String raw) {
        int n;
        try {
            n = Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("bad port in forward spec: " + raw);
        }
        if (n < 1 || n > 65535) {
            throw new IllegalArgumentException("port out of range in forward spec: " + raw);
        }
        return n;
    }
}
