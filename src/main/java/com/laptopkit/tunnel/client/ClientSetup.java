package com.laptopkit.tunnel.client;

import java.util.ArrayList;
import java.util.List;

import com.laptopkit.tunnel.common.ForwardSpec;
import com.laptopkit.tunnel.common.Reverse;

/**
 * Everything the client needs to run, already resolved: the URL normalised, the forwards parsed
 * into the two kinds, the transport decided.
 *
 * <p>It exists so that nothing downstream of here has to know where the values came from.
 * {@code TunnelConfig} is what a person edits; this is what the client is handed, and a test can
 * build one directly without touching global state.
 *
 * @param transport the carrier to use, or null for auto: start on a WebSocket and alternate
 *                  after any attempt that fails to settle
 * @param healthPort where the client's health endpoint listens; zero for none
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
            throw new IllegalArgumentException("no server URL: set TunnelConfig.CLIENT_URL");
        }
        if (locals.isEmpty() && reverses.isEmpty()) {
            throw new IllegalArgumentException("no forwards: set TunnelConfig.FORWARDS");
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

    /** Split chisel-style forward strings into the local and reverse ones. */
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
        return new ClientSetup(TunnelClient.normalizeUrl(url), auth, keepaliveSeconds,
                locals, reverses, transport, healthHost, healthPort);
    }
}
