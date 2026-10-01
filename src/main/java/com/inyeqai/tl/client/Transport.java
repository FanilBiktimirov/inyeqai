package com.inyeqai.tl.client;

import java.util.Locale;

/**
 * How the client carries frames to the server. The tunnel protocol is the same either way;
 * only the thing underneath it differs.
 */
enum Transport {

    /** One WebSocket, the normal choice. */
    WEBSOCKET("websocket"),
    /**
     * Plain HTTP: a streaming response down, batched POSTs up. For networks where the
     * WebSocket upgrade does not survive the trip.
     */
    HTTP("http");

    private final String label;

    Transport(String label) {
        this.label = label;
    }

    /** What logs and the health endpoint call it. */
    String label() {
        return label;
    }

    /** The other one, for {@code --transport auto} to try next. */
    Transport other() {
        return this == WEBSOCKET ? HTTP : WEBSOCKET;
    }

    /**
     * Parse the {@code --transport} value. {@code auto} is handled by the caller, which has
     * to alternate rather than pick, so it is not a value here.
     */
    static Transport parse(String raw) {
        return switch (raw.trim().toLowerCase(Locale.ROOT)) {
            case "ws", "websocket", "wss" -> WEBSOCKET;
            case "http", "https" -> HTTP;
            default -> throw new IllegalArgumentException(
                    "--transport: expected auto, ws or http, got " + raw);
        };
    }
}
