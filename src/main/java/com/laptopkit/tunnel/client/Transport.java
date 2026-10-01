package com.laptopkit.tunnel.client;

/**
 * How the client carries frames to the server. The tunnel protocol is the same either way;
 * only the thing underneath it differs.
 */
public enum Transport {

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

}
