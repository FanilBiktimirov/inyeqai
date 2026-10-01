package com.laptopkit.tunnel.client;

/**
 * Чем клиент довозит кадры до сервера. Протокол туннеля в любом случае один и тот же;
 * отличается только то, что лежит под ним.
 */
public enum Transport {

    /** Один WebSocket, обычный выбор. */
    WEBSOCKET("websocket"),
    /**
     * Обычный HTTP: потоковый ответ вниз, пачки POST вверх. Для сетей, где апгрейд до
     * WebSocket не доживает до другого конца.
     */
    HTTP("http");

    private final String label;

    Transport(String label) {
        this.label = label;
    }

    /** Как это называют логи и health-эндпойнт. */
    String label() {
        return label;
    }

    /** Другой из двух — его {@code --transport auto} попробует следующим. */
    Transport other() {
        return this == WEBSOCKET ? HTTP : WEBSOCKET;
    }

}
