package com.inyeqai.tl.common;

/**
 * Один обратный проброс — в том виде, в котором клиент просит сервер его поднять.
 *
 * <p>Сервер открывает слушателя на {@code bindHost:serverPort}; каждое принятое там
 * соединение уходит по туннелю к клиенту, а тот дозванивается до
 * {@code clientHost:clientPort} у себя. Так работает проброс {@code R:...}.
 */
public record Reverse(String bindHost, int serverPort, String clientHost, int clientPort) {
}
