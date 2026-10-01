package com.inyeqai.tunnel.common;

/**
 * One reverse forward, as the client asks the server to set it up.
 *
 * <p>The server opens a listener on {@code bindHost:serverPort}; every connection
 * accepted there is tunnelled to the client, which dials {@code clientHost:clientPort}
 * on its own side. Mirror of chisel's {@code R:...} forward.
 */
public record Reverse(String bindHost, int serverPort, String clientHost, int clientPort) {
}
