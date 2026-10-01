package com.inyeqai.tl.common;

/**
 * One reverse forward, as the client asks the server to set it up.
 *
 * <p>The server opens a listener on {@code bindHost:serverPort}; every connection
 * accepted there is tunnelled to the client, which dials {@code clientHost:clientPort}
 * on its own side. This is what an {@code R:...} forward asks for.
 */
public record Reverse(String bindHost, int serverPort, String clientHost, int clientPort) {
}
