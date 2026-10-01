package com.laptopkit.tunnel.server;

import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;

/**
 * Rejects the WebSocket upgrade with 401 unless the client presents the configured token in
 * {@code X-Tunnel-Auth}. An empty configured token disables the check.
 */
public class AuthHandshakeInterceptor implements HandshakeInterceptor {

    private final SharedSecret secret;

    AuthHandshakeInterceptor(SharedSecret secret) {
        this.secret = secret;
    }

    @Override
    public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response,
                                   WebSocketHandler wsHandler, Map<String, Object> attributes) {
        if (secret.accepts(request.getHeaders().getFirst("X-Tunnel-Auth"))) {
            return true;
        }
        response.setStatusCode(HttpStatus.UNAUTHORIZED);
        return false;
    }

    @Override
    public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response,
                               WebSocketHandler wsHandler, Exception exception) {
        // nothing
    }
}
