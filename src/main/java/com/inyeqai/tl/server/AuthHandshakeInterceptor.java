package com.inyeqai.tl.server;

import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;

/**
 * Отклоняет WebSocket-апгрейд с 401, если клиент не предъявил настроенный токен в
 * {@code X-TL-Auth}. Пустой токен в настройках отключает проверку.
 */
public class AuthHandshakeInterceptor implements HandshakeInterceptor {

    private final SharedSecret secret;

    AuthHandshakeInterceptor(SharedSecret secret) {
        this.secret = secret;
    }

    @Override
    public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response,
                                   WebSocketHandler wsHandler, Map<String, Object> attributes) {
        if (secret.accepts(request.getHeaders().getFirst("X-TL-Auth"))) {
            return true;
        }
        response.setStatusCode(HttpStatus.UNAUTHORIZED);
        return false;
    }

    @Override
    public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response,
                               WebSocketHandler wsHandler, Exception exception) {
        // ничего
    }
}
