package com.inyeqai.tl.server;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;

@Configuration
@EnableWebSocket
@EnableConfigurationProperties(TLProperties.class)
public class WebSocketConfig implements WebSocketConfigurer {

    private final TLProperties props;
    private final TLWebSocketHandler handler;

    public WebSocketConfig(TLProperties props, TLWebSocketHandler handler) {
        this.props = props;
        this.handler = handler;
    }

    /** The {@code --auth} token, shared by the handshake and {@code GET /status}. */
    @Bean
    SharedSecret sharedSecret() {
        return new SharedSecret(props.getAuth());
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(handler, props.getPath())
                .addInterceptors(new AuthHandshakeInterceptor(sharedSecret()))
                .setAllowedOriginPatterns("*");
    }

    /** Raise the container message-size limits above our 16 KB payload chunks. */
    @Bean
    public ServletServerContainerFactoryBean createWebSocketContainer() {
        ServletServerContainerFactoryBean c = new ServletServerContainerFactoryBean();
        c.setMaxBinaryMessageBufferSize(256 * 1024);
        c.setMaxTextMessageBufferSize(64 * 1024);
        // No container idle cap: liveness is decided by the handler's own ping/pong reaper,
        // which also releases the reverse listeners a dead session was holding. Leaving it
        // to the container would cut idle-but-healthy tunnels instead.
        c.setMaxSessionIdleTimeout(0L);
        return c;
    }
}
