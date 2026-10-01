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

    /** Токен {@code --auth}, общий для рукопожатия и {@code GET /status}. */
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

    /** Поднять лимиты контейнера на размер сообщения выше наших кусков данных по 16 КБ. */
    @Bean
    public ServletServerContainerFactoryBean createWebSocketContainer() {
        ServletServerContainerFactoryBean c = new ServletServerContainerFactoryBean();
        c.setMaxBinaryMessageBufferSize(256 * 1024);
        c.setMaxTextMessageBufferSize(64 * 1024);
        // Никакого ограничения на простой со стороны контейнера: живость решает собственный
        // ping/pong-отстрел в хендлере, который ещё и освобождает обратные слушатели, занятые
        // мёртвой сессией. Оставь это контейнеру — и он вместо того резал бы простаивающие,
        // но здоровые туннели.
        c.setMaxSessionIdleTimeout(0L);
        return c;
    }
}
