package com.inyeqai.tunnel.server;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Серверные настройки, привязанные к {@code tunnel.*} (свойства, env или маппинг из CLI). */
@ConfigurationProperties(prefix = "tunnel")
public class TunnelProperties {

    /**
     * Общий секрет, который клиент должен прислать в {@code X-Tunnel-Auth}. Пусто = открыто
     * (только для dev).
     */
    private String auth = "";

    /** Путь, на который смонтирован WebSocket-эндпойнт. */
    private String path = "/tunnel";

    /**
     * Куда клиентам можно достучаться — регулярные выражения, которые сопоставляются с
     * {@code host:port} для прямых потоков и с {@code R:bind:port} для обратных слушателей.
     * Пусто — значит без ограничений, и тогда любой прошедший аутентификацию клиент может
     * дозвониться куда угодно, докуда достаёт сервер. Шаблоны неявно не привязываются к
     * границам строки — пишите {@code ^...$}.
     */
    private List<String> allow = new ArrayList<>();

    /**
     * Отдавать {@code GET /status} — живой срез сессий и потоков. Закрыт тем же токеном, что
     * и туннель; поставьте false, чтобы убрать эндпойнт совсем.
     */
    private boolean status = true;

    /** Как часто пинговать каждого клиента. Ноль выключает серверный keepalive и отстрел. */
    private Duration keepalive = Duration.ofSeconds(25);

    /**
     * Сколько молчания нужно, чтобы считать клиента ушедшим и закрыть его сессию. Именно это
     * освобождает обратные слушатели, которые всё ещё держит мёртвый клиент, — чтобы
     * переподключающийся клиент снова смог занять свои порты.
     */
    private Duration pongTimeout = Duration.ofSeconds(75);

    public String getAuth() {
        return auth;
    }

    public void setAuth(String auth) {
        this.auth = auth;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public List<String> getAllow() {
        return allow;
    }

    public void setAllow(List<String> allow) {
        this.allow = allow == null ? new ArrayList<>() : allow;
    }

    public boolean isStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    public Duration getKeepalive() {
        return keepalive;
    }

    public void setKeepalive(Duration keepalive) {
        this.keepalive = keepalive;
    }

    public Duration getPongTimeout() {
        return pongTimeout;
    }

    public void setPongTimeout(Duration pongTimeout) {
        this.pongTimeout = pongTimeout;
    }
}
