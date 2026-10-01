package com.inyeqai.tl.server;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Server knobs, bound from {@code tl.*} (properties, env, or CLI mapping). */
@ConfigurationProperties(prefix = "tl")
public class TLProperties {

    /** Shared secret a client must send as {@code X-TL-Auth}. Empty = open (dev only). */
    private String auth = "";

    /** Path the WebSocket endpoint is mounted at. */
    private String path = "/tl";

    /**
     * Destinations clients may reach, as regular expressions matched against
     * {@code host:port} for forward streams and {@code R:bind:port} for reverse listeners.
     * Empty means no restriction, which lets any authenticated client dial anything the
     * server can reach. Patterns are not anchored implicitly &mdash; write {@code ^...$}.
     */
    private List<String> allow = new ArrayList<>();

    /**
     * Serve {@code GET /status}, the live view of sessions and streams. It sits behind the
     * same token as the tunnel; set this to false to remove the endpoint entirely.
     */
    private boolean status = true;

    /** How often to ping each client. Zero disables server-side keepalive and reaping. */
    private Duration keepalive = Duration.ofSeconds(25);

    /**
     * Silence after which a client is considered gone and its session closed. This is what
     * frees the reverse listeners a dead client still holds, so a reconnecting client can
     * bind its ports again.
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
