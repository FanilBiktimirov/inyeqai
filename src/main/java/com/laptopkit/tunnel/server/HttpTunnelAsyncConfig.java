package com.laptopkit.tunnel.server;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * What the downstream response of the HTTP fallback needs from Spring MVC, and nothing else.
 *
 * <p>Two defaults would otherwise break it:
 *
 * <ul>
 *   <li><b>The async timeout.</b> Tomcat gives an async request 30 seconds. For a tunnel that
 *       is not a timeout but a hard limit on how long it may stay up, so a healthy idle
 *       carrier would be cut every half minute. Zero or less means no deadline, which is the
 *       honest setting here: liveness is decided by the keepalive exchange, which notices a
 *       dead peer rather than guessing from elapsed time. This mirrors
 *       {@code setMaxSessionIdleTimeout(0)} on the WebSocket container.
 *   <li><b>The executor.</b> Boot's default task executor keeps a handful of core threads and
 *       an unbounded queue, so beyond those few threads work would queue instead of running
 *       &mdash; and a queued downstream response is a session that never starts. The pool
 *       below hands every carrier a thread of its own instead, with no queue.
 * </ul>
 *
 * <p>Each connected HTTP-transport session holds one of these threads for as long as it is
 * connected, so {@code maxPoolSize} is also the limit on concurrent sessions over this
 * transport. That is a deliberate trade: a few hundred threads cost little, and writing
 * frames with ordinary blocking I/O keeps the carrier simple enough to reason about.
 */
@Configuration
@ConditionalOnProperty(name = "tunnel.http-fallback", matchIfMissing = true)
class HttpTunnelAsyncConfig implements WebMvcConfigurer {

    private final ThreadPoolTaskExecutor downstreamExecutor;

    HttpTunnelAsyncConfig(ThreadPoolTaskExecutor downstreamExecutor) {
        this.downstreamExecutor = downstreamExecutor;
    }

    @Bean(destroyMethod = "shutdown")
    static ThreadPoolTaskExecutor downstreamExecutor() {
        ThreadPoolTaskExecutor pool = new ThreadPoolTaskExecutor();
        pool.setThreadNamePrefix("tunnel-down-");
        pool.setCorePoolSize(2);
        pool.setMaxPoolSize(200);
        // No queue: a queued carrier is a session that silently never starts, so the pool
        // must grow to meet demand and refuse loudly once it cannot.
        pool.setQueueCapacity(0);
        pool.setKeepAliveSeconds(60);
        pool.setAllowCoreThreadTimeOut(true);
        pool.setDaemon(true);
        return pool;
    }

    @Override
    public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
        configurer.setDefaultTimeout(-1);
        configurer.setTaskExecutor(downstreamExecutor);
    }
}
