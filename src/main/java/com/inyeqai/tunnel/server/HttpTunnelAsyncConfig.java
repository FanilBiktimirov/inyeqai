package com.inyeqai.tunnel.server;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Всё, что ответу потока вниз в запасном HTTP-транспорте нужно от Spring MVC, и ничего сверх
 * этого.
 *
 * <p>Иначе его сломали бы два дефолта:
 *
 * <ul>
 *   <li><b>Async-таймаут.</b> Tomcat даёт асинхронному запросу 30 секунд. Для туннеля это не
 *       таймаут, а жёсткий предел на то, сколько ему позволено жить: здоровый простаивающий
 *       транспорт рубили бы каждые полминуты. Ноль или меньше означает «без срока» — и это
 *       здесь честная настройка: живость решает обмен keepalive, который замечает мёртвого
 *       соседа, а не гадает по прошедшему времени. То же самое, что
 *       {@code setMaxSessionIdleTimeout(0)} у WebSocket-контейнера.
 *   <li><b>Исполнитель.</b> У дефолтного task executor'а в Boot'е горстка core-потоков и
 *       неограниченная очередь, так что дальше этой горстки работа встала бы в очередь вместо
 *       того, чтобы выполняться, — а ответ потока вниз, попавший в очередь, это сессия, которая
 *       так и не началась. Пул ниже вместо этого выдаёт каждому транспорту собственный поток и
 *       очереди не имеет.
 * </ul>
 *
 * <p>Каждая подключённая сессия HTTP-транспорта держит один такой поток всё время, пока
 * подключена, поэтому {@code maxPoolSize} — это ещё и предел на число одновременных сессий на
 * этом транспорте. Это осознанный компромисс: пара сотен потоков стоит дёшево, а запись кадров
 * обычным блокирующим I/O оставляет транспорт достаточно простым, чтобы о нём можно было
 * рассуждать.
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
        // Без очереди: транспорт, попавший в очередь, это сессия, которая молча так и не
        // началась, поэтому пул должен расти под спрос и громко отказывать, когда уже не может.
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
