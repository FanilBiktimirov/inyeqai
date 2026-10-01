package com.laptopkit.tunnel.server;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import jakarta.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.PongMessage;
import org.springframework.web.socket.WebSocketMessage;

import com.laptopkit.tunnel.common.Frames;
import com.laptopkit.tunnel.common.Framing;

/**
 * Резервный транспорт: тот же туннель по обычному HTTP — для сетей, где WebSocket-апгрейд так
 * и не доезжает. Прокси, которые срезают {@code Upgrade}, TLS-инспекция, которая калечит
 * рукопожатие, и шлюзы, которые просто отвечают 400, — всё это делает WebSocket-транспорт
 * непригодным, пока обычные запросы продолжают работать.
 *
 * <pre>
 *   POST {path}/http/connect            открыть сессию, возвращает её id
 *   GET  {path}/http/down/{id}?from=N   долгоживущий ответ с кадрами сервер -&gt; клиент,
 *                                       продолжая после кадра N (0 = с самого начала)
 *   POST {path}/http/up/{id}?batch=N    пачка кадров N, клиент -&gt; сервер; повторяется
 * </pre>
 *
 * <p>Схема нарочно несимметричная. Вниз — один потоковый ответ, потому что сервер должен уметь
 * пушить в любой момент. Вверх — череда коротких POST'ов, а не одно длинное тело запроса:
 * посредник, который буферизует тело запроса, прежде чем переслать его дальше, устроил бы
 * потоковой загрузке дедлок, а ограничение на размер тела на входе — в той цепочке, под которую
 * этот туннель и писался, у squid'а стоит {@code REQUEST_BODY_MAX} в 500 КБ — убило бы её
 * начисто. Короткие POST'ы с известным размером выживают и там, и там.
 *
 * <p>Сессии живут дольше отдельных запросов, поэтому ничего здесь не привязано к потоку
 * запроса; всё, что касается самой сессии, делает {@link TunnelWebSocketHandler} через
 * {@link HttpCarrierSession} — там же объяснено, зачем так устроено.
 *
 * <p>Восстановить можно оба направления, и восстанавливать их приходится по-разному, потому что
 * оборвавшийся ответ и оборвавшийся запрос оставляют открытыми разные вопросы:
 *
 * <ul>
 *   <li><b>Поток вниз</b> кончается, не сказав, сколько из него доехало. Поэтому кадры
 *       нумеруются и хранятся до подтверждения, а {@code from=N} просит всё после последнего,
 *       который у клиента есть. См. {@link HttpCarrierSession#pumpTo}.
 *   <li><b>Поток вверх</b> срывается, не сказав, применил ли сервер пачку. Поэтому каждый POST
 *       несёт номер пачки: тот же номер снова — это повтор, на который надо ответить, но не
 *       применять, а номер с пропуском значит, что кадров не хватает и сессии не продолжиться.
 *       Повтор без этого продублировал бы байты внутри потока, а это порча данных, которую не
 *       заметит ни одна из сторон.
 * </ul>
 *
 * <p>Клиент рулит по кодам ответа: 409 значит «подожди и спроси ещё раз», потому что другой
 * ответ ещё сворачивается, а 404 и 410 значат, что сессию уже не спасти и туннель надо
 * поднимать заново.
 */
@RestController
@ConditionalOnProperty(name = "tunnel.http-fallback", matchIfMissing = true)
@RequestMapping("${tunnel.path:/tunnel}/http")
class HttpTunnelController {

    private static final Logger log = LoggerFactory.getLogger(HttpTunnelController.class);

    /**
     * Предел на один POST потока вверх. Клиент пакует пачки намного меньше; тело больше этого
     * значит запутавшегося или враждебного отправителя, а не настоящий трафик.
     */
    private static final int MAX_UP_BYTES = 8 * 1024 * 1024;

    private final TunnelWebSocketHandler handler;
    private final SharedSecret secret;
    private final ConcurrentHashMap<String, HttpCarrierSession> carriers = new ConcurrentHashMap<>();

    HttpTunnelController(TunnelWebSocketHandler handler, SharedSecret secret) {
        this.handler = handler;
        this.secret = secret;
        log.info("HTTP fallback transport enabled; set tunnel.http-fallback=false to remove it");
    }

    @PostMapping("/connect")
    ResponseEntity<String> connect(
            @RequestHeader(name = "X-Tunnel-Auth", required = false) String token,
            HttpServletRequest request) {
        if (!secret.accepts(token)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("unauthorized\n");
        }
        // Префикс — чтобы /status и логи говорили, каким транспортом пришла сессия.
        String id = "http-" + UUID.randomUUID();
        HttpCarrierSession carrier = new HttpCarrierSession(
                id, URI.create(request.getRequestURL().toString()),
                new InetSocketAddress(request.getRemoteAddr(), request.getRemotePort()),
                new InetSocketAddress(request.getLocalAddr(), request.getLocalPort()),
                this::release);
        carriers.put(id, carrier);
        handler.afterConnectionEstablished(carrier);
        // Сессию, клиент которой так и не открыл поток вниз, отстреливает keepalive-проход
        // самого обработчика: без потока вниз pong прийти не может, так что она протухает, как
        // любой другой замолчавший клиент.
        return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(id + "\n");
    }

    @GetMapping("/down/{id}")
    ResponseEntity<StreamingResponseBody> down(
            @PathVariable String id,
            @RequestParam(name = "from", defaultValue = "0") long from,
            @RequestHeader(name = "X-Tunnel-Auth", required = false) String token) {
        if (!secret.accepts(token)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        HttpCarrierSession carrier = carriers.get(id);
        if (carrier == null) {
            return ResponseEntity.notFound().build();
        }
        if (from < 0) {
            return ResponseEntity.badRequest().build();
        }
        // Проверяем до того, как ответ закоммичен, чтобы на невозможный запрос клиент получил
        // код, с которым можно что-то сделать, а не поток, умирающий на первом кадре.
        if (!carrier.canResumeFrom(from)) {
            log.warn("cannot resume {} from frame {}, ending the session", id, from);
            carrier.close(CloseStatus.SERVER_ERROR);
            return ResponseEntity.status(HttpStatus.GONE).build();
        }
        if (!carrier.attachDown()) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }
        log.debug("downstream attached for {} from frame {}", id, from);
        StreamingResponseBody body = out -> carrier.pumpTo(out, from);
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header("Cache-Control", "no-store")
                // Просим nginx не буферизовать этот ответ. Буферизация держала бы кадры, пока
                // какой-нибудь буфер не наполнится, а для туннеля это не задержка, а затык.
                .header("X-Accel-Buffering", "no")
                .body(body);
    }

    @PostMapping("/up/{id}")
    ResponseEntity<String> up(
            @PathVariable String id,
            // Берём как необязательный и проверяем ниже, чтобы запросу без него отказали по
            // правильной причине: дальше проверки токена не проходит ничто, и вызывающий без
            // токена не узнаёт, в чём именно он ошибся.
            @RequestParam(name = "batch", defaultValue = "-1") long batch,
            @RequestHeader(name = "X-Tunnel-Auth", required = false) String token,
            InputStream body) throws IOException {
        if (!secret.accepts(token)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("unauthorized\n");
        }
        if (batch < 1) {
            return ResponseEntity.badRequest().body("missing or bad batch number\n");
        }
        HttpCarrierSession carrier = carriers.get(id);
        if (carrier == null) {
            // Клиент не должен продолжать постить в сессию, которой больше нет; 404 говорит
            // ему выбросить этот транспорт и переподключиться.
            return ResponseEntity.notFound().build();
        }
        ReentrantLock lock = carrier.inboundLock();
        lock.lock();
        try {
            if (!carrier.beginBatch(batch)) {
                // Повтор уже применённой пачки. Ответить, не применяя её заново, — в этом весь
                // смысл: клиент никак не мог узнать, что наш прошлый ответ потерялся, а те же
                // кадры дважды продублировали бы байты внутри потока.
                log.debug("upstream batch {} on {} was already applied", batch, id);
                return ResponseEntity.noContent().build();
            }
            deliver(carrier, body);
            // Отмечаем только сейчас: пачку, сорвавшуюся на середине, повторить нельзя, поэтому
            // ниже сессию просто заканчиваем, а клиент поднимает новую.
            carrier.batchApplied(batch);
        } catch (HttpCarrierSession.CannotResume e) {
            log.warn("ending {}: {}", id, e.getMessage());
            carrier.close(CloseStatus.SERVER_ERROR);
            return ResponseEntity.status(HttpStatus.GONE).body(e.getMessage() + "\n");
        } catch (IOException e) {
            log.debug("bad upstream batch on {}: {}", id, e.toString());
            carrier.close(CloseStatus.BAD_DATA);
            return ResponseEntity.badRequest().body("bad frame batch\n");
        } finally {
            lock.unlock();
        }
        return ResponseEntity.noContent().build();
    }

    /**
     * Отдать сессии кадры из одного POST'а в том порядке, в каком их отправили. Порядок — и
     * есть весь смысл замка вокруг этого вызова: эти кадры несут байтовые потоки.
     */
    private void deliver(HttpCarrierSession carrier, InputStream body) throws IOException {
        DataInputStream in = Framing.reader(body);
        int total = 0;
        byte[] frame;
        while ((frame = Framing.read(in)) != null) {
            total += frame.length;
            if (total > MAX_UP_BYTES) {
                throw new IOException("upstream batch over " + MAX_UP_BYTES + " bytes");
            }
            if (frame.length == 0) {
                throw new IOException("empty frame");
            }
            if (!handleCarrierFrame(carrier, frame)) {
                // Всё остальное — трафик туннеля, он уходит обработчику нетронутым, как будто
                // WebSocket-сообщение доставил контейнер.
                dispatch(carrier, new BinaryMessage(frame));
            }
        }
    }

    /**
     * Отдать одно сообщение обработчику сессии. Его колбэк может бросить что угодно, и что
     * угодно брошенное оставляет эту сессию на середине байтового потока: продолжать разумным
     * образом уже нельзя, поэтому пачка срывается, а вместе с ней уходит и сессия.
     */
    private void dispatch(HttpCarrierSession carrier, WebSocketMessage<?> message) throws IOException {
        try {
            handler.handleMessage(carrier, message);
        } catch (Exception e) {
            throw new IOException("the session handler failed on " + carrier.getId(), e);
        }
    }

    /**
     * Разобраться с собственными кадрами транспорта здесь, чтобы ни обработчик, ни
     * мультиплексор ни одного такого кадра не увидели.
     *
     * @return true, если кадр был делом транспорта
     */
    private boolean handleCarrierFrame(HttpCarrierSession carrier, byte[] frame) throws IOException {
        switch (frame[0]) {
            case Frames.PING -> carrier.enqueue(Frames.pong());
            // Позволяет серверу выбросить то, что он держал на случай, если этому клиенту
            // пришлось бы за этим вернуться; почему «записано» здесь не значит «доставлено» —
            // см. HttpCarrierSession.
            case Frames.ACK -> carrier.onAck(Frames.decode(frame).ackThrough);
            // Докладываем как pong, чтобы слежение обработчика за живостью и строчка
            // «last pong» на /status значили на обоих транспортах одно и то же.
            case Frames.PONG -> dispatch(carrier, new PongMessage());
            case Frames.BYE -> {
                log.debug("client said goodbye on {}", carrier.getId());
                carrier.close(CloseStatus.NORMAL);
            }
            default -> {
                return false;
            }
        }
        return true;
    }

    /**
     * Вызывается ровно один раз на сессию, из {@link HttpCarrierSession#close}, чем бы её ни
     * закончили: {@code BYE} от клиента, обрывом ответа потока вниз или отстрелом замолчавших
     * сессий в самом обработчике, который закрывает переставшую отвечать сессию.
     */
    private void release(HttpCarrierSession carrier, CloseStatus status) {
        carriers.remove(carrier.getId(), carrier);
        // Закрывает мультиплексор и освобождает обратные слушатели — тот же колбэк, что
        // контейнер сделал бы для WebSocket'а.
        handler.afterConnectionClosed(carrier, status);
    }
}
