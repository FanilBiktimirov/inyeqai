package com.inyeqai.tl.server;

import java.util.List;

import com.inyeqai.tl.common.StreamInfo;

/**
 * Как выглядит одна сессия туннеля в конкретный момент. Направления байтов названы с точки
 * зрения сервера, а {@code lastPongMillis} — это возраст свежайшего ответа на keepalive, то
 * самое число, которое говорит, жива ли сессия на самом деле или всего лишь всё ещё
 * подключена.
 *
 * @param streams       живые потоки этой сессии
 * @param resumes       сколько раз транспорт этой сессии приходилось восстанавливать; для
 *                      WebSocket всегда ноль — у него такого понятия нет
 * @param holdingBytes  байты, записанные но ещё не подтверждённые; держим на случай, если их
 *                      придётся отправить заново
 */
public record SessionSnapshot(
        String id,
        String remote,
        long upMillis,
        long lastPongMillis,
        int openStreams,
        long streamsCarried,
        long bytesToClient,
        long bytesFromClient,
        List<Integer> reversePorts,
        List<StreamInfo> streams,
        long resumes,
        int holdingBytes) {
}
