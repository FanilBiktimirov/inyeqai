package com.laptopkit.tunnel.common;

/**
 * Один живой поток — для логов и экрана статуса.
 *
 * @param id        id потока на проводе, ровно такой, каким его видят обе стороны
 * @param dst       куда он идёт, в виде {@code host:port}
 * @param ageMillis сколько он уже открыт
 * @param sent      сколько байт эта сторона положила в туннель для него
 * @param received  сколько байт эта сторона вынула из туннеля и записала в его сокет
 */
public record StreamInfo(int id, String dst, long ageMillis, long sent, long received) {

    /**
     * Id в том виде, в каком его должен читать человек. Id потоков несут инициатора в старшем
     * бите, поэтому каждый id, выданный сервером, — отрицательный {@code int}; печать без знака
     * даёт на обоих концах туннеля один и тот же токен, без обманчивого минуса.
     */
    public String label() {
        return Integer.toUnsignedString(id);
    }
}
