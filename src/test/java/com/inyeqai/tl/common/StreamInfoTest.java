package com.inyeqai.tl.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class StreamInfoTest {

    @Test
    void clientIdsReadAsThemselves() {
        assertEquals("1", new StreamInfo(1, "h:1", 0, 0, 0).label());
        assertEquals("42", new StreamInfo(42, "h:1", 0, 0, 0).label());
    }

    @Test
    void serverIdsReadUnsignedRatherThanNegative() {
        // Сервер ставит старший бит у каждого выданного id, поэтому как знаковый int первый
        // из них равен -2147483647. Логи на обоих концах должны показывать один и тот же токен,
        // а минус перед id потока читается как ошибка.
        int firstServerId = 1 | 0x80000000;
        assertTrue(firstServerId < 0, "sanity: the raw id really is negative");
        assertEquals("2147483649", new StreamInfo(firstServerId, "h:1", 0, 0, 0).label());
        assertEquals("4294967295", new StreamInfo(-1, "h:1", 0, 0, 0).label());
    }
}
