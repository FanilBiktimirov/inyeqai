package com.inyeqai.tunnel.server;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Проба живости — та же, что и проверки {@code /healthz} в остальной цепочке. */
@RestController
public class HealthController {

    @GetMapping("/healthz")
    public String healthz() {
        return "ok";
    }
}
