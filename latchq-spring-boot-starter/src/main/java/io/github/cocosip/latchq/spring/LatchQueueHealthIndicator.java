package io.github.cocosip.latchq.spring;

import io.github.cocosip.latchq.LatchQueueFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Reports {@code UP} while the shared {@link LatchQueueFactory} is open and {@code DOWN} once it
 * has been closed (or when a custom factory implementation reports itself inactive). Wired
 * automatically when Spring Boot Actuator is present; disable by defining your own {@code
 * latchQueueHealthIndicator} bean.
 */
public class LatchQueueHealthIndicator implements HealthIndicator {

    private final LatchQueueFactory factory;

    public LatchQueueHealthIndicator(LatchQueueFactory factory) {
        this.factory = factory;
    }

    @Override
    public Health health() {
        return factory.isActive()
                ? Health.up().build()
                : Health.down().withDetail("reason", "latchq factory is closed").build();
    }
}
