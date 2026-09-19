package io.github.cocosip.latchq.internal;

import io.github.cocosip.latchq.LatchQueue;
import io.github.cocosip.latchq.LatchQueueFactory;
import io.github.cocosip.latchq.config.LatchQueueConfiguration;
import io.github.cocosip.latchq.config.LatchQueueOptions;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default factory: caches one queue per (payload type, name) pair and auto-initializes it on first
 * use, mirroring the FASTER reference factory. A failed creation is not cached, so a transient
 * problem can be retried. Closing the factory closes every created queue.
 */
public final class DefaultLatchQueueFactory implements LatchQueueFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultLatchQueueFactory.class);

    private final LatchQueueOptions options;
    private final ConcurrentHashMap<CacheKey, LatchQueue<?>> queues = new ConcurrentHashMap<>();
    private volatile boolean closed;

    public DefaultLatchQueueFactory(LatchQueueOptions options) {
        Objects.requireNonNull(options, "options");
        options.validate();
        this.options = options;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> LatchQueue<T> getOrCreate(String name, Class<T> type) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(type, "type");
        if (closed) {
            throw new io.github.cocosip.latchq.exception.LatchQException(
                    "the latchq factory is closed and cannot create new queues");
        }
        options.validate();
        LatchQueue<?> queue =
                queues.computeIfAbsent(
                        new CacheKey(type, name),
                        key ->
                                createAndInitialize(
                                        name, key.type(), options.getConfiguration(name)));
        // Cast is safe because the cache key pins the exact payload type.
        return (LatchQueue<T>) queue;
    }

    private <T> DefaultLatchQueue<T> createAndInitialize(
            String name, Class<T> type, LatchQueueConfiguration configuration) {
        DefaultLatchQueue<T> created = new DefaultLatchQueue<>(name, type, options, configuration);
        created.initialize();
        return created;
    }

    @Override
    public void close() {
        closed = true;
        queues.values()
                .forEach(
                        queue -> {
                            try {
                                queue.close();
                            } catch (RuntimeException e) {
                                LOG.error("failed to close queue during factory shutdown", e);
                            }
                        });
        queues.clear();
    }

    private record CacheKey(Class<?> type, String name) {}
}
