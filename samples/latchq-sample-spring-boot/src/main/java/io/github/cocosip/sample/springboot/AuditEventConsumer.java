package io.github.cocosip.sample.springboot;

import io.github.cocosip.latchq.LatchQueue;
import io.github.cocosip.latchq.LatchQueueFactory;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Consumes audit events concurrently and commits out of order. In production the processing must be
 * idempotent (see the LatchQ docs on gap handling).
 */
@Service
public class AuditEventConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(AuditEventConsumer.class);

    /** Example payload type; the queue is keyed by (name, type). */
    public record AuditEvent(long id, String action) {}

    private final LatchQueueFactory factory;
    private final LatchQueue<AuditEvent> queue;
    private final ExecutorService workers = Executors.newFixedThreadPool(2);
    private final AtomicBoolean running = new AtomicBoolean(true);

    public AuditEventConsumer(LatchQueueFactory factory) {
        this.factory = factory;
        this.queue = factory.getOrCreate("audit", AuditEvent.class);
    }

    @PostConstruct
    void start() {
        for (int i = 0; i < 2; i++) {
            workers.submit(this::consumeLoop);
        }
    }

    /** Publishes an event through the queue; called by the controller. */
    public long publish(AuditEvent event) {
        return queue.write(event);
    }

    /** Streams the queue contents as JSONL for front-end download; business-owned API layer. */
    public io.github.cocosip.latchq.ExportResult exportAll(String targetDirectory) {
        return queue.export(targetDirectory, 0L, null, 10000);
    }

    public io.github.cocosip.latchq.LatchQueueMetrics metrics() {
        return queue.metrics();
    }

    private void consumeLoop() {
        while (running.get()) {
            var batch = queue.read(20, Duration.ofMillis(500));
            if (batch.isEmpty()) {
                continue;
            }
            for (var entry : batch) {
                LOG.info("processed {}", entry.data()); // idempotent processing in production
            }
            queue.commit(batch.stream().map(io.github.cocosip.latchq.LogEntry::position).toList());
        }
    }

    @PreDestroy
    void stop() {
        running.set(false);
        workers.shutdown();
        // queues (and the final checkpoint) are closed by the factory's destroy method
    }
}
