package io.github.cocosip.sample.springboot;

import io.github.cocosip.latchq.LatchQueue;
import io.github.cocosip.latchq.LatchQueueFactory;
import io.github.cocosip.latchq.LogEntry;
import io.github.cocosip.latchq.exception.LatchQDeserializationException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
            List<LogEntry<AuditEvent>> batch;
            try {
                batch = queue.read(20, Duration.ofMillis(500));
            } catch (LatchQDeserializationException e) {
                // poison message: the entries read before it travel inside the exception -
                // process and commit them first, then abandon the poison entry explicitly,
                // otherwise it is re-delivered after a restart (see the README on payload drift)
                processAndCommit(e.getSuccessfullyRead());
                LOG.error(
                        "poison message at [{}, {}) - skipping it via forceCommitGap",
                        e.getIndex(),
                        e.getNextIndex(),
                        e);
                queue.forceCommitGap(e.getIndex(), e.getNextIndex());
                continue;
            }
            processAndCommit(batch);
        }
    }

    private void processAndCommit(List<LogEntry<AuditEvent>> entries) {
        if (entries.isEmpty()) {
            return;
        }
        for (LogEntry<AuditEvent> entry : entries) {
            LOG.info("processed {}", entry.data()); // idempotent processing in production
        }
        queue.commit(entries.stream().map(LogEntry::position).toList());
    }

    @PreDestroy
    void stop() {
        running.set(false);
        workers.shutdown();
        try {
            // let the in-flight batch finish processing and committing before the factory
            // destroy method closes the queues
            if (!workers.awaitTermination(5, TimeUnit.SECONDS)) {
                workers.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            workers.shutdownNow();
        }
        // queues (and the final checkpoint) are closed by the factory's destroy method
    }
}
