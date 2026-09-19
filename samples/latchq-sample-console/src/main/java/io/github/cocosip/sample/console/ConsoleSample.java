package io.github.cocosip.sample.console;

import io.github.cocosip.latchq.LatchQueue;
import io.github.cocosip.latchq.LatchQueueBuilder;
import io.github.cocosip.latchq.LogEntry;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Plain-Java example: one producer writes orders, three consumers process them concurrently and
 * commit out of order. Run: mvnw -pl samples/latchq-sample-console compile exec:java
 * -Dexec.mainClass=io.github.cocosip.sample.console.ConsoleSample
 */
public final class ConsoleSample {

    /** Example payload; business processing must be idempotent (see the LatchQ docs). */
    public record Order(long orderId, String item) {}

    private static final int TOTAL = 100;

    private final LatchQueue<Order> queue;
    private final AtomicInteger consumed = new AtomicInteger();

    private ConsoleSample(LatchQueue<Order> queue) {
        this.queue = queue;
    }

    public static void main(String[] args) throws Exception {
        // manual assembly without any DI framework
        LatchQueue<Order> queue =
                LatchQueueBuilder.create("orders", Order.class)
                        .rootPath("target/sample-data")
                        .configuration(c -> c.setFileName("orders"))
                        .build();
        ConsoleSample sample = new ConsoleSample(queue);

        ExecutorService consumers = Executors.newFixedThreadPool(3);
        for (int i = 0; i < 3; i++) {
            consumers.submit(sample::consumeLoop);
        }

        for (int i = 0; i < TOTAL; i++) {
            queue.write(new Order(i, "item-" + i));
        }
        while (sample.consumed.get() < TOTAL) {
            Thread.sleep(50);
        }

        consumers.shutdown();
        consumers.awaitTermination(10, TimeUnit.SECONDS);
        System.out.println("final metrics: " + queue.metrics());
        queue.close();
    }

    private void consumeLoop() {
        while (consumed.get() < TOTAL) {
            var batch = queue.read(10, Duration.ofMillis(500));
            if (batch.isEmpty()) {
                continue;
            }
            // process each entry (idempotently in production!), then commit its position
            for (LogEntry<Order> entry : batch) {
                System.out.println(
                        Thread.currentThread().threadId() + " processed " + entry.data());
            }
            queue.commit(batch.stream().map(LogEntry::position).toList());
            consumed.addAndGet(batch.size());
        }
    }
}
