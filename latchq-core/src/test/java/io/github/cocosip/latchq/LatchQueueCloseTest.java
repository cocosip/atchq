package io.github.cocosip.latchq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.cocosip.latchq.exception.LatchQException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Close contract: consumers blocked in a read (with or without timeout) are woken by {@code
 * close()} and fail with a clear closed error instead of hanging until interrupted.
 */
class LatchQueueCloseTest {

    private static final Path BASE = Path.of("target", "test-data", "latchq-close");

    record Event(int id) {}

    @BeforeEach
    @AfterEach
    void purge() throws IOException {
        if (Files.exists(BASE)) {
            Files.walk(BASE)
                    .sorted(Comparator.reverseOrder())
                    .forEach(
                            p -> {
                                try {
                                    Files.delete(p);
                                } catch (IOException ignored) {
                                    // locked left-over from a previous run, ignore
                                }
                            });
        }
    }

    private static LatchQueue<Event> newQueue() {
        return LatchQueueBuilder.create("close", Event.class)
                .rootPath(BASE.toString())
                .configuration(c -> c.setFileName("close"))
                .build();
    }

    @Test
    void closeWakesConsumerBlockedInBlockingRead() throws Exception {
        LatchQueue<Event> queue = newQueue();
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread consumer =
                Thread.ofPlatform()
                        .start(
                                () -> {
                                    try {
                                        queue.read(10);
                                    } catch (Throwable t) {
                                        thrown.set(t);
                                    }
                                });
        await().atMost(Duration.ofSeconds(5))
                .until(() -> consumer.getState() == Thread.State.WAITING);

        queue.close();

        consumer.join(5000);
        assertThat(consumer.isAlive()).isFalse();
        assertThat(thrown.get()).isInstanceOf(LatchQException.class).hasMessageContaining("closed");
    }

    @Test
    void closeWakesConsumerBlockedInTimedRead() throws Exception {
        LatchQueue<Event> queue = newQueue();
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread consumer =
                Thread.ofPlatform()
                        .start(
                                () -> {
                                    try {
                                        // far beyond the test duration: only the close wake can
                                        // end this read early
                                        queue.read(10, Duration.ofSeconds(60));
                                    } catch (Throwable t) {
                                        thrown.set(t);
                                    }
                                });
        await().atMost(Duration.ofSeconds(5))
                .until(() -> consumer.getState() == Thread.State.TIMED_WAITING);

        queue.close();

        consumer.join(5000);
        assertThat(consumer.isAlive()).isFalse();
        assertThat(thrown.get()).isInstanceOf(LatchQException.class).hasMessageContaining("closed");
    }

    @Test
    void closeCompletesWhenHandOffQueueIsFullAndNobodyDrains() {
        LatchQueue<Event> queue =
                LatchQueueBuilder.create("close-full", Event.class)
                        .rootPath(BASE.toString())
                        .configuration(
                                c -> {
                                    c.setFileName("close-full");
                                    c.setPreReadCapacity(4);
                                })
                        .build();
        for (int i = 0; i < 20; i++) {
            queue.write(new Event(i));
        }
        // no consumer ever drains the hand-off queue: the wake marker cannot be enqueued, and
        // close() must give up after its bounded grace period instead of hanging
        long start = System.nanoTime();
        queue.close();
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(10));
    }
}
