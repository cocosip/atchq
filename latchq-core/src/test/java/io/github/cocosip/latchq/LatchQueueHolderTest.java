package io.github.cocosip.latchq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.latchq.config.LatchQueueOptions;
import io.github.cocosip.latchq.exception.LatchQException;
import io.github.cocosip.latchq.exception.LatchQNotInitializedException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Static holder contract: fail-fast before init and on double init, shared (name, type) caching
 * across threads, and reset closing the underlying factory and allowing re-init.
 */
class LatchQueueHolderTest {

    private static final Path BASE = Path.of("target", "test-data", "latchq-holder");

    record Event(int id) {}

    @BeforeEach
    @AfterEach
    void resetHolderAndPurge() throws IOException {
        LatchQueueHolder.reset();
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

    private static LatchQueueOptions options(String fileName) {
        LatchQueueOptions options = new LatchQueueOptions();
        options.setRootPath(BASE.resolve(fileName).toString());
        options.configure("events", c -> c.setFileName(fileName));
        return options;
    }

    @Test
    void accessBeforeInitFailsFast() {
        assertThat(LatchQueueHolder.isInitialized()).isFalse();
        assertThatThrownBy(LatchQueueHolder::getInstance)
                .isInstanceOf(LatchQNotInitializedException.class);
        assertThatThrownBy(() -> LatchQueueHolder.getOrCreate("events", Event.class))
                .isInstanceOf(LatchQNotInitializedException.class);
    }

    @Test
    void doubleInitFailsFast() {
        LatchQueueHolder.init(options("double-init"));
        assertThatThrownBy(() -> LatchQueueHolder.init(o -> o.setRootPath("whatever")))
                .isInstanceOf(LatchQException.class)
                .hasMessageContaining("already initialized");
    }

    @Test
    void initCachesQueuesPerNameAndType() {
        LatchQueueHolder.init(options("cache"));
        assertThat(LatchQueueHolder.isInitialized()).isTrue();

        LatchQueue<Event> first = LatchQueueHolder.getOrCreate("events", Event.class);
        LatchQueue<Event> second = LatchQueueHolder.getOrCreate("events", Event.class);
        assertThat(second).isSameAs(first);
        assertThat(first.isInitialized()).isTrue();
        assertThat(first.write(new Event(1))).isPositive();

        // getInstance exposes the same factory, so every class sees one shared instance
        assertThat(LatchQueueHolder.getInstance())
                .extracting(f -> f.getOrCreate("events", Event.class))
                .isSameAs(first);
    }

    @Test
    void concurrentFirstAccessReturnsOneSharedInstance() throws Exception {
        LatchQueueHolder.init(options("concurrent"));
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        AtomicReference<LatchQueue<Event>> reference = new AtomicReference<>();
        try {
            for (int i = 0; i < threads; i++) {
                pool.submit(
                        () -> {
                            ready.countDown();
                            go.await();
                            LatchQueue<Event> queue =
                                    LatchQueueHolder.getOrCreate("events", Event.class);
                            reference.compareAndSet(null, queue);
                            assertThat(queue).isSameAs(reference.get());
                            return null;
                        });
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            go.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        } finally {
            go.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void resetClosesFactoryAndAllowsReInit() {
        LatchQueueHolder.init(options("reset"));
        LatchQueue<Event> queue = LatchQueueHolder.getOrCreate("events", Event.class);
        queue.write(new Event(1));

        LatchQueueHolder.reset();

        assertThat(LatchQueueHolder.isInitialized()).isFalse();
        // the stale reference is a closed queue: guarded as a state error, per existing semantics
        assertThatThrownBy(() -> queue.write(new Event(2))).isInstanceOf(LatchQException.class);
        assertThatThrownBy(() -> LatchQueueHolder.getOrCreate("events", Event.class))
                .isInstanceOf(LatchQNotInitializedException.class);
        assertThatThrownBy(() -> LatchQueueHolder.getInstance().getOrCreate("events", Event.class))
                .isInstanceOf(LatchQNotInitializedException.class);

        // reset is idempotent and a fresh init starts a clean lifecycle
        LatchQueueHolder.reset();
        LatchQueueHolder.init(options("reset"));
        assertThat(LatchQueueHolder.getOrCreate("events", Event.class).write(new Event(3)))
                .isPositive();
    }
}
