package io.github.cocosip.latchq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.latchq.chronicle.TinyRollCycle;
import io.github.cocosip.latchq.config.LatchQueueOptions;
import io.github.cocosip.latchq.exception.LatchQException;
import io.github.cocosip.latchq.exception.LatchQInvalidConfigurationException;
import io.github.cocosip.latchq.exception.LatchQNotInitializedException;
import io.github.cocosip.latchq.internal.DefaultLatchQueue;
import io.github.cocosip.latchq.internal.DefaultLatchQueueFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * M1 integration tests: write/read round trip with chained positions across roll boundaries,
 * concurrent writers and consumers, catch-up tail delivery, shutdown guards, factory caching and
 * reopen behaviour. Runs on the 1-second {@link TinyRollCycle} so roll boundaries are hit
 * deterministically.
 */
class LatchQueueIntegrationTest {

    private static final Path BASE = Path.of("target", "test-data", "latchq");

    record Event(int id, String text) {}

    private static long epoch() {
        return LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    private static void purgeBase() {
        if (Files.exists(BASE)) {
            try {
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
            } catch (IOException ignored) {
                // ignore
            }
        }
    }

    private static LatchQueue<Event> newQueue(String name) {
        return LatchQueueBuilder.create(name, Event.class)
                .rootPath(BASE.toString())
                .configuration(
                        c -> {
                            c.setFileName(name);
                            // the 1-second test roll cycle crosses boundaries deterministically
                            c.setBuilderCustomizer(
                                    b -> b.rollCycle(new TinyRollCycle()).epoch(epoch()));
                        })
                .build();
    }

    private static List<Event> readAllAvailable(
            LatchQueue<Event> queue, int expected, long deadlineMillis) {
        List<Event> all = new ArrayList<>();
        while (all.size() < expected && System.currentTimeMillis() < deadlineMillis) {
            queue.read(16, Duration.ofMillis(500)).forEach(entry -> all.add(entry.data()));
        }
        return all;
    }

    @Test
    void writeThenReadAllDeliversEveryMessageWithChainedPositions() {
        purgeBase();
        try (LatchQueue<Event> queue = newQueue("chain")) {
            int total = 30;
            for (int i = 0; i < total; i++) {
                long index = queue.write(new Event(i, "e" + i));
                assertThat(index).isPositive();
                try {
                    Thread.sleep(60); // spread writes across roll boundaries
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            List<LogEntry<Event>> reads = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 20000;
            while (reads.size() < total && System.currentTimeMillis() < deadline) {
                reads.addAll(queue.read(10, Duration.ofSeconds(2)));
            }
            assertThat(reads).hasSize(total);
            int provisionalGaps = 0;
            for (int i = 0; i < total; i++) {
                assertThat(reads.get(i).data().id()).isEqualTo(i);
                assertThat(reads.get(i).index()).isPositive();
                assertThat(reads.get(i).nextIndex()).isGreaterThan(reads.get(i).index());
                if (i > 0) {
                    // The chain is numerically unbroken within a cycle. Across an idle-roll
                    // boundary the previous tail was delivered with an in-cycle provisional
                    // nextIndex (monotone but not equal); the M2 merge consumes the recorded
                    // empty-gap correction for that step, so only monotonicity is asserted here.
                    if (reads.get(i - 1).nextIndex() != reads.get(i).index()) {
                        assertThat(reads.get(i - 1).nextIndex()).isLessThan(reads.get(i).index());
                        provisionalGaps++;
                    }
                }
            }
            System.out.println("[chain] provisional gap steps: " + provisionalGaps);
            LatchQueueMetrics metrics = queue.metrics();
            assertThat(metrics.totalWriteCount()).isEqualTo(total);
            assertThat(metrics.totalReadCount()).isEqualTo(total);
        }
    }

    @Test
    void singleMessageBecomesReadableWhileProducerIsIdle() {
        purgeBase();
        try (LatchQueue<Event> queue = newQueue("single")) {
            queue.write(new Event(1, "only"));
            List<LogEntry<Event>> reads = queue.read(5, Duration.ofSeconds(2));
            // catch-up tail delivery: the last written message must not be starved by an idle
            // producer
            assertThat(reads).hasSize(1);
            assertThat(reads.get(0).data().id()).isEqualTo(1);
            assertThat(reads.get(0).nextIndex()).isGreaterThan(reads.get(0).index());
        }
    }

    @Test
    void concurrentWritersAndConsumersDeliverEveryMessageExactlyOnce() throws Exception {
        purgeBase();
        try (LatchQueue<Event> queue = newQueue("concurrent")) {
            int writers = 4;
            int perWriter = 100;
            ExecutorService pool = Executors.newFixedThreadPool(writers + 3);
            CountDownLatch start = new CountDownLatch(1);
            List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
            for (int w = 0; w < writers; w++) {
                final int base = w * perWriter;
                futures.add(
                        pool.submit(
                                () -> {
                                    start.await();
                                    for (int i = 0; i < perWriter; i++) {
                                        queue.write(new Event(base + i, "w" + base + "-" + i));
                                    }
                                    return null;
                                }));
            }
            Set<Integer> consumedIds = ConcurrentHashMap.newKeySet();
            List<List<Long>> batchChains =
                    java.util.Collections.synchronizedList(new ArrayList<>());
            AtomicInteger consumed = new AtomicInteger();
            long deadline = System.currentTimeMillis() + 30000;
            for (int c = 0; c < 3; c++) {
                futures.add(
                        pool.submit(
                                () -> {
                                    start.await();
                                    while (consumed.get() < writers * perWriter
                                            && System.currentTimeMillis() < deadline) {
                                        LogEntryList<Event> batch =
                                                queue.read(7, Duration.ofMillis(300));
                                        if (batch.isEmpty()) {
                                            continue;
                                        }
                                        for (int i = 0; i < batch.size(); i++) {
                                            if (i > 0
                                                    && batch.get(i - 1).nextIndex()
                                                            != batch.get(i).index()) {
                                                // across an idle-roll boundary the provisional tail
                                                // nextIndex is
                                                // monotone but not equal; the M2 merge consumes the
                                                // correction
                                                assertThat(batch.get(i - 1).nextIndex())
                                                        .isLessThan(batch.get(i).index());
                                            }
                                            consumedIds.add(batch.get(i).data().id());
                                            consumed.incrementAndGet();
                                        }
                                    }
                                    return null;
                                }));
            }
            start.countDown();
            for (java.util.concurrent.Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
            pool.shutdown();
            assertThat(consumedIds).hasSize(writers * perWriter);
            assertThat(consumed.get()).isEqualTo(writers * perWriter);
        }
    }

    @Test
    void largeMessageWritesWithRaisedMaxMessageSize() {
        purgeBase();
        // default cap is 16 MiB per message; raising maxMessageSizeBytes admits bigger entries
        // (the library derives the Chronicle block size, usable write space = blockSize/2 - 4)
        try (LatchQueue<Event> queue =
                LatchQueueBuilder.create("bigmessage", Event.class)
                        .rootPath(BASE.toString())
                        .configuration(
                                c -> {
                                    c.setFileName("bigmessage");
                                    c.setMaxMessageSizeBytes(64L * 1024 * 1024);
                                    c.setBuilderCustomizer(
                                            b -> b.rollCycle(new TinyRollCycle()).epoch(epoch()));
                                })
                        .build()) {
            byte[] pad = new byte[32 * 1024 * 1024];
            java.util.Arrays.fill(pad, (byte) 'z');
            Event big = new Event(1, new String(pad, java.nio.charset.StandardCharsets.ISO_8859_1));
            assertThat(queue.write(big)).isPositive();
            List<LogEntry<Event>> reads = queue.read(1, Duration.ofSeconds(5));
            assertThat(reads).hasSize(1);
            assertThat(reads.get(0).data().text().length()).isEqualTo(big.text().length());
        }
    }

    @Test
    void oversizedMessageIsRejectedWithClearErrorBeforeTouchingStorage() {
        purgeBase();
        try (LatchQueue<Event> queue =
                LatchQueueBuilder.create("smallcap", Event.class)
                        .rootPath(BASE.toString())
                        .configuration(
                                c -> {
                                    c.setFileName("smallcap");
                                    c.setMaxMessageSizeBytes(1024);
                                    c.setBuilderCustomizer(
                                            b -> b.rollCycle(new TinyRollCycle()).epoch(epoch()));
                                })
                        .build()) {
            Event tooBig = new Event(1, "y".repeat(2048));
            assertThatThrownBy(() -> queue.write(tooBig))
                    .isInstanceOf(LatchQException.class)
                    .hasMessageContaining("maxMessageSizeBytes");
            // the rejected write left no scan-able data behind
            assertThat(queue.read(1, Duration.ofMillis(300))).isEmpty();
        }
    }

    @Test
    void readWithTimeoutOnEmptyQueueReturnsEmpty() {
        purgeBase();
        try (LatchQueue<Event> queue = newQueue("empty")) {
            long start = System.currentTimeMillis();
            LogEntryList<Event> reads = queue.read(5, Duration.ofMillis(300));
            assertThat(reads).isEmpty();
            assertThat(System.currentTimeMillis() - start).isGreaterThanOrEqualTo(250);
        }
    }

    @Test
    void uninitializedQueueRejectsUseAndInitializeFixesIt() {
        purgeBase();
        LatchQueueOptions options = new LatchQueueOptions();
        options.setRootPath(BASE.toString());
        options.configure("raw", c -> c.setFileName("raw"));
        DefaultLatchQueue<Event> queue =
                new DefaultLatchQueue<>(
                        "raw", Event.class, options, options.getConfiguration("raw"));
        assertThatThrownBy(() -> queue.write(new Event(1, "x")))
                .isInstanceOf(LatchQNotInitializedException.class);
        assertThat(queue.isInitialized()).isFalse();
        queue.initialize();
        assertThat(queue.isInitialized()).isTrue();
        assertThat(queue.write(new Event(1, "x"))).isPositive();
        queue.close();
    }

    @Test
    void metricsIsCallableInAnyLifecycleState() {
        purgeBase();
        LatchQueueOptions options = new LatchQueueOptions();
        options.setRootPath(BASE.toString());
        options.configure("metrics-raw", c -> c.setFileName("metrics-raw"));
        DefaultLatchQueue<Event> queue =
                new DefaultLatchQueue<>(
                        "metrics-raw",
                        Event.class,
                        options,
                        options.getConfiguration("metrics-raw"));
        // before initialize(): the storage fields read as -1 instead of throwing, even inside
        // the window where initialize() has built the queue but not flipped the flag
        LatchQueueMetrics before = queue.metrics();
        assertThat(before.truncateBeforeIndex()).isNegative();
        assertThat(before.firstIndex()).isNegative();
        assertThat(before.lastIndexAppended()).isNegative();
        queue.initialize();
        assertThat(queue.metrics().firstIndex()).isNegative(); // empty queue
        queue.write(new Event(1, "x"));
        assertThat(queue.metrics().lastIndexAppended()).isPositive();
        queue.close();
        assertThat(queue.metrics().lastIndexAppended()).isNegative();
    }

    @Test
    void closedQueueRejectsFurtherUseAndCloseIsIdempotent() {
        purgeBase();
        LatchQueue<Event> queue = newQueue("closed");
        queue.write(new Event(1, "x"));
        queue.close();
        queue.close(); // idempotent
        assertThatThrownBy(() -> queue.write(new Event(2, "y")))
                .isInstanceOf(LatchQException.class);
        assertThatThrownBy(() -> queue.read(1, Duration.ofMillis(10)))
                .isInstanceOf(LatchQException.class);
        // metrics stays readable after close
        assertThat(queue.metrics()).isNotNull();
    }

    @Test
    void reopenRereadsFromTheEarliestMessageUntilCheckpointingExists() {
        purgeBase();
        try (LatchQueue<Event> first = newQueue("reopen")) {
            for (int i = 0; i < 5; i++) {
                first.write(new Event(i, "r" + i));
            }
        }
        // M1 restores from the earliest message; M2 replaces this with the persisted checkpoint.
        try (LatchQueue<Event> second = newQueue("reopen")) {
            List<Event> reads = readAllAvailable(second, 5, System.currentTimeMillis() + 10000);
            assertThat(reads).hasSize(5);
            assertThat(reads.get(4).id()).isEqualTo(4);
        }
    }

    @Test
    void factoryCachesPerNameAndTypeAndAutoInitializes() {
        purgeBase();
        LatchQueueOptions options = new LatchQueueOptions();
        options.setRootPath(BASE.toString());
        options.configure("f-events", c -> c.setFileName("f-events"));
        try (DefaultLatchQueueFactory factory = new DefaultLatchQueueFactory(options)) {
            LatchQueue<Event> first = factory.getOrCreate("f-events", Event.class);
            LatchQueue<Event> second = factory.getOrCreate("f-events", Event.class);
            assertThat(first).isSameAs(second);
            assertThat(first.isInitialized()).isTrue();
            first.write(new Event(1, "via-factory"));
            assertThat(first.read(1, Duration.ofSeconds(2))).hasSize(1);
            // unknown queue name fails fast instead of falling back to a default config
            assertThatThrownBy(() -> factory.getOrCreate("nope", Event.class))
                    .isInstanceOf(LatchQInvalidConfigurationException.class)
                    .hasMessageContaining("nope");
        }
        // factory.close() closed the cached queues
        LatchQueue<Event> afterClose =
                LatchQueueBuilder.create("f-events-closed-check", Event.class)
                        .rootPath(BASE.toString())
                        .configuration(c -> c.setFileName("f-events-closed-check"))
                        .build();
        assertThat(afterClose.isInitialized()).isTrue();
        afterClose.close();
    }
}
