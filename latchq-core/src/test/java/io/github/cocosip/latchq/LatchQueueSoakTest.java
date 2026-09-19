package io.github.cocosip.latchq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.cocosip.latchq.chronicle.TinyRollCycle;
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
 * M5 milestone tests: a longer soak run with deliberate consumer stalls (gap creation and
 * recovery), a LatchQ-level hard-kill crash recovery matrix on a child JVM, and a dedicated
 * roll-boundary soak. The 1-second {@link TinyRollCycle} makes rolls frequent and deterministic.
 */
class LatchQueueSoakTest {

    private static final Path BASE = Path.of("target", "test-data", "latchq-soak");

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
                                        // locked left-over, ignore
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
                            c.setCompleteIntervalMillis(50);
                            c.setCheckpointIntervalMillis(50);
                            c.setBuilderCustomizer(
                                    b -> b.rollCycle(new TinyRollCycle()).epoch(epoch()));
                        })
                .build();
    }

    /** T5.1: writers + consumers, some consumers stall to create real gaps that later recover. */
    @Test
    void soakWithStallingConsumersRecoversAndNeverLosesOrDuplicates() throws Exception {
        purgeBase();
        try (LatchQueue<Event> queue = newQueue("soak")) {
            int writers = 3;
            int perWriter = 300;
            ExecutorService pool = Executors.newFixedThreadPool(writers + 4);
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
                                        if (i % 50 == 0) {
                                            Thread.sleep(30); // spread across roll boundaries
                                        }
                                    }
                                    return null;
                                }));
            }
            Set<Integer> consumed = ConcurrentHashMap.newKeySet();
            AtomicInteger processed = new AtomicInteger();
            for (int c = 0; c < 4; c++) {
                futures.add(
                        pool.submit(
                                () -> {
                                    start.await();
                                    var random = java.util.concurrent.ThreadLocalRandom.current();
                                    while (processed.get() < writers * perWriter) {
                                        var batch = queue.read(9, Duration.ofMillis(300));
                                        if (batch.isEmpty()) {
                                            continue;
                                        }
                                        // deliberate stall: this consumer holds a batch for a
                                        // while, creating
                                        // a gap ahead of faster consumers that must later recover
                                        if (random.nextInt(10) == 0) {
                                            Thread.sleep(random.nextLong(100, 400));
                                        }
                                        queue.commit(
                                                batch.stream().map(LogEntry::position).toList());
                                        batch.forEach(entry -> consumed.add(entry.data().id()));
                                        processed.addAndGet(batch.size());
                                    }
                                    return null;
                                }));
            }
            start.countDown();
            for (var future : futures) {
                future.get(120, TimeUnit.SECONDS);
            }
            pool.shutdown();

            assertThat(consumed).hasSize(writers * perWriter);
            // everything committed and merged after the stalls recovered
            await().atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> assertThat(queue.metrics().currentGapCount()).isZero());
            assertThat(queue.metrics().totalWriteCount()).isEqualTo(writers * perWriter);
        }
    }

    /** T5.3: roll-boundary soak - writes and reads spanning many 1-second cycles stay chained. */
    @Test
    void rollBoundarySoakKeepsPositionsChainedAcrossCycles() {
        purgeBase();
        try (LatchQueue<Event> queue = newQueue("rollsoak")) {
            int total = 40;
            List<Long> indexes = new ArrayList<>();
            for (int i = 0; i < total; i++) {
                indexes.add(queue.write(new Event(i, "r" + i)));
                try {
                    Thread.sleep(120); // guaranteed several roll boundaries over 40 writes
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            List<LogEntry<Event>> reads = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 30000;
            while (reads.size() < total && System.currentTimeMillis() < deadline) {
                queue.read(10, Duration.ofMillis(500)).forEach(reads::add);
            }
            assertThat(reads).hasSize(total);
            // every read position must never go backwards, even though the cycle component of
            // the index changes many times: chained entries have index == previous nextIndex,
            // idle-roll steps jump forward (the provisional gap the merge later consumes)
            long previousNext = -1;
            for (LogEntry<Event> read : reads) {
                assertThat(read.index()).isGreaterThanOrEqualTo(previousNext);
                previousNext = read.nextIndex();
            }
        }
    }

    /**
     * T5.2: hard-kill a child JVM (kill -9 equivalent) that wrote and committed 30 messages through
     * the LatchQ API. Reopening the same queue must not re-deliver the committed messages
     * (checkpoint survived the kill) and the queue must keep working.
     */
    @Test
    void hardKillAfterCommitDoesNotRedeliverCommittedMessages() throws Exception {
        purgeBase();
        Path root = BASE.resolve("kill-root").toAbsolutePath();
        Files.createDirectories(root);
        int normalMessages = 30;
        String javaBin =
                System.getProperty("java.home")
                        + java.io.File.separator
                        + "bin"
                        + java.io.File.separator
                        + (System.getProperty("os.name").toLowerCase().contains("win")
                                ? "java.exe"
                                : "java");
        Process child =
                new ProcessBuilder(
                                javaBin,
                                "--add-opens",
                                "java.base/java.lang=ALL-UNNAMED",
                                "--add-opens",
                                "java.base/java.lang.reflect=ALL-UNNAMED",
                                "--add-opens",
                                "java.base/java.io=ALL-UNNAMED",
                                "--add-opens",
                                "java.base/sun.nio.ch=ALL-UNNAMED",
                                "--add-exports",
                                "java.base/jdk.internal.ref=ALL-UNNAMED",
                                "-cp",
                                System.getProperty("java.class.path"),
                                "io.github.cocosip.latchq.chronicle.LatchQCrashChild",
                                root.toString(),
                                String.valueOf(normalMessages))
                        .redirectErrorStream(true)
                        .start();
        assertThat(child.getInputStream().readAllBytes())
                .asString()
                .contains("child: wrote and committed " + normalMessages);
        assertThat(child.waitFor(60, TimeUnit.SECONDS)).isTrue();
        assertThat(child.exitValue()).isEqualTo(7); // halt(7) after the checkpoint tick

        // reopen the SAME (type, fileName) directory the child used, with the identical
        // roll cycle, and verify the persisted checkpoint survived the kill
        try (LatchQueue<io.github.cocosip.latchq.chronicle.CrashPayload> reopened =
                LatchQueueBuilder.create(
                                "crash", io.github.cocosip.latchq.chronicle.CrashPayload.class)
                        .rootPath(root.toString())
                        .configuration(
                                c -> {
                                    c.setFileName("crash");
                                    c.setBuilderCustomizer(b -> b.rollCycle(new TinyRollCycle()));
                                })
                        .build()) {
            // committed messages were checkpointed before the kill: nothing is re-delivered
            assertThat(reopened.read(1, Duration.ofMillis(600))).isEmpty();

            // the queue keeps working after the crash
            for (int i = 0; i < 3; i++) {
                assertThat(reopened.write(new io.github.cocosip.latchq.chronicle.CrashPayload(i)))
                        .isPositive();
            }
            List<LogEntry<io.github.cocosip.latchq.chronicle.CrashPayload>> reads =
                    new ArrayList<>();
            long deadline = System.currentTimeMillis() + 5000;
            while (reads.size() < 3 && System.currentTimeMillis() < deadline) {
                reopened.read(3, Duration.ofMillis(500)).forEach(reads::add);
            }
            assertThat(reads).hasSize(3);
        }
    }
}
