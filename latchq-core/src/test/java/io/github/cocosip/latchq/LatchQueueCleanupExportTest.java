package io.github.cocosip.latchq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.cocosip.latchq.chronicle.TinyRollCycle;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * M3 integration tests: cycle-file cleanup behind the persisted checkpoint (never behind the
 * in-memory truncate index), safety of reads/restarts after cleanup, and the index-range JSONL
 * export with clamping, bounds and file splitting. Runs on the 1-second {@link TinyRollCycle}.
 */
class LatchQueueCleanupExportTest {

    private static final Path BASE = Path.of("target", "test-data", "latchq-cleanup-export");

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

    private static Path queueDirectory(String fileName) {
        return Path.of(BASE.toString(), Event.class.getName(), fileName);
    }

    private static List<Path> cycleFiles(String fileName) throws IOException {
        try (var stream = Files.list(queueDirectory(fileName))) {
            List<Path> files = new ArrayList<>();
            stream.filter(p -> p.getFileName().toString().endsWith(".cq4")).forEach(files::add);
            return files;
        }
    }

    private static LatchQueue<Event> newQueue(
            String name, long checkpointIntervalMillis, long cleanupIntervalMillis) {
        return LatchQueueBuilder.create(name, Event.class)
                .rootPath(BASE.toString())
                .configuration(
                        c -> {
                            c.setFileName(name);
                            c.setCheckpointIntervalMillis((int) checkpointIntervalMillis);
                            c.setCleanupIntervalMillis((int) cleanupIntervalMillis);
                            c.setBuilderCustomizer(
                                    b -> b.rollCycle(new TinyRollCycle()).epoch(epoch()));
                        })
                .build();
    }

    private static List<LogEntry<Event>> readAll(
            LatchQueue<Event> queue, int expected, long deadlineMillis) {
        List<LogEntry<Event>> all = new ArrayList<>();
        while (all.size() < expected && System.currentTimeMillis() < deadlineMillis) {
            queue.read(expected, Duration.ofMillis(500)).forEach(all::add);
        }
        return all;
    }

    @Test
    void cleanupDeletesConsumedCycleFilesBehindTheCheckpoint() throws Exception {
        purgeBase();
        try (LatchQueue<Event> queue = newQueue("cleanup", 100, 100)) {
            List<LogEntry<Event>> reads;
            for (int i = 0; i < 3; i++) {
                queue.write(new Event(i, "e" + i));
                if (i < 2) {
                    Thread.sleep(1100); // one message per cycle
                }
            }
            reads = readAll(queue, 3, System.currentTimeMillis() + 5000);
            assertThat(reads).hasSize(3);
            queue.commit(reads.stream().map(LogEntry::position).toList());
            long expected = reads.get(2).nextIndex();
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(
                            () ->
                                    assertThat(queue.metrics().truncateBeforeIndex())
                                            .isEqualTo(expected));

            // checkpoint (100 ms) and cleanup (100 ms) both converge: the two earlier cycle
            // files are deleted, the current cycle file survives
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(cycleFiles("cleanup")).hasSize(1));

            // the queue keeps working after cleanup
            queue.write(new Event(9, "after-cleanup"));
            List<LogEntry<Event>> after = queue.read(1, Duration.ofSeconds(2));
            assertThat(after).hasSize(1);
            assertThat(after.get(0).data().id()).isEqualTo(9);
        }
    }

    @Test
    void cleanupNeverRunsAheadOfThePersistedCheckpoint() throws Exception {
        purgeBase();
        // checkpoint interval far exceeds the test duration: the truncate index advances in
        // memory but nothing is persisted, so cleanup must not delete anything
        try (LatchQueue<Event> queue = newQueue("lagging", 60000, 100)) {
            for (int i = 0; i < 2; i++) {
                queue.write(new Event(i, "e" + i));
                if (i == 0) {
                    Thread.sleep(1100);
                }
            }
            List<LogEntry<Event>> reads = readAll(queue, 2, System.currentTimeMillis() + 5000);
            assertThat(reads).hasSize(2);
            queue.commit(reads.stream().map(LogEntry::position).toList());
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(
                            () ->
                                    assertThat(queue.metrics().truncateBeforeIndex())
                                            .isEqualTo(reads.get(1).nextIndex()));
            Thread.sleep(500); // several cleanup ticks with no persisted checkpoint

            assertThat(cycleFiles("lagging")).hasSize(2); // nothing deleted
        }
    }

    @Test
    void restartAfterCleanupDoesNotReprocessOrLoseCommittedData() throws Exception {
        purgeBase();
        try (LatchQueue<Event> queue = newQueue("safe", 100, 100)) {
            for (int i = 0; i < 3; i++) {
                queue.write(new Event(i, "e" + i));
                if (i < 2) {
                    Thread.sleep(1100);
                }
            }
            List<LogEntry<Event>> reads = readAll(queue, 3, System.currentTimeMillis() + 5000);
            assertThat(reads).hasSize(3);
            queue.commit(reads.stream().map(LogEntry::position).toList());
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(cycleFiles("safe")).hasSize(1));
        }
        try (LatchQueue<Event> reopened = newQueue("safe", 100, 100)) {
            // everything committed was checkpointed; nothing is re-delivered after restart.
            // firstIndex() is recomputed fresh here - Chronicle caches it within a session,
            // so the value observed right after an in-session cleanup is stale
            assertThat(reopened.firstIndex()).isGreaterThan(0);
            assertThat(reopened.read(1, Duration.ofMillis(400))).isEmpty();
        }
    }

    @Test
    void exportRoundTripsWithBoundsAndFileSplitting() throws Exception {
        purgeBase();
        Path exportDir = BASE.resolve("export-out");
        Path exportDir2 = BASE.resolve("export-out2");
        try (LatchQueue<Event> queue = newQueue("export", 60000, 60000)) {
            for (int i = 0; i < 10; i++) {
                queue.write(new Event(i, "e" + i));
            }
            List<LogEntry<Event>> reads = readAll(queue, 10, System.currentTimeMillis() + 5000);
            assertThat(reads).hasSize(10);

            ExportResult result = queue.export(exportDir.toString(), reads.get(0).index(), null, 4);
            assertThat(result.count()).isEqualTo(10);
            assertThat(result.filePaths()).hasSize(3); // 4 + 4 + 2
            assertThat(result.fromIndex()).isEqualTo(reads.get(0).index());

            // every exported line parses back to the original event, in order
            List<Event> exported = new ArrayList<>();
            for (String path : result.filePaths()) {
                for (String line : Files.readAllLines(Path.of(path), StandardCharsets.UTF_8)) {
                    if (!line.isBlank()) {
                        String[] parts = line.split(",");
                        exported.add(
                                new Event(Integer.parseInt(parts[0].replaceAll("\\D", "")), "e"));
                    }
                }
            }
            assertThat(exported)
                    .extracting(Event::id)
                    .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

            // bounded export: only the first five entries
            ExportResult bounded =
                    queue.export(
                            exportDir.toString(),
                            reads.get(0).index(),
                            reads.get(4).nextIndex(),
                            10);
            assertThat(bounded.count()).isEqualTo(5);

            // incremental export continues from the previous exclusive end
            queue.write(new Event(10, "e10"));
            queue.write(new Event(11, "e11"));
            ExportResult incremental =
                    queue.export(exportDir2.toString(), result.toIndex(), null, 10);
            assertThat(incremental.count()).isEqualTo(2);
        }
    }

    @Test
    void exportFromIndexBelowTheEarliestSurvivingMessageIsClamped() {
        purgeBase();
        try (LatchQueue<Event> queue = newQueue("clamp", 60000, 60000)) {
            for (int i = 0; i < 5; i++) {
                queue.write(new Event(i, "e" + i));
            }
            long first = queue.firstIndex();
            ExportResult result =
                    queue.export(BASE.resolve("export-clamp").toString(), first - 1000, null, 10);
            assertThat(result.fromIndex()).isEqualTo(first);
            assertThat(result.count()).isEqualTo(5);
        }
    }

    @Test
    void exportEmptyRangeProducesAnEmptyResult() {
        purgeBase();
        try (LatchQueue<Event> queue = newQueue("emptyrange", 60000, 60000)) {
            for (int i = 0; i < 3; i++) {
                queue.write(new Event(i, "e" + i));
            }
            long first = queue.firstIndex();
            ExportResult result =
                    queue.export(BASE.resolve("export-empty").toString(), first + 1000, null, 10);
            assertThat(result.count()).isZero();
            assertThat(result.filePaths()).isEmpty();
            assertThat(result.fromIndex()).isEqualTo(result.toIndex());
        }
    }
}
