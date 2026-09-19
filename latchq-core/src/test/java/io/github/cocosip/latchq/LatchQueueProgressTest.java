package io.github.cocosip.latchq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.cocosip.latchq.chronicle.TinyRollCycle;
import io.github.cocosip.latchq.internal.CheckpointStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * M2 integration tests: out-of-order commits, interval merging, gap warning/force-skip, manual gap
 * filling, atomic checkpoint persistence and restart recovery (including the provisional-truncate
 * and empty-gap-correction cases). Runs on the 1-second {@link TinyRollCycle} with fast background
 * intervals so the async behaviour converges quickly.
 */
class LatchQueueProgressTest {

    private static final Path BASE = Path.of("target", "test-data", "latchq-progress");

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

    private static LatchQueue<Event> newQueue(String name, int forceCompleteGapTimeoutMillis) {
        return LatchQueueBuilder.create(name, Event.class)
                .rootPath(BASE.toString())
                .configuration(
                        c -> {
                            c.setFileName(name);
                            c.setCompleteIntervalMillis(50);
                            c.setCheckpointIntervalMillis(50);
                            c.setForceCompleteGapTimeoutMillis(forceCompleteGapTimeoutMillis);
                            c.setBuilderCustomizer(
                                    b -> b.rollCycle(new TinyRollCycle()).epoch(epoch()));
                        })
                .build();
    }

    private static Path queueDirectory(String fileName) {
        return Path.of(BASE.toString(), Event.class.getName(), fileName);
    }

    @Test
    void commitThenMergeAdvancesTruncate() {
        purgeBase();
        try (LatchQueue<Event> queue = newQueue("merge", 60000)) {
            List<LogEntry<Event>> reads = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                queue.write(new Event(i, "e" + i));
            }
            reads.addAll(queue.read(5, Duration.ofSeconds(2)));
            assertThat(reads).hasSize(5);
            queue.commit(
                    java.util.List.of(
                            reads.get(4)
                                    .position())); // only the last one: everything before is a gap

            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(
                            () -> assertThat(queue.metrics().completedRangeCount()).isEqualTo(1));

            queue.commit(reads.stream().limit(4).map(LogEntry::position).toList());
            long expectedTruncate = reads.get(4).nextIndex();
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(
                            () -> {
                                LatchQueueMetrics metrics = queue.metrics();
                                assertThat(metrics.truncateBeforeIndex())
                                        .isEqualTo(expectedTruncate);
                                assertThat(metrics.completedRangeCount()).isZero();
                                assertThat(metrics.currentGapCount()).isZero();
                            });
        }
    }

    @Test
    void outOfOrderCommitsMergeOnlyWhenTheGapFills() {
        purgeBase();
        try (LatchQueue<Event> queue = newQueue("outoforder", 60000)) {
            List<LogEntry<Event>> reads = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                queue.write(new Event(i, "e" + i));
            }
            reads.addAll(queue.read(5, Duration.ofSeconds(2)));
            // commit everything except the first entry: the truncate index must not move
            queue.commit(reads.stream().skip(1).map(LogEntry::position).toList());

            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(
                            () -> assertThat(queue.metrics().completedRangeCount()).isEqualTo(4));
            long truncateBefore = queue.metrics().truncateBeforeIndex();
            assertThat(truncateBefore).isEqualTo(reads.get(0).index());

            // fill the gap: everything merges in one step
            queue.commit(java.util.List.of(reads.get(0).position()));
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(
                            () ->
                                    assertThat(queue.metrics().truncateBeforeIndex())
                                            .isEqualTo(reads.get(4).nextIndex()));
        }
    }

    @Test
    void staleOverlapAndNullPositionsAreIgnored() {
        purgeBase();
        try (LatchQueue<Event> queue = newQueue("validation", 60000)) {
            List<LogEntry<Event>> reads = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                queue.write(new Event(i, "e" + i));
            }
            reads.addAll(queue.read(3, Duration.ofSeconds(2)));
            queue.commit(reads.stream().map(LogEntry::position).toList());
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(
                            () -> assertThat(queue.metrics().completedRangeCount()).isZero());
            long truncate = queue.metrics().truncateBeforeIndex();

            long committedBefore = queue.metrics().totalCommittedRanges();
            // null, invalid-after-truncate (stale) and boundary-crossing (overlap) are all skipped
            List<Position> junk = new ArrayList<>();
            junk.add(null);
            junk.add(new Position(reads.get(0).index(), truncate)); // fully stale
            junk.add(
                    new Position(
                            reads.get(1).index(), reads.get(2).nextIndex())); // crosses truncate
            queue.commit(junk);
            assertThat(queue.metrics().totalCommittedRanges()).isEqualTo(committedBefore);
            assertThat(queue.metrics().completedRangeCount()).isZero();
        }
    }

    @Test
    void gapIsForceSkippedAfterTheTimeout() {
        purgeBase();
        try (LatchQueue<Event> queue = newQueue("forceskip", 300)) {
            List<LogEntry<Event>> reads = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                queue.write(new Event(i, "e" + i));
            }
            reads.addAll(queue.read(4, Duration.ofSeconds(2)));
            // entries 0 and 1 are never committed: a 2-message gap sits in front
            queue.commit(List.of(reads.get(2).position(), reads.get(3).position()));

            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(
                            () -> assertThat(queue.metrics().completedRangeCount()).isEqualTo(2));
            // after ~300 ms the gap is force-skipped and the truncate jumps behind it
            long expectedTruncate = reads.get(3).nextIndex();
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(
                            () ->
                                    assertThat(queue.metrics().truncateBeforeIndex())
                                            .isEqualTo(expectedTruncate));
            // at least the two uncommitted messages; the gap can measure larger in index space
            // when the writes straddle a roll boundary (indexes are cycle-scoped, not dense)
            assertThat(queue.metrics().largestGapSize()).isGreaterThanOrEqualTo(2);
            assertThat(queue.metrics().currentGapCount()).isZero();
        }
    }

    @Test
    void forceCommitGapFillsManuallyAndValidatesArguments() {
        purgeBase();
        try (LatchQueue<Event> queue = newQueue("manual", 60000)) {
            List<LogEntry<Event>> reads = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                queue.write(new Event(i, "e" + i));
            }
            reads.addAll(queue.read(3, Duration.ofSeconds(2)));
            // nothing committed; manually abandon everything
            queue.forceCommitGap(reads.get(0).index(), reads.get(2).nextIndex());
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(
                            () ->
                                    assertThat(queue.metrics().truncateBeforeIndex())
                                            .isEqualTo(reads.get(2).nextIndex()));

            assertThatThrownBy(() -> queue.forceCommitGap(-1, 10))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("non-negative");
            assertThatThrownBy(() -> queue.forceCommitGap(10, 10))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("greater than");
        }
    }

    @Test
    void checkpointSurvivesRestartWithoutReprocessing() {
        purgeBase();
        long truncateAfterCommit;
        try (LatchQueue<Event> queue = newQueue("persist", 60000)) {
            List<LogEntry<Event>> reads = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                queue.write(new Event(i, "e" + i));
            }
            reads.addAll(queue.read(5, Duration.ofSeconds(2)));
            queue.commit(reads.stream().map(LogEntry::position).toList());
            long expected = reads.get(4).nextIndex();
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(
                            () ->
                                    assertThat(queue.metrics().truncateBeforeIndex())
                                            .isEqualTo(expected));
            truncateAfterCommit = queue.metrics().truncateBeforeIndex();
        }
        assertThat(Files.exists(queueDirectory("persist").resolve("checkpoint"))).isTrue();

        try (LatchQueue<Event> reopened = newQueue("persist", 60000)) {
            // the checkpoint is loaded: the truncate index starts at the persisted value and
            // nothing is re-delivered
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(
                            () ->
                                    assertThat(reopened.metrics().truncateBeforeIndex())
                                            .isEqualTo(truncateAfterCommit));
            assertThat(reopened.read(1, Duration.ofMillis(400))).isEmpty();
            // new messages continue from the restored position
            reopened.write(new Event(100, "after-restart"));
            List<LogEntry<Event>> reads = reopened.read(1, Duration.ofSeconds(2));
            assertThat(reads).hasSize(1);
            assertThat(reads.get(0).data().id()).isEqualTo(100);
        }
    }

    @Test
    void staleCheckpointIsClampedToTheEarliestMessage() throws IOException {
        purgeBase();
        try (LatchQueue<Event> queue = newQueue("clamped", 60000)) {
            List<LogEntry<Event>> reads = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                queue.write(new Event(i, "e" + i));
            }
            reads.addAll(queue.read(5, Duration.ofSeconds(2)));
            queue.commit(reads.stream().map(LogEntry::position).toList());
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(
                            () -> assertThat(queue.metrics().completedRangeCount()).isZero());
        }
        // simulate a checkpoint that lags behind a cleanup: truncate = 1 (well below firstIndex)
        new CheckpointStore(queueDirectory("clamped")).write(1L, Map.of());

        try (LatchQueue<Event> reopened = newQueue("clamped", 60000)) {
            List<LogEntry<Event>> reads = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 10000;
            while (reads.size() < 5 && System.currentTimeMillis() < deadline) {
                reads.addAll(reopened.read(5, Duration.ofMillis(500)));
            }
            assertThat(reads).hasSize(5); // resumed from the earliest existing message
        }
    }

    @Test
    void emptyGapCorrectionBridgesAnIdleRollAcrossRestart() throws Exception {
        purgeBase();
        long messageTwoIndex;
        try (LatchQueue<Event> queue = newQueue("corrections", 60000)) {
            // m1 delivered and committed; the tail delivery makes its nextIndex provisional
            queue.write(new Event(1, "m1"));
            List<LogEntry<Event>> first = queue.read(1, Duration.ofSeconds(2));
            assertThat(first).hasSize(1);
            queue.commit(java.util.List.of(first.get(0).position()));
            long provisional = first.get(0).nextIndex();

            Thread.sleep(1200); // idle roll: the next write lands in a later cycle
            messageTwoIndex = queue.write(new Event(2, "m2"));
            assertThat(messageTwoIndex).isNotEqualTo(provisional);

            // m2 stays uncommitted; close persists the truncate and the correction
        }
        try (LatchQueue<Event> reopened = newQueue("corrections", 60000)) {
            List<LogEntry<Event>> reads = reopened.read(1, Duration.ofSeconds(2));
            assertThat(reads).hasSize(1); // m2 is delivered again (it was never committed)
            assertThat(reads.get(0).data().id()).isEqualTo(2);
            reopened.commit(java.util.List.of(reads.get(0).position()));

            // without the persisted correction this merge would stall for the force-skip
            // timeout; with it the truncate advances past the empty gap almost instantly
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(
                            () ->
                                    assertThat(reopened.metrics().truncateBeforeIndex())
                                            .isEqualTo(reads.get(0).nextIndex()));
        }
    }
}
