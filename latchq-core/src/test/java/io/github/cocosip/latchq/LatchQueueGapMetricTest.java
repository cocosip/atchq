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
import java.util.Comparator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Gap metric contract: {@code currentGapCount} reflects a detected, persisting gap even while no
 * merge progress is made (previously it only refreshed on progress, reporting a stale value).
 */
class LatchQueueGapMetricTest {

    private static final Path BASE = Path.of("target", "test-data", "latchq-gap-metric");

    record Event(int id) {}

    private static long epoch() {
        return LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

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

    private LatchQueue<Event> newQueue() {
        return LatchQueueBuilder.create("gaps", Event.class)
                .rootPath(BASE.toString())
                .configuration(
                        c -> {
                            c.setFileName("gaps");
                            c.setCompleteIntervalMillis(50);
                            c.setForceCompleteGapTimeoutMillis(0); // never auto-skip in this test
                            c.setBuilderCustomizer(
                                    b -> b.rollCycle(new TinyRollCycle()).epoch(epoch()));
                        })
                .build();
    }

    @Test
    void currentGapCountReflectsAPersistingGapWithoutProgress() {
        LatchQueue<Event> queue = newQueue();
        for (int i = 0; i < 3; i++) {
            queue.write(new Event(i));
        }
        LogEntryList<Event> first = queue.read(1, Duration.ofSeconds(5));
        LogEntryList<Event> second = queue.read(1, Duration.ofSeconds(5));
        LogEntryList<Event> third = queue.read(1, Duration.ofSeconds(5));
        assertThat(first).hasSize(1);
        assertThat(second).hasSize(1);
        assertThat(third).hasSize(1);

        // commit around the middle entry: it becomes a persisting gap at [second, third)
        queue.commit(first.getPositions());
        queue.commit(third.getPositions());

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(
                        () -> {
                            LatchQueueMetrics metrics = queue.metrics();
                            assertThat(metrics.currentGapCount()).isEqualTo(1);
                            assertThat(metrics.completedRangeCount()).isEqualTo(1);
                        });
        queue.close();
    }
}
