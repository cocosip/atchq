package io.github.cocosip.latchq;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Sync contract: {@code syncIntervalMillis} drives a periodic {@code appender.sync()} sweep (the
 * spike-verified implementation route). The sweep must run across writer threads without errors and
 * leave the data readable after a reopen.
 */
class LatchQueueSyncTest {

    private static final Path BASE = Path.of("target", "test-data", "latchq-sync");

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
        return LatchQueueBuilder.create("sync", Event.class)
                .rootPath(BASE.toString())
                .configuration(
                        c -> {
                            c.setFileName("sync");
                            c.setSyncIntervalMillis(50);
                        })
                .build();
    }

    @Test
    void periodicSyncSweepLeavesDataIntactAfterReopen() throws Exception {
        LatchQueue<Event> queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.write(new Event(i));
        }
        // several sync intervals pass, with the sweep syncing the writer-thread appender
        // cross-thread; any failure there would surface as a warn/error, not a throw
        Thread.sleep(200);
        queue.close();

        LatchQueue<Event> reopened = newQueue();
        LogEntryList<Event> batch = reopened.read(100);
        assertThat(batch).hasSize(10);
        for (int i = 0; i < 10; i++) {
            assertThat(batch.get(i).data().id()).isEqualTo(i);
        }
        reopened.close();
    }
}
