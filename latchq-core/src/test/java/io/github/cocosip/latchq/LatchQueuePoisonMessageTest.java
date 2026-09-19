package io.github.cocosip.latchq;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.latchq.exception.LatchQDeserializationException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Poison-message contract: an entry whose payload no longer deserializes surfaces as a {@link
 * LatchQDeserializationException} carrying its index range so the application can skip it with
 * {@code forceCommitGap} and keep consuming, while entries with unknown extra JSON fields are read
 * leniently (payload evolution). Raw foreign payloads are injected through a direct Chronicle
 * appender, mimicking a payload schema that drifted from the stored bytes.
 */
class LatchQueuePoisonMessageTest {

    private static final Path BASE = Path.of("target", "test-data", "latchq-poison");

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

    private static LatchQueue<Event> newQueue(String fileName) {
        return LatchQueueBuilder.create("poison", Event.class)
                .rootPath(BASE.toString())
                .configuration(c -> c.setFileName(fileName))
                .build();
    }

    private static Path queueDirectory(String fileName) {
        return BASE.resolve(Event.class.getName()).resolve(fileName);
    }

    /** Appends raw payload bytes exactly the way the queue itself stores a message. */
    private static void rawAppend(String fileName, byte[] payload) {
        try (ChronicleQueue raw =
                        ChronicleQueue.singleBuilder(queueDirectory(fileName).toString())
                                .rollCycle(RollCycles.DEFAULT)
                                .build();
                ExcerptAppender appender = raw.createAppender();
                DocumentContext ctx = appender.writingDocument()) {
            ctx.wire().writeBytes(out -> out.write(payload));
        }
    }

    @Test
    void unreadablePayloadThrowsWithIndexRangeAndCanBeSkipped() {
        String fileName = "skip";
        LatchQueue<Event> queue = newQueue(fileName);
        long first = queue.write(new Event(1));
        queue.close();

        rawAppend(fileName, "definitely not json".getBytes(StandardCharsets.UTF_8));

        queue = newQueue(fileName);
        long third = queue.write(new Event(3));

        // the first read delivers Event(1), then fails on the injected poison entry
        LatchQDeserializationException poison = null;
        try {
            queue.read(10);
        } catch (LatchQDeserializationException e) {
            poison = e;
        }
        assertThat(poison).isNotNull();
        assertThat(poison.getIndex()).isBetween(first + 1, third - 1);
        assertThat(poison.getNextIndex()).isEqualTo(third);

        // skipping the poison range lets consumption continue behind it
        queue.forceCommitGap(poison.getIndex(), poison.getNextIndex());
        LogEntryList<Event> batch = queue.read(10);
        assertThat(batch).hasSize(1);
        assertThat(batch.get(0).data().id()).isEqualTo(3);
        queue.close();
    }

    @Test
    void unknownExtraJsonFieldsAreReadLeniently() {
        String fileName = "lenient";
        LatchQueue<Event> queue = newQueue(fileName);
        long first = queue.write(new Event(1));
        queue.close();

        rawAppend(
                fileName,
                "{\"id\":2,\"unknownField\":\"ignored\"}".getBytes(StandardCharsets.UTF_8));

        queue = newQueue(fileName);
        LogEntryList<Event> batch = queue.read(10);
        assertThat(batch).hasSize(2);
        assertThat(batch.get(0).data().id()).isEqualTo(1);
        assertThat(batch.get(1).data().id()).isEqualTo(2);
        assertThat(batch.get(1).position().index()).isGreaterThan(first);
        queue.close();
    }
}
