package io.github.cocosip.latchq;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.latchq.exception.LatchQException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Guard against two live {@code LatchQueue} instances on one storage directory (their checkpoints
 * and cleanup would interleave), including distinct configurations whose fileNames collapse onto
 * the same directory through the case normalization of {@code TypeStorageNaming}.
 */
class LatchQueueDirectoryGuardTest {

    private static final Path BASE = Path.of("target", "test-data", "latchq-dir-guard");

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

    private static LatchQueueBuilder<Event> builder(String fileName) {
        return LatchQueueBuilder.create("guard", Event.class)
                .rootPath(BASE.toString())
                .configuration(c -> c.setFileName(fileName));
    }

    @Test
    void secondInstanceOnSameDirectoryFailsFastAndCloseReleasesIt() {
        LatchQueue<Event> first = builder("guard").build();
        try {
            assertThatThrownBy(() -> builder("guard").build())
                    .isInstanceOf(LatchQException.class)
                    .hasMessageContaining("already opened");
        } finally {
            first.close();
        }
        // close() released the directory: a fresh instance can be opened on it again
        assertThatCode(
                        () -> {
                            LatchQueue<Event> second = builder("guard").build();
                            second.close();
                        })
                .doesNotThrowAnyException();
    }

    @Test
    void caseVariantFileNameCollidesWithLiveInstance() {
        // both names normalize to the same on-disk directory, so the second instance would
        // silently interleave its checkpoint and cleanup with the first one
        LatchQueue<Event> first = builder("guard").build();
        try {
            assertThatThrownBy(() -> builder("Guard").build())
                    .isInstanceOf(LatchQException.class)
                    .hasMessageContaining("already opened");
        } finally {
            first.close();
        }
    }
}
