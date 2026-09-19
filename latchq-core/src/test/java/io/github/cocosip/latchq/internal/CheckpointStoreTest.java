package io.github.cocosip.latchq.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CheckpointStoreTest {

    @TempDir Path dir;

    @Test
    void writeThenReadRoundTripsTruncateAndCorrections() throws Exception {
        CheckpointStore store = new CheckpointStore(dir);
        store.write(12345L, Map.of(100L, 200L, 300L, 400L));
        CheckpointStore.Checkpoint checkpoint = store.read();
        assertThat(checkpoint).isNotNull();
        assertThat(checkpoint.truncate()).isEqualTo(12345L);
        assertThat(checkpoint.corrections())
                .containsExactlyInAnyOrderEntriesOf(Map.of(100L, 200L, 300L, 400L));
    }

    @Test
    void missingFileReadsAsNull() {
        assertThat(new CheckpointStore(dir).read()).isNull();
    }

    @Test
    void corruptFileReadsAsNullInsteadOfFailingStartup() throws Exception {
        CheckpointStore store = new CheckpointStore(dir);
        store.write(1L, Map.of());
        Files.writeString(dir.resolve("checkpoint"), "{not json at all");
        assertThat(store.read()).isNull();
    }

    @Test
    void tempLeftoverFromACrashDoesNotAffectTheCheckpoint() throws Exception {
        CheckpointStore store = new CheckpointStore(dir);
        store.write(42L, Map.of());
        // simulate a crash during a later write: a stale temp file with garbage
        Files.writeString(dir.resolve("checkpoint.tmp"), "half-written garbage");
        assertThat(store.read()).isNotNull();
        assertThat(store.read().truncate()).isEqualTo(42L);
        // the next write replaces both the checkpoint and the stale temp
        store.write(43L, Map.of());
        assertThat(store.read().truncate()).isEqualTo(43L);
        assertThat(Files.exists(dir.resolve("checkpoint.tmp"))).isFalse();
    }

    @Test
    void emptyCorrectionsRoundTrip() throws Exception {
        CheckpointStore store = new CheckpointStore(dir);
        store.write(7L, Map.of());
        CheckpointStore.Checkpoint checkpoint = store.read();
        assertThat(checkpoint.truncate()).isEqualTo(7L);
        assertThat(checkpoint.corrections()).isEmpty();
    }
}
