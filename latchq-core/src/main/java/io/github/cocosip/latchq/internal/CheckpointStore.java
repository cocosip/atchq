package io.github.cocosip.latchq.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Durable consumption checkpoint stored next to the queue ({@code checkpoint} file inside the queue
 * directory). Writes are atomic: JSON bytes go to a temp file, are fsynced, and are then moved over
 * the target with {@code ATOMIC_MOVE}, so a crash mid-write can never corrupt the previous
 * checkpoint.
 *
 * <p>Persisted state: the truncate index (first message not known to be processed) plus the
 * empty-gap corrections recorded by the scan thread, so an idle-roll boundary survives a restart
 * without stalling the merge on a fake gap.
 */
public final class CheckpointStore {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointStore.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path queueDirectory;
    private final Path file;
    private final Path tempFile;

    public CheckpointStore(Path queueDirectory) {
        this.queueDirectory = queueDirectory;
        this.file = queueDirectory.resolve("checkpoint");
        this.tempFile = queueDirectory.resolve("checkpoint.tmp");
    }

    /**
     * Atomically persists the truncate index and the empty-gap corrections. Guarded against
     * concurrent writers (the checkpoint task and the final write of {@code close()} racing a
     * timed-out shutdown join): both share the temp file, so overlapping writes could corrupt it.
     */
    public synchronized void write(long truncateIndex, Map<Long, Long> corrections)
            throws IOException {
        Map<String, Object> document = new HashMap<>();
        document.put("truncate", truncateIndex);
        document.put("corrections", corrections);
        byte[] bytes = MAPPER.writeValueAsBytes(document);
        try (FileChannel channel =
                FileChannel.open(
                        tempFile,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING)) {
            channel.write(java.nio.ByteBuffer.wrap(bytes));
            channel.force(true); // fsync before the atomic move
        }
        try {
            Files.move(
                    tempFile,
                    file,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (java.nio.file.AtomicMoveNotSupportedException e) {
            // filesystem without atomic move support (rare): plain replace still keeps the
            // fsync-before-replace ordering
            Files.move(tempFile, file, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * Reads the persisted checkpoint, or {@code null} when it does not exist or is unreadable (a
     * corrupt checkpoint falls back to the earliest message rather than failing the startup).
     */
    public Checkpoint read() {
        if (!Files.exists(file)) {
            return null;
        }
        try {
            JsonNode root = MAPPER.readTree(Files.readString(file, StandardCharsets.UTF_8));
            long truncate = root.get("truncate").asLong();
            Map<Long, Long> corrections = new HashMap<>();
            JsonNode correctionsNode = root.get("corrections");
            if (correctionsNode != null && correctionsNode.isObject()) {
                for (Map.Entry<String, JsonNode> field : correctionsNode.properties()) {
                    corrections.put(Long.parseLong(field.getKey()), field.getValue().asLong());
                }
            }
            return new Checkpoint(truncate, corrections);
        } catch (IOException | RuntimeException e) {
            LOG.warn(
                    "checkpoint file {} is unreadable, falling back to the earliest message",
                    file,
                    e);
            return null;
        }
    }

    /** Persisted checkpoint content. */
    public record Checkpoint(long truncate, Map<Long, Long> corrections) {
        public Checkpoint {
            corrections = Map.copyOf(corrections);
        }
    }
}
