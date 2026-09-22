package io.github.cocosip.latchq.config;

import io.github.cocosip.latchq.exception.LatchQInvalidConfigurationException;
import java.util.function.Consumer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

/**
 * Per-queue configuration. Defaults intentionally mirror the FASTER reference configuration where
 * the semantics survive the Chronicle migration; see the design document for the field mapping
 * table (chapter 8).
 */
public class LatchQueueConfiguration {

    /** Directory name of the queue below {@code rootPath/<type>}; required. */
    private String fileName;

    /**
     * Upper bound for a single message's serialized size; writes are rejected with a clear error
     * before touching storage when exceeded (M0/M1 finding: Chronicle's underlying limit is the
     * appender write buffer, whose usable size is {@code blockSize/2 - 4}; the library derives the
     * block size internally from this setting, so raising it admits bigger messages). Defaults to
     * 20 MiB, which comfortably covers the expected workload of sub-megabyte messages with the
     * occasional larger one. Note that every appender allocates the derived buffer, so very large
     * values multiply with the number of concurrent writer threads and may require raising {@code
     * -XX:MaxDirectMemorySize}.
     */
    private long maxMessageSizeBytes = 20L * 1024 * 1024;

    /**
     * Chronicle roll cycle name, resolved from the {@code RollCycles} constants. The cycle choice
     * carries the capacity semantics of the FASTER {@code Capacity}/{@code SegmentSizeBits}
     * options; its index capacity must cover the per-cycle message volume.
     */
    private String rollCycle = "DEFAULT";

    /**
     * Interval between {@code appender.sync()} flushes. Chronicle writes are visible to tailers
     * immediately, so this only controls the durability window - how much tail data may be lost on
     * a crash. 0 disables periodic syncing.
     */
    private int syncIntervalMillis = 2000;

    /** Interval of the background range-merging (complete) task; must be positive. */
    private int completeIntervalMillis = 3000;

    /** Interval of the background checkpoint persistence task; must be positive. */
    private int checkpointIntervalMillis = 2000;

    /** Interval of the background cycle-file cleanup task; must be positive. */
    private int cleanupIntervalMillis = 300000;

    /** Capacity of the bounded hand-off queue between the scan thread and consumers. */
    private int preReadCapacity = 5000;

    /** Delay before the first gap warning and interval between repeated gap warnings. */
    private int gapTimeoutMillis = 600000;

    /**
     * Gap auto-skip timeout; a gap older than this is force-completed (data in the gap is
     * abandoned). 0 disables timeout-driven skipping - note that exceeding {@link
     * #maxCompletedRanges} still forces a skip.
     */
    private int forceCompleteGapTimeoutMillis = 120000;

    /** Memory protection bound for the completed-range set; exceeding it forces a gap skip. */
    private int maxCompletedRanges = 10000;

    /** Advanced hook applied to the Chronicle queue builder before it is built. */
    private transient Consumer<SingleChronicleQueueBuilder> builderCustomizer;

    public void validate() {
        if (fileName == null || fileName.isBlank()) {
            throw new LatchQInvalidConfigurationException(
                    "fileName must be configured for the queue");
        }
        if (maxMessageSizeBytes <= 0 || maxMessageSizeBytes > Integer.MAX_VALUE) {
            throw new LatchQInvalidConfigurationException(
                    "maxMessageSizeBytes must be positive and not exceed "
                            + Integer.MAX_VALUE
                            + " (serialized payloads are byte arrays)");
        }
        if (rollCycle == null || rollCycle.isBlank()) {
            throw new LatchQInvalidConfigurationException("rollCycle must not be blank");
        }
        if (syncIntervalMillis < 0) {
            throw new LatchQInvalidConfigurationException(
                    "syncIntervalMillis must not be negative");
        }
        if (completeIntervalMillis <= 0
                || checkpointIntervalMillis <= 0
                || cleanupIntervalMillis <= 0) {
            // a zero interval would turn the respective background loop into a busy spin
            throw new LatchQInvalidConfigurationException(
                    "completeIntervalMillis, checkpointIntervalMillis and cleanupIntervalMillis"
                            + " must be positive");
        }
        if (preReadCapacity <= 0) {
            throw new LatchQInvalidConfigurationException("preReadCapacity must be positive");
        }
        if (gapTimeoutMillis < 0 || forceCompleteGapTimeoutMillis < 0 || maxCompletedRanges < 0) {
            throw new LatchQInvalidConfigurationException(
                    "gap handling settings must not be negative");
        }
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public long getMaxMessageSizeBytes() {
        return maxMessageSizeBytes;
    }

    public void setMaxMessageSizeBytes(long maxMessageSizeBytes) {
        this.maxMessageSizeBytes = maxMessageSizeBytes;
    }

    public String getRollCycle() {
        return rollCycle;
    }

    public void setRollCycle(String rollCycle) {
        this.rollCycle = rollCycle;
    }

    public int getSyncIntervalMillis() {
        return syncIntervalMillis;
    }

    public void setSyncIntervalMillis(int syncIntervalMillis) {
        this.syncIntervalMillis = syncIntervalMillis;
    }

    public int getCompleteIntervalMillis() {
        return completeIntervalMillis;
    }

    public void setCompleteIntervalMillis(int completeIntervalMillis) {
        this.completeIntervalMillis = completeIntervalMillis;
    }

    public int getCheckpointIntervalMillis() {
        return checkpointIntervalMillis;
    }

    public void setCheckpointIntervalMillis(int checkpointIntervalMillis) {
        this.checkpointIntervalMillis = checkpointIntervalMillis;
    }

    public int getCleanupIntervalMillis() {
        return cleanupIntervalMillis;
    }

    public void setCleanupIntervalMillis(int cleanupIntervalMillis) {
        this.cleanupIntervalMillis = cleanupIntervalMillis;
    }

    public int getPreReadCapacity() {
        return preReadCapacity;
    }

    public void setPreReadCapacity(int preReadCapacity) {
        this.preReadCapacity = preReadCapacity;
    }

    public int getGapTimeoutMillis() {
        return gapTimeoutMillis;
    }

    public void setGapTimeoutMillis(int gapTimeoutMillis) {
        this.gapTimeoutMillis = gapTimeoutMillis;
    }

    public int getForceCompleteGapTimeoutMillis() {
        return forceCompleteGapTimeoutMillis;
    }

    public void setForceCompleteGapTimeoutMillis(int forceCompleteGapTimeoutMillis) {
        this.forceCompleteGapTimeoutMillis = forceCompleteGapTimeoutMillis;
    }

    public int getMaxCompletedRanges() {
        return maxCompletedRanges;
    }

    public void setMaxCompletedRanges(int maxCompletedRanges) {
        this.maxCompletedRanges = maxCompletedRanges;
    }

    public Consumer<SingleChronicleQueueBuilder> getBuilderCustomizer() {
        return builderCustomizer;
    }

    public void setBuilderCustomizer(Consumer<SingleChronicleQueueBuilder> builderCustomizer) {
        this.builderCustomizer = builderCustomizer;
    }
}
