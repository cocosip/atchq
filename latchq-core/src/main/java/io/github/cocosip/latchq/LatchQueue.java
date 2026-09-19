package io.github.cocosip.latchq;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

/**
 * A persistent queue supporting multi-threaded concurrent production and multi-threaded concurrent
 * batch consumption with out-of-order commits. One instance exists per (name, payload type) pair
 * inside a JVM; instances are created through {@link LatchQueueFactory} or {@link
 * LatchQueueBuilder}.
 *
 * <p>All methods are synchronous blocking calls. Consumers are expected to follow the read -
 * process - {@link #commit(Collection)} pattern; commits may arrive out of order and are merged by
 * the library to advance the safe consumption point. Business processing must be idempotent (see
 * the design document for the gap-forcing semantics).
 *
 * @param <T> the payload type
 */
public interface LatchQueue<T> extends AutoCloseable {

    /** Initializes the queue (idempotent). {@link LatchQueueFactory} calls this automatically. */
    void initialize();

    /** Whether this queue has been initialized. */
    boolean isInitialized();

    /** Writes a single payload and returns its index. */
    long write(T entity);

    /**
     * Writes a batch of payloads and returns their indexes in order. Not atomic: if a write fails
     * mid-batch, the earlier payloads are already durable but their indexes are not returned;
     * callers that must account for every message should write individually or reconcile via {@link
     * #lastIndexAppended()}.
     */
    List<Long> batchWrite(List<T> values);

    /**
     * Reads up to {@code count} entries, blocking until at least one entry is available. Additional
     * entries are drained without waiting. Throws a {@code LatchQException} instead of hanging when
     * the queue is closed while waiting. An entry that cannot be deserialized throws a {@code
     * LatchQDeserializationException} carrying its index range; skip it via {@link
     * #forceCommitGap(long, long)} to keep consuming.
     */
    LogEntryList<T> read(int count);

    /**
     * Reads up to {@code count} entries, waiting at most {@code timeout} for the first one. Returns
     * an empty list when nothing arrives within the timeout.
     */
    LogEntryList<T> read(int count, Duration timeout);

    /**
     * Commits completed position ranges. Commits may arrive out of order; ranges are merged in the
     * background to advance the safe consumption point. Invalid, stale (already truncated) and
     * boundary-crossing ranges are ignored with a warning.
     */
    void commit(Collection<Position> positions);

    /**
     * Manually marks the range {@code [gapStart, gapEnd)} as completed, skipping over a persistent
     * gap. Data inside the range is abandoned - use with care.
     */
    void forceCommitGap(long gapStart, long gapEnd);

    /**
     * Index of the earliest message still present in the queue, or {@code -1} when the queue is
     * empty (messages removed by cleanup are not visible).
     */
    long firstIndex();

    /** Index of the most recently written message, or {@code -1} when the queue is empty. */
    long lastIndexAppended();

    /** Read-only metrics snapshot. */
    LatchQueueMetrics metrics();

    /**
     * Exports entries in the index range {@code [fromIndex, toIndex)} as JSONL files (one JSON
     * document per line) written into {@code targetDirectory}, splitting into a new file every
     * {@code entriesPerFile} entries. Uses an isolated temporary tailer and does not affect the
     * consumer position. When {@code toIndex} is {@code null}, all existing entries are exported.
     */
    ExportResult export(String targetDirectory, long fromIndex, Long toIndex, int entriesPerFile);

    /**
     * Closes the queue gracefully: stops the background scan, performs a final checkpoint and
     * releases storage. Idempotent; all other methods throw after close. Consumers blocked in a
     * read are woken with a {@code LatchQException} instead of hanging.
     */
    @Override
    void close();
}
