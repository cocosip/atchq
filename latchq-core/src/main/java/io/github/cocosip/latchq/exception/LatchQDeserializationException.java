package io.github.cocosip.latchq.exception;

import io.github.cocosip.latchq.LogEntry;
import java.io.Serial;
import java.util.ArrayList;
import java.util.List;

/**
 * Thrown by {@code read} when a stored message cannot be deserialized into the configured payload
 * type (typically a payload schema that drifted from the stored JSON). The exception carries the
 * position range of the failed entry so the application can skip it explicitly with {@code
 * forceCommitGap(index, nextIndex)} and continue consuming.
 *
 * <p>When the failed entry was not the first one of its read batch, the entries deserialized before
 * it are carried by {@link #getSuccessfullyRead()}: process them and commit their positions before
 * skipping the poison entry, otherwise the uncommitted prefix would block the consumption progress
 * until the gap force-skip timeout abandons it. Without a {@code forceCommitGap} call the failed
 * entry is re-delivered after a restart, since it was never committed.
 */
public class LatchQDeserializationException extends LatchQException {

    @Serial private static final long serialVersionUID = 1L;

    private final long index;
    private final long nextIndex;

    /**
     * Declared as {@link ArrayList} (serializable) because the exception itself is serializable.
     */
    private final ArrayList<LogEntry<?>> successfullyRead;

    public LatchQDeserializationException(
            String message, long index, long nextIndex, Throwable cause) {
        this(message, index, nextIndex, cause, List.of());
    }

    /**
     * @param successfullyRead the entries of the same read batch that were deserialized before the
     *     failure (possibly empty); never {@code null}
     */
    public LatchQDeserializationException(
            String message,
            long index,
            long nextIndex,
            Throwable cause,
            List<? extends LogEntry<?>> successfullyRead) {
        super(message, cause);
        this.index = index;
        this.nextIndex = nextIndex;
        this.successfullyRead = new ArrayList<>(successfullyRead);
    }

    /** Index of the message that could not be deserialized. */
    public long getIndex() {
        return index;
    }

    /** Index of the message after the failed one (exclusive end for {@code forceCommitGap}). */
    public long getNextIndex() {
        return nextIndex;
    }

    /**
     * Entries of the same read batch that were successfully deserialized before the failure, in
     * batch order (empty when the failed entry was the first one). The payload type of every entry
     * is the payload type of the queue that produced it, so the caller may assign the result to
     * {@code List<LogEntry<MyPayload>>}.
     *
     * <p>Process these entries and commit their positions before calling {@code
     * forceCommitGap(index, nextIndex)}, so the committed prefix and the skipped poison range join
     * into one continuous range and the consumption progress advances past the failure.
     */
    @SuppressWarnings("unchecked")
    public <X> List<LogEntry<X>> getSuccessfullyRead() {
        return (List<LogEntry<X>>) (List<?>) successfullyRead;
    }
}
