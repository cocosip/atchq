package io.github.cocosip.latchq.exception;

/**
 * Thrown by {@code read} when a stored message cannot be deserialized into the configured payload
 * type (typically a payload schema that drifted from the stored JSON). The failed entry is no
 * longer deliverable in the running session; the exception carries its position range so the
 * application can skip it explicitly with {@code forceCommitGap(index, nextIndex)} and continue
 * consuming. Without that call the entry is re-delivered after a restart, since it was never
 * committed.
 */
public class LatchQDeserializationException extends LatchQException {

    private final long index;
    private final long nextIndex;

    public LatchQDeserializationException(
            String message, long index, long nextIndex, Throwable cause) {
        super(message, cause);
        this.index = index;
        this.nextIndex = nextIndex;
    }

    /** Index of the message that could not be deserialized. */
    public long getIndex() {
        return index;
    }

    /** Index of the message after the failed one (exclusive end for {@code forceCommitGap}). */
    public long getNextIndex() {
        return nextIndex;
    }
}
