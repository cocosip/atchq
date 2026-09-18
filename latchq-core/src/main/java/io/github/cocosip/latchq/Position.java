package io.github.cocosip.latchq;

/**
 * A position range in the queue, semantics matching the FASTER reference implementation with {@code
 * Address}/{@code NextAddress} replaced by {@code index}/{@code nextIndex}.
 *
 * <p>{@code index} is the index of a message and {@code nextIndex} is the index of the message that
 * follows it. The library guarantees that positions handed to callers are always chained (the
 * nextIndex of one entry equals the index of the following entry), even across roll-cycle
 * boundaries, so positions can be committed out of order and merged safely.
 *
 * @param index the index of the message
 * @param nextIndex the index of the next message (exclusive end of this position)
 */
public record Position(long index, long nextIndex) {

    public Position {
        if (index < 0) {
            throw new IllegalArgumentException("index must be non-negative, got: " + index);
        }
        if (nextIndex <= index) {
            throw new IllegalArgumentException(
                    "nextIndex must be greater than index, index: "
                            + index
                            + ", nextIndex: "
                            + nextIndex);
        }
    }

    /** Returns true when the range is usable for committing. */
    public boolean isValid() {
        return index >= 0 && nextIndex > index;
    }

    /** Number of messages covered by this position (1 for a single-message position). */
    public long length() {
        return nextIndex - index;
    }

    @Override
    public String toString() {
        return "[" + index + ", " + nextIndex + ")";
    }
}
