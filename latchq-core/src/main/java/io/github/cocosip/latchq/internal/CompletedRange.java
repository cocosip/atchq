package io.github.cocosip.latchq.internal;

import java.util.Comparator;

/**
 * An immutable committed range {@code [start, end)} in index space, ordered by start then end so it
 * can live in a sorted set (duplicates collapse, out-of-order commits stay ordered).
 */
public record CompletedRange(long start, long end) implements Comparable<CompletedRange> {

    public CompletedRange {
        if (start < 0) {
            throw new IllegalArgumentException("start must be non-negative, got: " + start);
        }
        if (end <= start) {
            throw new IllegalArgumentException(
                    "end must be greater than start, start: " + start + ", end: " + end);
        }
    }

    @Override
    public int compareTo(CompletedRange other) {
        int byStart = Long.compare(start, other.start);
        return byStart != 0 ? byStart : Long.compare(end, other.end);
    }

    @Override
    public String toString() {
        return "[" + start + ", " + end + ")";
    }

    /** Comparator exposing the natural order for sorted-set construction. */
    public static Comparator<CompletedRange> naturalOrder() {
        return Comparator.naturalOrder();
    }
}
