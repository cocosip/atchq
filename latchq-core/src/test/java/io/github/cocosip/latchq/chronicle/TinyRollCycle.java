package io.github.cocosip.latchq.chronicle;

import net.openhft.chronicle.queue.RollCycle;

/**
 * 1-second roll cycle for deterministic roll-boundary testing. Index layout mirrors the
 * conventional test cycles: {@code index = (cycle << 32) | sequenceNumber}.
 */
public final class TinyRollCycle implements RollCycle {

    @Override
    public String format() {
        return "yyyyMMddHHmmss";
    }

    @Override
    public int lengthInMillis() {
        return 1000;
    }

    @Override
    public int defaultIndexCount() {
        // The index capacity must cover the per-cycle message volume, otherwise writes fail with
        // "Unable to index N, the number of entries exceeds max number for the current rollcycle".
        return 1 << 16;
    }

    @Override
    public int defaultIndexSpacing() {
        return 1;
    }

    @Override
    public long toIndex(int cycle, long sequenceNumber) {
        return ((long) cycle << 32) | sequenceNumber;
    }

    @Override
    public long toSequenceNumber(long index) {
        return index & 0xFFFFFFFFL;
    }

    @Override
    public int toCycle(long index) {
        return (int) (index >>> 32);
    }

    @Override
    public long maxMessagesPerCycle() {
        return 1L << 32;
    }

    @Override
    public String toString() {
        return "TinyRollCycle(1s)";
    }
}
