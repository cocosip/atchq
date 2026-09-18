package io.github.cocosip.latchq;

/**
 * Read-only metrics snapshot, field names aligned with the FASTER reference implementation. Note
 * that {@code largestGapSize} counts messages, not bytes (Chronicle indexes are message ordinals,
 * not byte offsets).
 */
public record LatchQueueMetrics(
        long totalWriteCount,
        long totalReadCount,
        long totalCommittedRanges,
        long currentGapCount,
        long largestGapSize,
        int completedRangeCount,
        long truncateBeforeIndex,
        long firstIndex,
        long lastIndexAppended) {}
