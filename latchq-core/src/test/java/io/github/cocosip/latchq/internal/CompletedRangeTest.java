package io.github.cocosip.latchq.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/** Pure ordering/merging semantics of the committed-range set. */
class CompletedRangeTest {

    @Test
    void rejectsInvalidRanges() {
        assertThatThrownBy(() -> new CompletedRange(-1, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-negative");
        assertThatThrownBy(() -> new CompletedRange(5, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("greater than start");
        assertThatThrownBy(() -> new CompletedRange(6, 5))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void sortsByStartThenEnd() {
        TreeSet<CompletedRange> ranges = new TreeSet<>(CompletedRange.naturalOrder());
        ranges.add(new CompletedRange(10, 12));
        ranges.add(new CompletedRange(2, 4));
        ranges.add(new CompletedRange(2, 3));
        assertThat(ranges).extracting(CompletedRange::start).containsExactly(2L, 2L, 10L);
        assertThat(ranges).extracting(CompletedRange::end).containsExactly(3L, 4L, 12L);
    }

    @Test
    void identicalRangesCollapseInTheSet() {
        TreeSet<CompletedRange> ranges = new TreeSet<>(CompletedRange.naturalOrder());
        assertThat(ranges.add(new CompletedRange(1, 2))).isTrue();
        assertThat(ranges.add(new CompletedRange(1, 2))).isFalse();
        assertThat(ranges).hasSize(1);
    }

    @Test
    void adjacentAndOverlappingRangesAreDistinguishableForTheMerger() {
        // the merge itself lives in DefaultLatchQueue.mergeAndAdvance; these cases document the
        // set semantics it relies on: exact adjacency (start == end) and overlaps are both kept
        TreeSet<CompletedRange> ranges = new TreeSet<>(CompletedRange.naturalOrder());
        ranges.add(new CompletedRange(1, 2));
        ranges.add(new CompletedRange(2, 3)); // exactly adjacent
        ranges.add(new CompletedRange(3, 9)); // adjacent again
        ranges.add(new CompletedRange(9, 20)); // adjacent again
        ranges.add(new CompletedRange(15, 30)); // overlapping a later range
        assertThat(ranges).hasSize(5);
    }
}
