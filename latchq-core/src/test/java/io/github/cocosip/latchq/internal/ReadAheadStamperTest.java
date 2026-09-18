package io.github.cocosip.latchq.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.latchq.chronicle.TinyRollCycle;
import org.junit.jupiter.api.Test;

/**
 * Verifies the read-ahead nextIndex stamping, the M0-decided defence against the roll-boundary
 * trap.
 */
class ReadAheadStamperTest {

    @Test
    void firstOfferIsHeldBackWithoutProducingAnEntry() {
        ReadAheadStamper stamper = new ReadAheadStamper();
        assertThat(stamper.offer(new BufferedLogEntry(new byte[] {1}, 100, -1))).isNull();
        assertThat(stamper.held()).isNotNull();
        assertThat(stamper.held().index()).isEqualTo(100);
    }

    @Test
    void subsequentOffersStampTheRealNextIndex() {
        ReadAheadStamper stamper = new ReadAheadStamper();
        stamper.offer(new BufferedLogEntry(new byte[] {1}, 100, -1));
        BufferedLogEntry stamped = stamper.offer(new BufferedLogEntry(new byte[] {2}, 101, -1));
        assertThat(stamped).isNotNull();
        assertThat(stamped.index()).isEqualTo(100);
        assertThat(stamped.nextIndex()).isEqualTo(101);
        assertThat(stamper.held().index()).isEqualTo(101);
    }

    @Test
    void deliveredEntriesChainAcrossCycleBoundariesAndIdleSkips() {
        // Index values modeled on the M0 verification run: consecutive messages spanning
        // a cycle boundary and an idle gap that skips two whole cycles.
        long[] realIndexes = {
            84951L << 32, // message in cycle 84951
            (84951L << 32) + 1, // next message, same cycle
            84953L << 32, // next message after a 2-cycle idle skip
            (84953L << 32) + 1 // last message; stays held, not delivered
        };
        ReadAheadStamper stamper = new ReadAheadStamper();
        long previousNextIndex = -1;
        int delivered = 0;
        for (long index : realIndexes) {
            BufferedLogEntry stamped =
                    stamper.offer(new BufferedLogEntry(new byte[] {1}, index, -1));
            if (stamped == null) {
                continue;
            }
            delivered++;
            if (previousNextIndex >= 0) {
                // Chain invariant: the predecessor's nextIndex equals this entry's index,
                // including the step into cycle 84953 where arithmetic guessing fails.
                assertThat(stamped.index()).isEqualTo(previousNextIndex);
            }
            previousNextIndex = stamped.nextIndex();
        }
        assertThat(delivered).isEqualTo(3); // the last read stays held
        assertThat(stamper.held().index()).isEqualTo(realIndexes[3]);
    }

    @Test
    void tailDeliveryIsProvisionalWithinTheSameCycle() {
        net.openhft.chronicle.queue.RollCycle cycle = new TinyRollCycle();
        ReadAheadStamper stamper = new ReadAheadStamper();
        assertThat(stamper.deliverTailProvisional(cycle)).isNull(); // nothing held

        stamper.offer(new BufferedLogEntry(new byte[] {1}, 84951L << 32, -1));
        BufferedLogEntry tail = stamper.deliverTailProvisional(cycle);
        assertThat(tail).isNotNull();
        assertThat(tail.index()).isEqualTo(84951L << 32);
        // provisional nextIndex = in-cycle sequence + 1
        assertThat(tail.nextIndex()).isEqualTo((84951L << 32) + 1);
        assertThat(stamper.held()).isNull();
        assertThat(stamper.deliverTailProvisional(cycle)).isNull(); // already delivered
    }
}
