package io.github.cocosip.latchq.internal;

import net.openhft.chronicle.queue.RollCycle;

/**
 * Stamps scanned messages with their real nextIndex and controls when entries become deliverable to
 * consumers.
 *
 * <p>Two delivery rules implement the M0-decided defence against the roll-boundary trap:
 *
 * <ol>
 *   <li><b>Look-ahead stamping</b> - an entry is delivered only once the next message has been
 *       read, so its nextIndex is the real index of the follower, never an arithmetic guess. Inside
 *       a cycle {@code index + 1} happens to be correct, but across cycle boundaries (and after
 *       idle gaps that skip cycles) it is not, which would create permanent fake gaps and silent
 *       data loss on force-skip.
 *   <li><b>Provisional tail delivery</b> - when the queue is caught up, the held entry is delivered
 *       with an in-cycle provisional nextIndex ({@code toIndex(cycle, seq + 1)}), so an idle
 *       producer never starves consumers. The scan thread remembers the provisional index; when the
 *       real next message arrives at a different index (a roll happened in between), it records an
 *       "empty gap correction" so the M2 merge can skip the empty range instead of waiting for the
 *       force-skip timeout.
 * </ol>
 *
 * <p>The entry held back at close time was never handed to any consumer and is simply dropped (it
 * is re-scanned after a restart, which is covered by the idempotency contract).
 */
public final class ReadAheadStamper {

    private BufferedLogEntry held;

    /**
     * Offers the next raw scan result. Returns the previously held entry, now stamped with {@code
     * raw.index()} as its nextIndex, or {@code null} on the very first offer.
     */
    public synchronized BufferedLogEntry offer(BufferedLogEntry raw) {
        if (held == null) {
            held = raw;
            return null;
        }
        BufferedLogEntry stamped = new BufferedLogEntry(held.data(), held.index(), raw.index());
        held = raw;
        return stamped;
    }

    /**
     * Called when the queue is caught up: delivers the held entry with an in-cycle provisional
     * nextIndex, or {@code null} when nothing is held or the provisional would cross the cycle end
     * (in which case the entry stays held until the next real read).
     */
    public synchronized BufferedLogEntry deliverTailProvisional(RollCycle rollCycle) {
        if (held == null) {
            return null;
        }
        long sequence = rollCycle.toSequenceNumber(held.index());
        if (sequence + 1 >= rollCycle.maxMessagesPerCycle()) {
            return null; // cannot guess across the cycle end; wait for the next real read
        }
        long provisional = rollCycle.toIndex(rollCycle.toCycle(held.index()), sequence + 1);
        BufferedLogEntry tail = new BufferedLogEntry(held.data(), held.index(), provisional);
        held = null;
        return tail;
    }

    /** The entry currently held back (not yet deliverable), or {@code null}. */
    public synchronized BufferedLogEntry held() {
        return held;
    }
}
