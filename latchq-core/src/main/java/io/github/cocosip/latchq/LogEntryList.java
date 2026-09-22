package io.github.cocosip.latchq;

import java.io.Serial;
import java.util.ArrayList;
import java.util.List;

/**
 * A batch of entries returned by {@link LatchQueue#read(int)}. Provides {@link #getPositions()} for
 * the common "process the batch, then commit all positions" pattern.
 */
public final class LogEntryList<T> extends ArrayList<LogEntry<T>> {

    @Serial private static final long serialVersionUID = 1L;

    /** Extracts the position of every entry in this batch, in iteration order. */
    public List<Position> getPositions() {
        List<Position> positions = new ArrayList<>(size());
        for (LogEntry<T> entry : this) {
            positions.add(entry.position());
        }
        return positions;
    }
}
