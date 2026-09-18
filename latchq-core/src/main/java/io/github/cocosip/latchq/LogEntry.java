package io.github.cocosip.latchq;

/**
 * A single entry read from the queue: the deserialized payload plus its position range.
 *
 * @param <T> the payload type
 * @param data the deserialized payload
 * @param index the index of this message
 * @param nextIndex the index of the message that follows this one
 */
public record LogEntry<T>(T data, long index, long nextIndex) {

    /** The position range of this entry, ready to be committed. */
    public Position position() {
        return new Position(index, nextIndex);
    }
}
