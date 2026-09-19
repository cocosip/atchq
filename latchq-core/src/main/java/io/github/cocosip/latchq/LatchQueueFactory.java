package io.github.cocosip.latchq;

/**
 * Factory managing one {@link LatchQueue} instance per (name, payload type) pair. {@link
 * #getOrCreate} auto-initializes the queue, mirroring the FASTER reference factory. Implementations
 * must be safe for concurrent use and should be closed once on shutdown, which closes every queue
 * created through the factory.
 */
public interface LatchQueueFactory extends AutoCloseable {

    /** Returns the shared queue for the given name and payload type, creating it on first use. */
    <T> LatchQueue<T> getOrCreate(String name, Class<T> type);

    /** Whether this factory is still open and usable. */
    default boolean isActive() {
        return true;
    }

    /** Closes every queue created by this factory. Idempotent. */
    @Override
    void close();
}
