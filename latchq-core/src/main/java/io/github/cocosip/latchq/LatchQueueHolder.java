package io.github.cocosip.latchq;

import io.github.cocosip.latchq.config.LatchQueueOptions;
import io.github.cocosip.latchq.exception.LatchQException;
import io.github.cocosip.latchq.exception.LatchQNotInitializedException;
import io.github.cocosip.latchq.internal.DefaultLatchQueueFactory;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Static access point to a process-wide shared {@link LatchQueueFactory} for applications without a
 * DI container: initialize once at startup, then reach the same factory from any class without
 * passing it through constructors. Applications using a DI container (e.g. Spring Boot via
 * latchq-spring-boot-starter) should prefer an injected factory instead - the holder keeps its own
 * independent factory instance.
 *
 * <pre>{@code
 * // startup, once
 * LatchQueueHolder.init(options -> options
 *         .setRootPath("/data/latchq")
 *         .configure("audit", c -> c.setFileName("audit")));
 *
 * // anywhere, in any class
 * LatchQueue<AuditEvent> queue = LatchQueueHolder.getOrCreate("audit", AuditEvent.class);
 *
 * // shutdown, once: closes every queue and performs the final checkpoint
 * LatchQueueHolder.reset();
 * }</pre>
 *
 * The holder never registers a JVM shutdown hook; the application owns the lifecycle and must call
 * {@link #reset()} (or close the factory it obtained via {@link #getInstance()}) on shutdown.
 * Missing that call only costs the final checkpoint - data already synced to disk stays valid.
 */
public final class LatchQueueHolder {

    private static volatile LatchQueueFactory factory;

    private LatchQueueHolder() {}

    /** Initializes the shared factory with the given options. Fails fast if already initialized. */
    public static synchronized void init(LatchQueueOptions options) {
        Objects.requireNonNull(options, "options");
        if (factory != null) {
            throw new LatchQException(
                    "the latchq holder is already initialized; call reset() first to reconfigure");
        }
        factory = new DefaultLatchQueueFactory(options);
    }

    /** Convenience overload that builds the options through the given configurer. */
    public static void init(Consumer<LatchQueueOptions> configurer) {
        Objects.requireNonNull(configurer, "configurer");
        LatchQueueOptions options = new LatchQueueOptions();
        configurer.accept(options);
        init(options);
    }

    /** Returns the shared factory; fails fast when the holder was never initialized. */
    public static LatchQueueFactory getInstance() {
        return requireFactory();
    }

    /** Delegates to the shared factory; fails fast when the holder was never initialized. */
    public static <T> LatchQueue<T> getOrCreate(String name, Class<T> type) {
        return requireFactory().getOrCreate(name, type);
    }

    /** Returns whether the shared factory is currently initialized. */
    public static boolean isInitialized() {
        return factory != null;
    }

    /** Closes the shared factory (final checkpoint) and allows a fresh init. Idempotent. */
    public static synchronized void reset() {
        LatchQueueFactory current = factory;
        factory = null;
        if (current != null) {
            current.close();
        }
    }

    private static LatchQueueFactory requireFactory() {
        LatchQueueFactory current = factory;
        if (current == null) {
            throw new LatchQNotInitializedException(
                    "the latchq holder is not initialized; call LatchQueueHolder.init(...) once at"
                            + " startup");
        }
        return current;
    }
}
