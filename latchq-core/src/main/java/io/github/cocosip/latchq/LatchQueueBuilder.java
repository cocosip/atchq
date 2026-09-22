package io.github.cocosip.latchq;

import io.github.cocosip.latchq.config.LatchQueueConfiguration;
import io.github.cocosip.latchq.config.LatchQueueOptions;
import io.github.cocosip.latchq.internal.DefaultLatchQueue;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Code-first entry point for manual wiring (no DI). Configures the queue inline and returns an
 * initialized instance:
 *
 * <pre>{@code
 * LatchQueue<Event> queue = LatchQueueBuilder.create("events", Event.class)
 *         .rootPath("/data/latchq")
 *         .configuration(c -> c.setFileName("events"))
 *         .build();
 * }</pre>
 */
public final class LatchQueueBuilder<T> {

    private final String name;
    private final Class<T> type;
    private final LatchQueueOptions options;

    private LatchQueueBuilder(String name, Class<T> type, LatchQueueOptions options) {
        this.name = Objects.requireNonNull(name, "name");
        this.type = Objects.requireNonNull(type, "type");
        this.options = Objects.requireNonNull(options, "options");
        // register the name so configuration(...) can amend it below
        this.options.configure(name, c -> {});
    }

    /** Starts building a queue with a fresh, empty options object. */
    public static <T> LatchQueueBuilder<T> create(String name, Class<T> type) {
        return new LatchQueueBuilder<>(name, type, new LatchQueueOptions());
    }

    /**
     * Builds the queue against externally managed options. Unlike the factory path, an unknown name
     * is not an error here: the name is registered in the options automatically (with a default
     * configuration when absent) so {@code configuration(...)} can amend it afterwards.
     */
    public static <T> LatchQueueBuilder<T> create(
            String name, Class<T> type, LatchQueueOptions options) {
        return new LatchQueueBuilder<>(name, type, options);
    }

    public LatchQueueBuilder<T> rootPath(String rootPath) {
        options.setRootPath(rootPath);
        return this;
    }

    public LatchQueueBuilder<T> configuration(Consumer<LatchQueueConfiguration> configurer) {
        options.configure(name, configurer);
        return this;
    }

    /**
     * Validates the options, initializes the queue and returns it. Configuration problems surface
     * as {@link io.github.cocosip.latchq.exception.LatchQInvalidConfigurationException}, matching
     * the factory path, so callers can catch them by type.
     */
    public LatchQueue<T> build() {
        options.validate();
        DefaultLatchQueue<T> queue =
                new DefaultLatchQueue<>(name, type, options, options.getConfiguration(name));
        queue.initialize();
        return queue;
    }
}
