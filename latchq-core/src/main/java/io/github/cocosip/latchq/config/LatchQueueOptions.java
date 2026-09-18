package io.github.cocosip.latchq.config;

import io.github.cocosip.latchq.exception.LatchQInvalidConfigurationException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Global options: the storage root path plus named per-queue configurations.
 *
 * <p>Unlike the FASTER reference implementation, looking up an unconfigured name fails fast instead
 * of silently falling back to a default configuration - a typo in a queue name must not silently
 * apply the wrong settings.
 */
public class LatchQueueOptions {

    /** Root directory below which every queue stores its files; required. */
    private String rootPath;

    private final Map<String, LatchQueueConfiguration> configurations = new LinkedHashMap<>();

    /** Adds or amends the configuration stored under {@code name}. */
    public LatchQueueOptions configure(String name, Consumer<LatchQueueConfiguration> configurer) {
        if (name == null || name.isBlank()) {
            throw new LatchQInvalidConfigurationException("queue name must not be blank");
        }
        if (configurer == null) {
            throw new LatchQInvalidConfigurationException("configurer must not be null");
        }
        LatchQueueConfiguration configuration =
                configurations.computeIfAbsent(name, key -> new LatchQueueConfiguration());
        configurer.accept(configuration);
        return this;
    }

    /** Validates the global options and every named configuration. */
    public void validate() {
        if (rootPath == null || rootPath.isBlank()) {
            throw new LatchQInvalidConfigurationException(
                    "rootPath must be configured in LatchQueueOptions");
        }
        for (Map.Entry<String, LatchQueueConfiguration> entry : configurations.entrySet()) {
            try {
                entry.getValue().validate();
            } catch (LatchQInvalidConfigurationException e) {
                throw new LatchQInvalidConfigurationException(
                        "invalid configuration for queue '"
                                + entry.getKey()
                                + "': "
                                + e.getMessage());
            }
        }
    }

    /**
     * Returns the configuration registered under {@code name}; fails fast when the name is unknown
     * (deliberate deviation from the FASTER default-fallback behaviour).
     */
    public LatchQueueConfiguration getConfiguration(String name) {
        LatchQueueConfiguration configuration = configurations.get(name);
        if (configuration == null) {
            throw new LatchQInvalidConfigurationException(
                    "no configuration registered for queue name '"
                            + name
                            + "', known names: "
                            + configurations.keySet());
        }
        return configuration;
    }

    public String getRootPath() {
        return rootPath;
    }

    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
    }

    public Map<String, LatchQueueConfiguration> getConfigurations() {
        return configurations;
    }
}
