package io.github.cocosip.latchq.spring;

import io.github.cocosip.latchq.config.LatchQueueConfiguration;
import io.github.cocosip.latchq.config.LatchQueueOptions;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring-style view of {@link io.github.cocosip.latchq.config.LatchQueueOptions}: plain getters and
 * setters so {@code @ConfigurationProperties} can bind {@code latchq.root-path} and {@code
 * latchq.configurations.<name>.*} directly from YAML/properties.
 */
@ConfigurationProperties(prefix = "latchq")
public class LatchQueueProperties {

    /** Root directory below which every queue stores its files; required. */
    private String rootPath;

    /** Per-queue configurations keyed by queue name. */
    private Map<String, LatchQueueConfiguration> configurations = new LinkedHashMap<>();

    /**
     * Maps the bound properties onto core options. The single source of the field mapping: every
     * setting of {@link LatchQueueConfiguration} must be copied here exactly once.
     */
    public LatchQueueOptions toOptions() {
        LatchQueueOptions options = new LatchQueueOptions();
        options.setRootPath(rootPath);
        configurations.forEach(
                (name, bound) -> {
                    options.configure(name, c -> {});
                    LatchQueueConfiguration target = options.getConfiguration(name);
                    target.setFileName(bound.getFileName());
                    target.setMaxMessageSizeBytes(bound.getMaxMessageSizeBytes());
                    target.setRollCycle(bound.getRollCycle());
                    target.setSyncIntervalMillis(bound.getSyncIntervalMillis());
                    target.setCompleteIntervalMillis(bound.getCompleteIntervalMillis());
                    target.setCheckpointIntervalMillis(bound.getCheckpointIntervalMillis());
                    target.setCleanupIntervalMillis(bound.getCleanupIntervalMillis());
                    target.setPreReadCapacity(bound.getPreReadCapacity());
                    target.setGapTimeoutMillis(bound.getGapTimeoutMillis());
                    target.setForceCompleteGapTimeoutMillis(
                            bound.getForceCompleteGapTimeoutMillis());
                    target.setMaxCompletedRanges(bound.getMaxCompletedRanges());
                });
        return options;
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

    public void setConfigurations(Map<String, LatchQueueConfiguration> configurations) {
        this.configurations = configurations;
    }
}
