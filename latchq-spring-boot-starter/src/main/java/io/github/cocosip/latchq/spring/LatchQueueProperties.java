package io.github.cocosip.latchq.spring;

import io.github.cocosip.latchq.config.LatchQueueConfiguration;
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
