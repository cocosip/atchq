package io.github.cocosip.latchq.spring;

import io.github.cocosip.latchq.LatchQueueFactory;
import io.github.cocosip.latchq.config.LatchQueueConfiguration;
import io.github.cocosip.latchq.config.LatchQueueOptions;
import io.github.cocosip.latchq.internal.DefaultLatchQueueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration: binds {@code latchq.*} properties into {@link LatchQueueOptions} and
 * registers a {@link LatchQueueFactory} bean. The factory (and with it every created queue) is
 * closed when the application context shuts down, which performs the final checkpoint.
 */
@AutoConfiguration
@EnableConfigurationProperties(LatchQueueProperties.class)
@ConditionalOnProperty(
        prefix = "latchq",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
public class LatchQueueAutoConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(LatchQueueAutoConfiguration.class);

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(LatchQueueFactory.class)
    public LatchQueueFactory latchQueueFactory(LatchQueueProperties properties) {
        LatchQueueOptions options = new LatchQueueOptions();
        options.setRootPath(properties.getRootPath());
        properties
                .getConfigurations()
                .forEach((name, configuration) -> options.configure(name, c -> {}));
        // copy the bound settings over the registered configurations
        properties
                .getConfigurations()
                .forEach(
                        (name, bound) -> {
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
        options.validate();
        LOG.info("LatchQueue factory created with rootPath={}", properties.getRootPath());
        return new DefaultLatchQueueFactory(options);
    }
}
