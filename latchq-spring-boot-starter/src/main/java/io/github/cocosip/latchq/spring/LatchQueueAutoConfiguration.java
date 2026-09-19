package io.github.cocosip.latchq.spring;

import io.github.cocosip.latchq.LatchQueueFactory;
import io.github.cocosip.latchq.config.LatchQueueOptions;
import io.github.cocosip.latchq.internal.DefaultLatchQueueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
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
        LatchQueueOptions options = properties.toOptions();
        options.validate();
        LOG.info("LatchQueue factory created with rootPath={}", properties.getRootPath());
        return new DefaultLatchQueueFactory(options);
    }

    /** Health indicator support, only wired when Spring Boot Actuator is on the classpath. */
    @ConditionalOnClass(name = "org.springframework.boot.actuate.health.HealthIndicator")
    static class LatchQueueHealthConfiguration {

        @Bean
        @ConditionalOnMissingBean(name = "latchQueueHealthIndicator")
        org.springframework.boot.actuate.health.HealthIndicator latchQueueHealthIndicator(
                LatchQueueFactory factory) {
            return new LatchQueueHealthIndicator(factory);
        }
    }
}
