package io.github.cocosip.latchq.spring;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.latchq.config.LatchQueueConfiguration;
import io.github.cocosip.latchq.config.LatchQueueOptions;
import org.junit.jupiter.api.Test;

/**
 * Mapping backstop for the properties-to-options conversion: every per-queue setting of {@link
 * LatchQueueConfiguration} must be carried over by {@link LatchQueueProperties#toOptions()} - a
 * forgotten field here would silently run with the default instead of the configured value.
 */
class LatchQueuePropertiesMappingTest {

    @Test
    void toOptionsCopiesEveryQueueSetting() {
        LatchQueueConfiguration bound = new LatchQueueConfiguration();
        bound.setFileName("events");
        bound.setMaxMessageSizeBytes(1024);
        bound.setRollCycle("MINUTELY");
        bound.setSyncIntervalMillis(1);
        bound.setCompleteIntervalMillis(2);
        bound.setCheckpointIntervalMillis(3);
        bound.setCleanupIntervalMillis(4);
        bound.setPreReadCapacity(5);
        bound.setGapTimeoutMillis(6);
        bound.setForceCompleteGapTimeoutMillis(7);
        bound.setMaxCompletedRanges(8);

        LatchQueueProperties properties = new LatchQueueProperties();
        properties.setRootPath("root");
        properties.getConfigurations().put("events", bound);

        LatchQueueOptions options = properties.toOptions();
        assertThat(options.getRootPath()).isEqualTo("root");
        LatchQueueConfiguration mapped = options.getConfiguration("events");
        assertThat(mapped.getFileName()).isEqualTo("events");
        assertThat(mapped.getMaxMessageSizeBytes()).isEqualTo(1024);
        assertThat(mapped.getRollCycle()).isEqualTo("MINUTELY");
        assertThat(mapped.getSyncIntervalMillis()).isEqualTo(1);
        assertThat(mapped.getCompleteIntervalMillis()).isEqualTo(2);
        assertThat(mapped.getCheckpointIntervalMillis()).isEqualTo(3);
        assertThat(mapped.getCleanupIntervalMillis()).isEqualTo(4);
        assertThat(mapped.getPreReadCapacity()).isEqualTo(5);
        assertThat(mapped.getGapTimeoutMillis()).isEqualTo(6);
        assertThat(mapped.getForceCompleteGapTimeoutMillis()).isEqualTo(7);
        assertThat(mapped.getMaxCompletedRanges()).isEqualTo(8);
    }

    @Test
    void toOptionsKeepsPerQueueDefaultsWhenNothingIsBound() {
        LatchQueueProperties properties = new LatchQueueProperties();
        properties.setRootPath("root");
        properties.getConfigurations().put("events", new LatchQueueConfiguration());

        LatchQueueConfiguration mapped = properties.toOptions().getConfiguration("events");
        LatchQueueConfiguration defaults = new LatchQueueConfiguration();
        defaults.setFileName(mapped.getFileName() == null ? null : defaults.getFileName());
        assertThat(mapped.getMaxMessageSizeBytes()).isEqualTo(defaults.getMaxMessageSizeBytes());
        assertThat(mapped.getSyncIntervalMillis()).isEqualTo(defaults.getSyncIntervalMillis());
        assertThat(mapped.getCompleteIntervalMillis())
                .isEqualTo(defaults.getCompleteIntervalMillis());
        assertThat(mapped.getCheckpointIntervalMillis())
                .isEqualTo(defaults.getCheckpointIntervalMillis());
        assertThat(mapped.getCleanupIntervalMillis())
                .isEqualTo(defaults.getCleanupIntervalMillis());
        assertThat(mapped.getPreReadCapacity()).isEqualTo(defaults.getPreReadCapacity());
        assertThat(mapped.getGapTimeoutMillis()).isEqualTo(defaults.getGapTimeoutMillis());
        assertThat(mapped.getForceCompleteGapTimeoutMillis())
                .isEqualTo(defaults.getForceCompleteGapTimeoutMillis());
        assertThat(mapped.getMaxCompletedRanges()).isEqualTo(defaults.getMaxCompletedRanges());
    }
}
