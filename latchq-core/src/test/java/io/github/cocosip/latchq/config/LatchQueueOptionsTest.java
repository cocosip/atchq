package io.github.cocosip.latchq.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.latchq.exception.LatchQInvalidConfigurationException;
import org.junit.jupiter.api.Test;

class LatchQueueOptionsTest {

    @Test
    void validOptionsPassValidation() {
        LatchQueueOptions options = new LatchQueueOptions();
        options.setRootPath("/data/latchq");
        options.configure("events", c -> c.setFileName("events"));
        assertThatCode(options::validate).doesNotThrowAnyException();
    }

    @Test
    void blankRootPathFailsValidation() {
        LatchQueueOptions options = new LatchQueueOptions();
        options.configure("events", c -> c.setFileName("events"));
        assertThatThrownBy(options::validate)
                .isInstanceOf(LatchQInvalidConfigurationException.class)
                .hasMessageContaining("rootPath");
    }

    @Test
    void blankFileNameFailsValidationAndReportsTheQueueName() {
        LatchQueueOptions options = new LatchQueueOptions();
        options.setRootPath("/data/latchq");
        options.configure("events", c -> {});
        assertThatThrownBy(options::validate)
                .isInstanceOf(LatchQInvalidConfigurationException.class)
                .hasMessageContaining("events")
                .hasMessageContaining("fileName");
    }

    @Test
    void negativeIntervalsFailValidation() {
        LatchQueueOptions options = new LatchQueueOptions();
        options.setRootPath("/data/latchq");
        options.configure(
                "events",
                c -> {
                    c.setFileName("events");
                    c.setSyncIntervalMillis(-1);
                });
        assertThatThrownBy(options::validate)
                .isInstanceOf(LatchQInvalidConfigurationException.class);
    }

    @Test
    void zeroEssentialBackgroundIntervalsFailValidation() {
        LatchQueueOptions options = new LatchQueueOptions();
        options.setRootPath("/data/latchq");
        options.configure(
                "events",
                c -> {
                    c.setFileName("events");
                    // a zero interval would turn the background loop into a busy spin
                    c.setCheckpointIntervalMillis(0);
                });
        assertThatThrownBy(options::validate)
                .isInstanceOf(LatchQInvalidConfigurationException.class)
                .hasMessageContaining("must be positive");
    }

    @Test
    void zeroSyncIntervalRemainsLegalBecauseItDisablesPeriodicSyncing() {
        LatchQueueOptions options = new LatchQueueOptions();
        options.setRootPath("/data/latchq");
        options.configure(
                "events",
                c -> {
                    c.setFileName("events");
                    c.setSyncIntervalMillis(0);
                });
        assertThatCode(options::validate).doesNotThrowAnyException();
    }

    @Test
    void unknownQueueNameFailsFastInsteadOfFallingBack() {
        LatchQueueOptions options = new LatchQueueOptions();
        options.setRootPath("/data/latchq");
        options.configure("events", c -> c.setFileName("events"));
        // Deliberate deviation from the FASTER reference behaviour: no silent default fallback.
        assertThatThrownBy(() -> options.getConfiguration("evnets"))
                .isInstanceOf(LatchQInvalidConfigurationException.class)
                .hasMessageContaining("evnets")
                .hasMessageContaining("events");
    }

    @Test
    void configureAmendsAnExistingConfiguration() {
        LatchQueueOptions options = new LatchQueueOptions();
        options.configure("events", c -> c.setFileName("first"));
        options.configure("events", c -> c.setPreReadCapacity(99));
        assertThat(options.getConfiguration("events").getFileName()).isEqualTo("first");
        assertThat(options.getConfiguration("events").getPreReadCapacity()).isEqualTo(99);
    }
}
