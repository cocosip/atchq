package io.github.cocosip.latchq.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.latchq.exception.LatchQInvalidConfigurationException;
import net.openhft.chronicle.queue.RollCycle;
import org.junit.jupiter.api.Test;

class RollCycleResolverTest {

    @Test
    void listsAtLeastOneAvailableRollCycle() {
        // Documents what the current Chronicle version actually exposes; the config default
        // must be one of these names.
        System.out.println(
                "[resolver] available roll cycles: " + RollCycleResolver.availableNames());
        assertThat(RollCycleResolver.availableNames()).isNotEmpty();
    }

    @Test
    void resolvesEveryAvailableNameCaseInsensitively() {
        for (String name : RollCycleResolver.availableNames()) {
            RollCycle resolved = RollCycleResolver.resolve(name.toLowerCase(java.util.Locale.ROOT));
            assertThat(resolved).isNotNull();
        }
    }

    @Test
    void resolvesTheConfiguredDefault() {
        // Must stay in sync with LatchQueueConfiguration's default.
        assertThat(
                        RollCycleResolver.resolve(
                                new io.github.cocosip.latchq.config.LatchQueueConfiguration()
                                        .getRollCycle()))
                .isNotNull();
    }

    @Test
    void unknownNameFailsFastWithAvailableNames() {
        assertThatThrownBy(() -> RollCycleResolver.resolve("NO_SUCH_CYCLE"))
                .isInstanceOf(LatchQInvalidConfigurationException.class)
                .hasMessageContaining("NO_SUCH_CYCLE")
                .hasMessageContaining("available");
    }
}
