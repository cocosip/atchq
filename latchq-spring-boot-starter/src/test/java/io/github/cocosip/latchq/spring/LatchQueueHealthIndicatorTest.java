package io.github.cocosip.latchq.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.cocosip.latchq.LatchQueueFactory;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;

/**
 * Actuator integration: the health indicator bean is wired automatically when actuator is on the
 * classpath and reports DOWN once the shared factory has been closed.
 */
@SpringBootTest(
        webEnvironment = WebEnvironment.NONE,
        classes = LatchQueueHealthIndicatorTest.TestApp.class,
        properties = {
            "latchq.root-path=${java.io.tmpdir}/latchq-health-test",
            "latchq.configurations.events.file-name=events"
        })
class LatchQueueHealthIndicatorTest {

    @Autowired LatchQueueFactory factory;

    @Autowired LatchQueueHealthIndicator healthIndicator;

    @Test
    void reportsUpWhileTheFactoryIsOpen() {
        assertThat(factory.isActive()).isTrue();
        assertThat(healthIndicator.health().getStatus().getCode()).isEqualTo("UP");
    }

    @Test
    void reportsDownOnceTheFactoryIsClosed() {
        LatchQueueFactory closed = mock(LatchQueueFactory.class);
        when(closed.isActive()).thenReturn(false);
        assertThat(new LatchQueueHealthIndicator(closed).health().getStatus().getCode())
                .isEqualTo("DOWN");
    }

    @SpringBootApplication
    static class TestApp {}
}
