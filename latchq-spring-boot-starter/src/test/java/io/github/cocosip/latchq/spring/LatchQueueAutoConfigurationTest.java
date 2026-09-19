package io.github.cocosip.latchq.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.latchq.LatchQueue;
import io.github.cocosip.latchq.LatchQueueFactory;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Boots a real application context from application-style properties and exercises the factory
 * bean.
 */
@SpringBootTest(
        webEnvironment = WebEnvironment.NONE,
        classes = LatchQueueAutoConfigurationTest.TestApp.class,
        properties = {
            "latchq.root-path=${java.io.tmpdir}/latchq-starter-test",
            "latchq.configurations.events.file-name=events",
            "latchq.configurations.events.sync-interval-millis=500"
        })
class LatchQueueAutoConfigurationTest {

    @Autowired LatchQueueFactory factory;

    @Autowired LatchQueueProperties properties;

    @Test
    void propertiesAreBoundFromYamlStyleProperties() {
        assertThat(properties.getRootPath()).contains("latchq-starter-test");
        assertThat(properties.getConfigurations()).containsKey("events");
        assertThat(properties.getConfigurations().get("events").getFileName()).isEqualTo("events");
        assertThat(properties.getConfigurations().get("events").getSyncIntervalMillis())
                .isEqualTo(500);
    }

    @Test
    void factoryBeanCreatesAndCachesQueues() {
        LatchQueue<SampleEvent> first = factory.getOrCreate("events", SampleEvent.class);
        LatchQueue<SampleEvent> second = factory.getOrCreate("events", SampleEvent.class);
        assertThat(first).isSameAs(second);
        assertThat(first.isInitialized()).isTrue();
        assertThat(first.write(new SampleEvent(1, "hello"))).isPositive();
    }

    @Test
    void contextShutdownClosesTheFactory() {
        // a separate programmatic context so the shared test context is not poisoned
        ConfigurableApplicationContext fresh =
                new SpringApplication(TestApp.class)
                        .run(
                                "--spring.main.web-application-type=none",
                                "--latchq.root-path=" + properties.getRootPath(),
                                "--latchq.configurations.shutdown.file-name=shutdown");
        LatchQueueFactory freshFactory = fresh.getBean(LatchQueueFactory.class);
        freshFactory.getOrCreate("shutdown", SampleEvent.class).write(new SampleEvent(1, "x"));

        fresh.close();

        // the destroy hook closed every queue behind the factory
        assertThatThrownBy(() -> freshFactory.getOrCreate("shutdown", SampleEvent.class))
                .isInstanceOf(Exception.class);
        assertThat(fresh.isActive()).isFalse();
    }

    record SampleEvent(int id, String text) {}

    @SpringBootApplication
    static class TestApp {}
}
