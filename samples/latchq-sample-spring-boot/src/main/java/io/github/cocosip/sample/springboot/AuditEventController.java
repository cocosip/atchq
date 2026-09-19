package io.github.cocosip.sample.springboot;

import io.github.cocosip.latchq.ExportResult;
import io.github.cocosip.latchq.LatchQueueMetrics;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Business-owned HTTP layer on top of the LatchQueue API. */
@RestController
@RequestMapping("/audit")
public class AuditEventController {

    private final AuditEventConsumer consumer;

    public AuditEventController(AuditEventConsumer consumer) {
        this.consumer = consumer;
    }

    public record PublishRequest(long id, String action) {}

    @PostMapping("/events")
    public Map<String, Object> publish(@RequestBody PublishRequest request) {
        long index =
                consumer.publish(new AuditEventConsumer.AuditEvent(request.id(), request.action()));
        return Map.of("index", index);
    }

    @GetMapping("/metrics")
    public LatchQueueMetrics metrics() {
        return consumer.metrics();
    }

    /** Triggers an export and returns the generated JSONL file paths for download. */
    @PostMapping("/export")
    public ExportResult export() {
        return consumer.exportAll("target/export");
    }
}
