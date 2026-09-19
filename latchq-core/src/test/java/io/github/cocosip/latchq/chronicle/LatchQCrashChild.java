package io.github.cocosip.latchq.chronicle;

import io.github.cocosip.latchq.LatchQueue;
import io.github.cocosip.latchq.LatchQueueBuilder;
import io.github.cocosip.latchq.LogEntry;
import java.time.Duration;

/** LatchQ-level crash child: writes and commits N messages via the LatchQueue API, then halts. */
public final class LatchQCrashChild {

    public static void main(String[] args) throws Exception {
        String root = args[0];
        int normal = Integer.parseInt(args[1]);
        LatchQueue<CrashPayload> queue =
                LatchQueueBuilder.create("crash", CrashPayload.class)
                        .rootPath(root)
                        .configuration(
                                c -> {
                                    c.setFileName("crash");
                                    c.setCheckpointIntervalMillis(50);
                                    // fast merge so the truncate index advances before the halt
                                    c.setCompleteIntervalMillis(50);
                                    c.setBuilderCustomizer(b -> b.rollCycle(new TinyRollCycle()));
                                })
                        .build();
        for (int i = 0; i < normal; i++) {
            queue.write(new CrashPayload(i));
        }
        var reads = queue.read(normal, Duration.ofSeconds(10));
        if (reads.size() != normal) {
            System.out.println("child: only read " + reads.size() + " of " + normal);
            System.exit(3);
        }
        queue.commit(reads.stream().map(LogEntry::position).toList());
        System.out.println("child: wrote and committed " + normal + " messages");
        System.out.flush();
        // give the checkpoint task one tick to persist the truncate index, then hard-kill
        Thread.sleep(500);
        Runtime.getRuntime().halt(7);
    }

    private LatchQCrashChild() {}
}
