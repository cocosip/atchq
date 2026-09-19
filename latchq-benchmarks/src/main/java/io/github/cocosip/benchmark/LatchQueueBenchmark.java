package io.github.cocosip.benchmark;

import io.github.cocosip.latchq.LatchQueue;
import io.github.cocosip.latchq.LatchQueueBuilder;
import io.github.cocosip.latchq.LogEntry;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * JMH benchmarks for LatchQueue: single-write throughput, batch-write throughput and a read-commit
 * pipeline benchmark that feeds itself (when drained it writes a fresh batch, so the numbers
 * measure real end-to-end read+commit cost).
 *
 * <p>Run:
 *
 * <pre>
 * mvnw -pl latchq-benchmarks package exec:java
 * </pre>
 *
 * Chronicle needs the JDK 21 JVM opens; the main method appends them to the forked JMH JVM
 * automatically. Restrict to one benchmark with -Dexec.args="write".
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 2)
@Fork(1)
public class LatchQueueBenchmark {

    private static final int BATCH = 100;

    /** self-feed size for the read benchmark: big enough to hide the scan catch-up latency */
    private static final int SELF_FEED = 2000;

    record Event(int id, String pad) {}

    private LatchQueue<Event> queue;
    private Path dataDir;
    private int nextId;

    @Setup(Level.Iteration)
    public void setUp() {
        dataDir = Path.of("target", "benchmark-data", String.valueOf(System.nanoTime()));
        queue =
                LatchQueueBuilder.create("bench", Event.class)
                        .rootPath(dataDir.toString())
                        .configuration(c -> c.setFileName("bench"))
                        .build();
        nextId = 0;
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        queue.close();
        deleteRecursively(dataDir);
    }

    private static void deleteRecursively(Path path) {
        if (path == null || !java.nio.file.Files.exists(path)) {
            return;
        }
        try {
            java.nio.file.Files.walk(path)
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(
                            p -> {
                                try {
                                    java.nio.file.Files.delete(p);
                                } catch (java.io.IOException ignored) {
                                    // benchmark cleanup best effort
                                }
                            });
        } catch (java.io.IOException ignored) {
            // ignore
        }
    }

    /** Single-message write throughput (ops = one write call). */
    @Benchmark
    public long write() {
        return queue.write(new Event(nextId++, "p".repeat(100)));
    }

    /** Batch write throughput (ops = one batchWrite call of 100 messages). */
    @Benchmark
    public List<Long> batchWrite() {
        var values = new ArrayList<Event>(BATCH);
        for (int i = 0; i < BATCH; i++) {
            values.add(new Event(nextId++, "p".repeat(100)));
        }
        return queue.batchWrite(values);
    }

    /**
     * Read+commit pipeline throughput (ops = one read call of up to 100 entries). Self-feeding:
     * once the scan thread is caught up the benchmark writes a fresh batch of 2000, so the reads
     * mostly drain real backlogs instead of measuring the catch-up poll latency. Report messages/s
     * as ops × 100 (the read batch size).
     */
    @Benchmark
    public int read() {
        var batch = queue.read(BATCH, Duration.ofMillis(50));
        int n = batch.size();
        if (n > 0) {
            queue.commit(batch.stream().map(LogEntry::position).toList());
        } else {
            var values = new ArrayList<Event>(SELF_FEED);
            for (int i = 0; i < SELF_FEED; i++) {
                values.add(new Event(nextId++, "p".repeat(100)));
            }
            queue.batchWrite(values);
        }
        return n;
    }

    public static void main(String[] args) throws Exception {
        var builder =
                new OptionsBuilder()
                        .include(LatchQueueBenchmark.class.getSimpleName())
                        .jvmArgsAppend(
                                "--add-opens=java.base/java.lang=ALL-UNNAMED",
                                "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
                                "--add-opens=java.base/java.io=ALL-UNNAMED",
                                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                                "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED");
        if (args.length > 0) {
            builder.include(".*" + args[0] + ".*");
        }
        new Runner(builder.build()).run();
    }
}
