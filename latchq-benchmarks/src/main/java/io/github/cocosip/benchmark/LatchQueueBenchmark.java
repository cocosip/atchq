package io.github.cocosip.benchmark;

import io.github.cocosip.latchq.LatchQueue;
import io.github.cocosip.latchq.LatchQueueBuilder;
import io.github.cocosip.latchq.LogEntry;
import java.nio.file.Path;
import java.time.Duration;
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

/**
 * JMH benchmarks for LatchQueue. Run (JDK 21 needs the Chronicle JVM opens, see
 * docs/spike-notes.md):
 *
 * <pre>
 * MAVEN_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED ..." \
 *   mvnw -pl latchq-benchmarks package exec:java
 * </pre>
 *
 * Optionally restrict to one benchmark: -Dexec.args="'write'"
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 3)
@Fork(1)
public class LatchQueueBenchmark {

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
    }

    /** Single-threaded write throughput (per write call, one message). */
    @Benchmark
    public long write() {
        return queue.write(new Event(nextId++, "p".repeat(100)));
    }

    /** Batch write throughput (per batchWrite call, 100 messages). */
    @Benchmark
    public java.util.List<Long> batchWrite() {
        var values = new java.util.ArrayList<Event>(100);
        for (int i = 0; i < 100; i++) {
            values.add(new Event(nextId++, "p".repeat(100)));
        }
        return queue.batchWrite(values);
    }

    /** Concurrent consumer throughput: drains whatever the scan thread has delivered. */
    @Benchmark
    public int read() {
        var batch = queue.read(100, Duration.ofMillis(1));
        int n = batch.size();
        if (n > 0) {
            // commit so progress advances; keeps the pipeline flowing across iterations
            for (LogEntry<Event> entry : batch) {
                queue.commit(java.util.List.of(entry.position()));
            }
        }
        return n;
    }
}
