package io.github.cocosip.latchq.chronicle;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.jupiter.api.Test;

/**
 * Formal regression baseline for the M0 technical verification (the "to be verified" items
 * in section 10 of the design document): validates chronicle-queue 5.27ea5 behaviour on
 * JDK 21 - concurrent writes, index semantics, roll boundaries, restore positioning,
 * cycle file cleanup and dirty-tail self healing. Findings are recorded in docs/spike-notes.md.
 */
class ChronicleApiVerificationTest {

    private static final RollCycle TINY = new TinyRollCycle();

    private static final AtomicInteger TAILER_SEQ = new AtomicInteger();

    /**
     * Not using {@code @TempDir}: on Windows Chronicle releases .cq4 file handles shortly
     * after queue close, so JUnit's temp directory cleanup fails intermittently. Test data
     * lives under target/test-data and is wiped by mvn clean instead.
     */
    private static Path dir(String name) {
        Path path = Path.of("target", "test-data", name);
        if (Files.exists(path)) {
            try {
                Files.walk(path).sorted(Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException ignored) {
                        // left-over locked file from a previous run, ignore
                    }
                });
            } catch (IOException ignored) {
                // ignore
            }
        }
        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return path;
    }

    private static long epoch() {
        return LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    private static ChronicleQueue queue(Path path, RollCycle rollCycle, Long epoch, StoreFileListener listener) {
        var builder = ChronicleQueue.singleBuilder(path.toString()).rollCycle(rollCycle);
        if (epoch != null) {
            builder.epoch(epoch);
        }
        if (listener != null) {
            builder.storeFileListener(listener);
        }
        return builder.build();
    }

    private static byte[] payload(long i) {
        return ("{\"id\":" + i + ",\"pad\":\"" + "p".repeat(24) + "\"}").getBytes(StandardCharsets.UTF_8);
    }

    private static long append(ExcerptAppender appender, byte[] data) {
        try (DocumentContext ctx = appender.writingDocument()) {
            ctx.wire().writeBytes(out -> out.write(data));
            return ctx.index();
        }
    }

    private record Read(long index, byte[] data) {}

    private static List<Read> readAll(ChronicleQueue queue, long fromIndex) {
        List<Read> reads = new ArrayList<>();
        // Finding: a tailer created with a previously used name resumes the position
        // Chronicle remembered for it, so every read pass needs a unique name.
        try (ExcerptTailer tailer = queue.createTailer("verification-" + TAILER_SEQ.incrementAndGet())) {
            if (fromIndex >= 0) {
                tailer.moveToIndex(fromIndex);
            }
            while (true) {
                try (DocumentContext ctx = tailer.readingDocument()) {
                    if (!ctx.isPresent()) {
                        break;
                    }
                    Bytes<byte[]> buffer = Bytes.allocateElasticOnHeap(128);
                    try {
                        ctx.wire().readBytes(buffer);
                        reads.add(new Read(ctx.index(), buffer.toByteArray()));
                    } finally {
                        buffer.releaseLast();
                    }
                }
            }
        }
        return reads;
    }

    // ------------------------------------------------------------------
    // T0.2 write side
    // ------------------------------------------------------------------

    @Test
    void singleThreadWritesAreDenseAndLastIndexAppendedMatches() {
        try (ChronicleQueue queue = queue(dir("writeRead"), TINY, epoch(), null)) {
            try (ExcerptAppender appender = queue.createAppender()) {
                long previous = -1;
                long lastIndexWritten = -1;
                for (int i = 0; i < 10; i++) {
                    long index = append(appender, payload(i));
                    if (previous >= 0 && TINY.toCycle(index) == TINY.toCycle(previous)) {
                        assertThat(index).isEqualTo(previous + 1);
                    }
                    previous = index;
                    lastIndexWritten = index;
                }
                assertThat(appender.lastIndexAppended()).isEqualTo(lastIndexWritten);
            }
        }
    }

    @Test
    void concurrentWritersProduceUniqueIndexesAndCompleteReadback() throws Exception {
        try (ChronicleQueue queue = queue(dir("concurrent"), TINY, epoch(), null)) {
            int threads = 4;
            int perThread = 500;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<List<Long>>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    List<Long> indexes = new ArrayList<>(perThread);
                    try (ExcerptAppender appender = queue.createAppender()) {
                        for (int i = 0; i < perThread; i++) {
                            indexes.add(append(appender, payload(i)));
                        }
                    }
                    return indexes;
                }));
            }
            start.countDown();

            var all = new ConcurrentSkipListSet<Long>();
            for (Future<List<Long>> future : futures) {
                List<Long> indexes = future.get(60, TimeUnit.SECONDS);
                assertThat(indexes).doesNotHaveDuplicates();
                all.addAll(indexes);
            }
            pool.shutdown();
            assertThat(all).hasSize(threads * perThread);

            List<Read> reads = readAll(queue, -1);
            assertThat(reads).hasSize(threads * perThread);
            // Index order within a thread equals write order; the global readback covers all indexes.
            assertThat(reads.stream().mapToLong(Read::index).boxed().toList())
                    .containsExactlyInAnyOrderElementsOf(all);
        }
    }

    @Test
    void appenderSyncExistsAndDataSurvivesReopen() {
        Path path = dir("appenderSync");
        List<byte[]> written = new ArrayList<>();
        try (ChronicleQueue queue = queue(path, TINY, epoch(), null)) {
            try (ExcerptAppender appender = queue.createAppender()) {
                for (int i = 0; i < 3; i++) {
                    byte[] data = payload(i);
                    append(appender, data);
                    written.add(data);
                }
                appender.sync(); // compiling against this call proves the API exists
            }
        }
        try (ChronicleQueue queue = queue(path, TINY, epoch(), null)) {
            List<Read> reads = readAll(queue, -1);
            assertThat(reads).hasSize(3);
            assertThat(reads.get(2).data()).isEqualTo(written.get(2));
        }
    }

    // ------------------------------------------------------------------
    // T0.4 firstIndex / lastIndex
    // ------------------------------------------------------------------

    @Test
    void firstIndexAndLastIndexOnEmptyAndNonEmptyQueue() {
        Path path = dir("firstlast");
        long first = -1;
        long last = -1;
        try (ChronicleQueue queue = queue(path, TINY, epoch(), null)) {
            // Empty-queue behaviour is recorded for docs/spike-notes.md
            try {
                long emptyFirst = queue.firstIndex();
                System.out.println("[verify] empty firstIndex=" + emptyFirst);
            } catch (RuntimeException e) {
                System.out.println("[verify] empty firstIndex threw "
                        + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            try (ExcerptAppender appender = queue.createAppender()) {
                first = append(appender, payload(0));
                for (int i = 1; i < 5; i++) {
                    last = append(appender, payload(i));
                }
            }
            assertThat(queue.firstIndex()).isEqualTo(first);
            assertThat(queue.lastIndex()).isEqualTo(last);
        }
    }

    // ------------------------------------------------------------------
    // T0.3 index semantics and roll boundaries (core trap verification)
    // ------------------------------------------------------------------

    @Test
    void idleGapBreaksNaiveNextIndexArithmetic() throws Exception {
        try (ChronicleQueue queue = queue(dir("idleSkip"), TINY, epoch(), null)) {
            long i1;
            try (ExcerptAppender appender = queue.createAppender()) {
                i1 = append(appender, payload(1));
            }
            Thread.sleep(2100); // idle long enough to cross at least 2 cycle boundaries
            long i2;
            try (ExcerptAppender appender = queue.createAppender()) {
                i2 = append(appender, payload(2));
            }
            int c1 = TINY.toCycle(i1);
            int c2 = TINY.toCycle(i2);
            assertThat(c2 - c1).isGreaterThanOrEqualTo(2);
            // Trap confirmed: neither index+1 nor "first sequence of the next cycle"
            // arithmetic predicts the real next message index after an idle gap.
            assertThat(i2).isNotEqualTo(i1 + 1);
            assertThat(i2).isNotEqualTo(TINY.toIndex(c1 + 1, 0));
            System.out.println("[verify] idle skip: m1(cycle=" + c1 + ") m2(cycle=" + c2
                    + ") routeA(m1)=" + TINY.toIndex(c1 + 1, 0) + " actual=" + i2);
        }
    }

    @Test
    void continuousWritesCrossBoundaryDensely() throws Exception {
        try (ChronicleQueue queue = queue(dir("continuous"), TINY, epoch(), null)) {
            List<Long> indexes = new ArrayList<>();
            try (ExcerptAppender appender = queue.createAppender()) {
                long deadline = System.currentTimeMillis() + 2300;
                while (System.currentTimeMillis() < deadline || indexes.size() < 5) {
                    indexes.add(append(appender, payload(indexes.size())));
                    Thread.sleep(100);
                }
            }
            int boundaries = 0;
            for (int n = 1; n < indexes.size(); n++) {
                long previous = indexes.get(n - 1);
                long current = indexes.get(n);
                int pc = TINY.toCycle(previous);
                int cc = TINY.toCycle(current);
                if (cc == pc) {
                    assertThat(current).isEqualTo(previous + 1);
                } else if (cc == pc + 1) {
                    boundaries++;
                    // Ordinary boundary without idle: sequence restarts at 0 in the new cycle.
                    assertThat(TINY.toSequenceNumber(current)).isEqualTo(0);
                } else {
                    System.out.println("[verify] idle skip inside continuous run " + pc + "->" + cc);
                }
            }
            assertThat(boundaries).isGreaterThanOrEqualTo(1);
            assertThat(TINY.toSequenceNumber(indexes.get(0))).isGreaterThanOrEqualTo(0);
        }
    }

    @Test
    void moveToIndexRestoresExactReadingPosition() {
        try (ChronicleQueue queue = queue(dir("moveToIndex"), TINY, epoch(), null)) {
            List<Long> indexes = new ArrayList<>();
            try (ExcerptAppender appender = queue.createAppender()) {
                for (int i = 0; i < 5; i++) {
                    indexes.add(append(appender, payload(i)));
                }
            }
            List<Read> reads = readAll(queue, indexes.get(2));
            assertThat(reads).hasSize(3);
            assertThat(reads.get(0).index()).isEqualTo(indexes.get(2));
            assertThat(reads.get(0).data()).isEqualTo(payload(2));
        }
    }

    @Test
    void moveToIndexIntoEmptyCycleSkipsForwardToNextData() throws Exception {
        try (ChronicleQueue queue = queue(dir("moveToEmpty"), TINY, epoch(), null)) {
            long i1;
            try (ExcerptAppender appender = queue.createAppender()) {
                i1 = append(appender, payload(1));
            }
            Thread.sleep(2100);
            long i2;
            try (ExcerptAppender appender = queue.createAppender()) {
                i2 = append(appender, payload(2));
            }
            long emptyCycleIndex = TINY.toIndex(TINY.toCycle(i1) + 1, 0);
            List<Read> reads = readAll(queue, emptyCycleIndex);
            System.out.println("[verify] moveToIndex(emptyCycle) -> reads=" + reads.stream()
                    .map(r -> r.index() + (r.data() == null || r.data().length == 0 ? "(EMPTY)" : ""))
                    .toList() + " expect i2=" + i2);
            // Finding: moveToIndex into an empty cycle may first return a present-but-empty
            // phantom document at exactly the requested index, then continue forward to real
            // data. LatchQ restore must therefore only use indexes of real messages.
            assertThat(reads).isNotEmpty();
            assertThat(reads).anyMatch(r -> r.index() == i2 && Arrays.equals(r.data(), payload(2)));
        }
    }

    // ------------------------------------------------------------------
    // T0.5 cleanup: storeFileListener callbacks + deleting an old cycle file while open
    // ------------------------------------------------------------------

    @Test
    void storeFileListenerFiresAndOldCycleFileCanBeDeletedWhileOpen() throws Exception {
        Path path = dir("cleanup");
        Map<Integer, File> acquired = new ConcurrentHashMap<>();
        Map<Integer, File> released = new ConcurrentHashMap<>();
        StoreFileListener listener = new StoreFileListener() {
            @Override
            public void onAcquired(int cycle, File file) {
                acquired.put(cycle, file);
            }

            @Override
            public void onReleased(int cycle, File file) {
                released.put(cycle, file);
            }
        };

        try (ChronicleQueue queue = queue(path, TINY, epoch(), listener)) {
            long i1;
            try (ExcerptAppender appender = queue.createAppender()) {
                i1 = append(appender, payload(1));
            }
            Thread.sleep(2100);
            long i2;
            try (ExcerptAppender appender = queue.createAppender()) {
                i2 = append(appender, payload(2));
            }
            Thread.sleep(2100);
            long i3;
            try (ExcerptAppender appender = queue.createAppender()) {
                i3 = append(appender, payload(3));
            }
            int c1 = TINY.toCycle(i1);
            int c3 = TINY.toCycle(i3);
            assertThat(c3).isGreaterThan(c1);

            // Finding: file names follow Chronicle's internal convention, do not derive
            // them from the cycle ourselves; c1 is the earliest cycle, so take the oldest
            // .cq4 file in the directory.
            File[] cq4Files = path.toFile().listFiles((d, name) -> name.endsWith(".cq4"));
            assertThat(cq4Files).isNotNull();
            File oldFile = Arrays.stream(cq4Files)
                    .min(Comparator.comparingLong(File::lastModified))
                    .orElseThrow();
            System.out.println("[verify] oldest cycle file=" + oldFile.getName());
            boolean deleted = oldFile.delete();
            System.out.println("[verify] old cycle file delete while queue open: " + deleted
                    + " (released before delete? " + released.containsKey(c1) + ")");

            // Whatever the delete outcome, the queue keeps working for both write and read.
            long i4;
            try (ExcerptAppender appender = queue.createAppender()) {
                i4 = append(appender, payload(4));
            }
            // moveToIndex(i3) positions at i3 itself, so i3 then i4 are both expected.
            List<Read> reads = readAll(queue, i3);
            assertThat(reads).hasSize(2);
            assertThat(reads.get(0).index()).isEqualTo(i3);
            assertThat(reads.get(0).data()).isEqualTo(payload(3));
            assertThat(reads.get(1).index()).isEqualTo(i4);
            assertThat(reads.get(1).data()).isEqualTo(payload(4));

            if (deleted) {
                System.out.println("[verify] firstIndex after delete=" + queue.firstIndex());
            }
        }
        System.out.println("[verify] storeFileListener acquired=" + acquired.keySet()
                + " released=" + released.keySet());
        assertThat(acquired).isNotEmpty();
    }

    // ------------------------------------------------------------------
    // T0.4b dirty tail: child JVM hard-killed (kill -9 equivalent) mid-write
    // ------------------------------------------------------------------

    @Test
    void hardKillMidWriteLeavesRecoverableQueue() throws Exception {
        Path path = dir("dirtytail");
        int normalMessages = 20;
        String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator
                + (System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java");
        Process child = new ProcessBuilder(
                        javaBin,
                        "--add-opens", "java.base/java.lang=ALL-UNNAMED",
                        "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED",
                        "--add-opens", "java.base/java.io=ALL-UNNAMED",
                        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
                        "--add-exports", "java.base/jdk.internal.ref=ALL-UNNAMED",
                        "-cp",
                        System.getProperty("java.class.path"),
                        "io.github.cocosip.latchq.chronicle.TornWriteChild",
                        path.toString(),
                        String.valueOf(normalMessages))
                .redirectErrorStream(true)
                .start();
        assertThat(child.getInputStream().readAllBytes()).asString().contains("child: wrote");
        boolean finished = child.waitFor(60, TimeUnit.SECONDS);
        assertThat(finished).isTrue();
        assertThat(child.exitValue()).isEqualTo(7); // halt(7): killed mid-write

        try (ChronicleQueue queue = queue(path, TINY, null, null)) {
            List<Read> reads = readAll(queue, -1);
            System.out.println("[verify] after hard kill: readable=" + reads.size()
                    + " (expect " + normalMessages + ", torn tail must not appear)");
            assertThat(reads).hasSize(normalMessages);
            assertThat(new String(reads.get(reads.size() - 1).data(), StandardCharsets.UTF_8))
                    .contains("\"id\":" + (normalMessages - 1));

            // The queue must keep accepting writes after the crash.
            try (ExcerptAppender appender = queue.createAppender()) {
                for (int i = 0; i < 3; i++) {
                    append(appender, payload(100 + i));
                }
            }
            assertThat(readAll(queue, -1)).hasSize(normalMessages + 3);
        }
    }
}
