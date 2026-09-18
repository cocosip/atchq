package io.github.cocosip.latchq.internal;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.cocosip.latchq.ExportResult;
import io.github.cocosip.latchq.LatchQueue;
import io.github.cocosip.latchq.LatchQueueMetrics;
import io.github.cocosip.latchq.LogEntry;
import io.github.cocosip.latchq.LogEntryList;
import io.github.cocosip.latchq.Position;
import io.github.cocosip.latchq.config.LatchQueueConfiguration;
import io.github.cocosip.latchq.config.LatchQueueOptions;
import io.github.cocosip.latchq.exception.LatchQException;
import io.github.cocosip.latchq.exception.LatchQNotInitializedException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.wire.DocumentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Core {@link LatchQueue} implementation on Chronicle Queue.
 *
 * <p>Concurrency model (see the design document, chapter 6): a single background scan thread
 * (virtual) owns the only tailer, reads messages sequentially and hands raw entries to consumers
 * through a bounded blocking queue with backpressure (put blocks, nothing is lost). Writers each
 * use their own appender obtained via a ThreadLocal. Consumption progress is tracked as {@code
 * truncateBeforeIndex} - the index of the first message not yet known to be processed - and merged
 * from out-of-order commits by the background complete task (M2).
 *
 * <p>nextIndex derivation follows the M0 decision (look-ahead): the scan thread holds back the most
 * recently read message until the next one arrives and stamps its real index, so every delivered
 * position is chained to the actual next message even across roll-cycle boundaries.
 */
public final class DefaultLatchQueue<T> implements LatchQueue<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultLatchQueue.class);

    /** Poll interval of the scan thread when the queue is caught up. */
    private static final long SCAN_POLL_MILLIS = 1;

    /** Sleep of the scan thread after a failed iteration, prevents rapid failure loops. */
    private static final long SCAN_ERROR_BACKOFF_MILLIS = 1000;

    /** Chronicle 5.27ea5 default appender block size; caps a single message at blockSize/2 - 4. */
    private static final long DEFAULT_BLOCK_SIZE_BYTES = 16L * 1024 * 1024;

    private final String name;
    private final Class<T> type;
    private final LatchQueueOptions options;
    private final LatchQueueConfiguration configuration;
    private final ObjectMapper objectMapper = newObjectMapper();
    private final Path queueDirectory;

    private final Object lifecycleLock = new Object();
    private volatile boolean initialized;
    private volatile boolean closed;

    private ChronicleQueue queue;
    private Thread scanThread;
    private volatile boolean running;

    private final ArrayBlockingQueue<BufferedLogEntry> pending;
    private final ReadAheadStamper stamper = new ReadAheadStamper();
    private final ThreadLocal<ExcerptAppender> appenderThreadLocal = new ThreadLocal<>();

    /**
     * Provisional nextIndex handed out with the tail entry while the queue was caught up, or -1.
     * When the real next message arrives at a different index (a roll happened while idle), an
     * empty-gap correction is recorded so the M2 merge skips the empty range immediately instead of
     * waiting for the force-skip timeout.
     */
    private final AtomicLong provisionalNextIndex = new AtomicLong(-1);

    /** Empty-gap corrections recorded by the scan thread: gapStart -&gt; next real index. */
    private final java.util.concurrent.ConcurrentHashMap<Long, Long> emptyGapCorrections =
            new java.util.concurrent.ConcurrentHashMap<>();

    /** Safety bound for the corrections map; exceeding it means the merge loop is not consuming. */
    private static final int MAX_EMPTY_GAP_CORRECTIONS = 1024;

    /** Index of the first message not yet known to be processed; -1 while unknown. */
    private final AtomicLong truncateBeforeIndex = new AtomicLong(-1);

    // Metrics; the gap-related counters are populated by the complete task added in M2.
    private final LongAdder totalWriteCount = new LongAdder();
    private final LongAdder totalReadCount = new LongAdder();
    private final LongAdder totalCommittedRanges = new LongAdder();
    private final AtomicLong currentGapCount = new AtomicLong();
    private final AtomicLong largestGapSize = new AtomicLong();

    public DefaultLatchQueue(
            String name,
            Class<T> type,
            LatchQueueOptions options,
            LatchQueueConfiguration configuration) {
        this.name = Objects.requireNonNull(name, "name");
        this.type = Objects.requireNonNull(type, "type");
        this.options = Objects.requireNonNull(options, "options");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.queueDirectory =
                TypeStorageNaming.queueDirectory(options.getRootPath(), type, configuration);
        this.pending = new ArrayBlockingQueue<>(configuration.getPreReadCapacity());
    }

    /**
     * Object mapper whose stream read constraints are unlimited: the per-message size is already
     * enforced by {@code maxMessageSizeBytes} on write, so Jackson's default 20M-char string limit
     * must not reject large (but admitted) payloads on read.
     */
    private static ObjectMapper newObjectMapper() {
        JsonFactory factory =
                JsonFactory.builder()
                        .streamReadConstraints(
                                StreamReadConstraints.builder()
                                        .maxStringLength(Integer.MAX_VALUE)
                                        .build())
                        .build();
        return new ObjectMapper(factory);
    }

    // ------------------------------------------------------------------
    // lifecycle
    // ------------------------------------------------------------------

    @Override
    public void initialize() {
        synchronized (lifecycleLock) {
            if (closed) {
                throw new LatchQException(
                        "queue '" + name + "' is closed and cannot be initialized");
            }
            if (initialized) {
                return;
            }
            options.validate();
            configuration.validate();
            try {
                Files.createDirectories(queueDirectory);
            } catch (IOException e) {
                throw new LatchQException("cannot create queue directory " + queueDirectory, e);
            }
            RollCycle rollCycle = RollCycleResolver.resolve(configuration.getRollCycle());
            // M1 finding: Chronicle caps a single write at blockSize/2 - 4 bytes, so the block
            // size is derived from maxMessageSizeBytes to make the configured bound authoritative
            long blockSize =
                    Math.max(
                            DEFAULT_BLOCK_SIZE_BYTES,
                            configuration.getMaxMessageSizeBytes() * 2 + 128);
            var builder =
                    ChronicleQueue.singleBuilder(queueDirectory.toString())
                            .rollCycle(rollCycle)
                            .blockSize(blockSize);
            if (configuration.getBuilderCustomizer() != null) {
                configuration.getBuilderCustomizer().accept(builder);
            }
            this.queue = builder.build();

            // Restore the consumer position: M1 starts from the earliest existing message;
            // M2 replaces this with the persisted checkpoint (clamped by firstIndex).
            long restore = safeFirstIndex();
            truncateBeforeIndex.set(restore);

            this.running = true;
            // The tailer is created, used and closed entirely on the scan thread: Chronicle
            // resources are single-threaded (ThreadingIllegalStateException otherwise).
            this.scanThread =
                    Thread.ofVirtual().name("latchq-scan-" + name).start(() -> scanLoop(restore));
            this.initialized = true;
            LOG.info(
                    "LatchQueue '{}' for type {} initialized, directory={}, restoreIndex={}, rollCycle={}",
                    name,
                    type.getName(),
                    queueDirectory,
                    restore,
                    rollCycle);
        }
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public void close() {
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            closed = true;
        }
        running = false;
        if (scanThread != null) {
            scanThread.interrupt();
            try {
                scanThread.join(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (queue != null) {
            // The scan thread already closed its tailer; this also closes appenders registered
            // with the queue and performs the final checkpoint flush once the task exists (M2).
            queue.close();
        }
        LOG.info("LatchQueue '{}' for type {} closed", name, type.getName());
    }

    // ------------------------------------------------------------------
    // write side
    // ------------------------------------------------------------------

    @Override
    public long write(T entity) {
        Objects.requireNonNull(entity, "entity");
        ensureUsable();
        byte[] json = serialize(entity);
        ExcerptAppender appender = appender();
        try (DocumentContext ctx = appender.writingDocument()) {
            ctx.wire().writeBytes(out -> out.write(json));
            long index = ctx.index();
            totalWriteCount.increment();
            return index;
        }
    }

    @Override
    public List<Long> batchWrite(List<T> values) {
        Objects.requireNonNull(values, "values");
        ensureUsable();
        List<Long> indexes = new ArrayList<>(values.size());
        for (T value : values) {
            indexes.add(write(value));
        }
        return indexes;
    }

    private byte[] serialize(T entity) {
        try {
            byte[] json = objectMapper.writeValueAsBytes(entity);
            // fail fast with a clear error instead of a mid-write buffer overflow
            if (json.length > configuration.getMaxMessageSizeBytes()) {
                throw new LatchQException(
                        "serialized payload of "
                                + json.length
                                + " bytes exceeds maxMessageSizeBytes "
                                + configuration.getMaxMessageSizeBytes()
                                + "; raise maxMessageSizeBytes to admit larger messages");
            }
            return json;
        } catch (IOException e) {
            throw new LatchQException("cannot serialize payload of type " + type.getName(), e);
        }
    }

    private ExcerptAppender appender() {
        ExcerptAppender appender = appenderThreadLocal.get();
        if (appender == null) {
            appender = queue.createAppender();
            appenderThreadLocal.set(appender);
        }
        return appender;
    }

    // ------------------------------------------------------------------
    // scan thread: single tailer + read-ahead stamping + bounded hand-off
    // ------------------------------------------------------------------

    private void scanLoop(long restoreIndex) {
        try (ExcerptTailer tailer =
                queue.createTailer()) { // unnamed on purpose - we own the position
            if (restoreIndex >= 0) {
                tailer.moveToIndex(restoreIndex);
            }
            while (running && !closed) {
                try {
                    if (!readOneIntoPending(tailer)) {
                        // caught up (or phantom document): brief park instead of busy spinning
                        Thread.sleep(SCAN_POLL_MILLIS);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception e) {
                    LOG.error(
                            "LatchQueue '{}' scan failed, retrying in {} ms",
                            name,
                            SCAN_ERROR_BACKOFF_MILLIS,
                            e);
                    try {
                        Thread.sleep(SCAN_ERROR_BACKOFF_MILLIS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        } catch (Exception e) {
            // only tailer creation/close lands here; the queue is shutting down anyway
            LOG.error("LatchQueue '{}' scan terminated: {}", name, e.getMessage(), e);
        }
    }

    /** Returns false when the queue was caught up (no deliverable entry was produced). */
    private boolean readOneIntoPending(ExcerptTailer tailer) throws InterruptedException {
        try (DocumentContext ctx = tailer.readingDocument()) {
            if (!ctx.isPresent()) {
                return deliverCaughtUpTail();
            }
            // read via the marshallable callback: the view exposes the exact message length, so
            // arbitrarily large messages are copied exactly (an elastic pre-sized buffer fails
            // beyond ~16 MiB)
            byte[][] holder = new byte[1][];
            ctx.wire().readBytes(in -> holder[0] = in.toByteArray());
            // M0 finding: moving to an empty cycle can yield a present-but-empty phantom
            // document at the requested index; real messages always carry payload bytes.
            if (holder[0].length == 0) {
                return false;
            }
            long index = ctx.index();
            reconcileProvisionalTail(index);
            truncateBeforeIndex.compareAndSet(-1, index);
            // offer() holds the entry back until the next real read stamps its nextIndex
            BufferedLogEntry stamped = stamper.offer(new BufferedLogEntry(holder[0], index, -1));
            if (stamped != null) {
                pending.put(stamped); // blocks when full: backpressure, no data loss
            }
            return stamped != null;
        }
    }

    /** Queue caught up: deliver the held tail entry with a provisional in-cycle nextIndex. */
    private boolean deliverCaughtUpTail() throws InterruptedException {
        BufferedLogEntry tail = stamper.deliverTailProvisional(rollCycle());
        if (tail == null) {
            return false;
        }
        provisionalNextIndex.set(tail.nextIndex());
        pending.put(tail);
        return true;
    }

    /**
     * Called on every real message read. When the previous tail entry was delivered with a
     * provisional nextIndex and the real follower landed elsewhere (roll while idle), the empty
     * range between them is recorded as a correction for the M2 merge.
     */
    private void reconcileProvisionalTail(long realIndex) {
        long provisional = provisionalNextIndex.getAndSet(-1);
        if (provisional >= 0 && provisional != realIndex) {
            if (emptyGapCorrections.size() >= MAX_EMPTY_GAP_CORRECTIONS) {
                LOG.error(
                        "LatchQueue '{}' accumulated {} empty-gap corrections; the merge task is not consuming them",
                        name,
                        emptyGapCorrections.size());
            }
            emptyGapCorrections.put(provisional, realIndex);
            LOG.info(
                    "LatchQueue '{}' recorded empty-gap correction [{}, {}) after an idle roll",
                    name,
                    provisional,
                    realIndex);
        }
    }

    private RollCycle rollCycle() {
        return queue.rollCycle();
    }

    // ------------------------------------------------------------------
    // read side
    // ------------------------------------------------------------------

    @Override
    public LogEntryList<T> read(int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("count must be positive, got: " + count);
        }
        ensureUsable();
        LogEntryList<T> result = new LogEntryList<>();
        BufferedLogEntry first;
        try {
            first = pending.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LatchQException("interrupted while waiting for queue entries", e);
        }
        result.add(deserialize(first));
        drainUpTo(result, count - 1);
        totalReadCount.add(result.size());
        return result;
    }

    @Override
    public LogEntryList<T> read(int count, Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (count <= 0) {
            throw new IllegalArgumentException("count must be positive, got: " + count);
        }
        ensureUsable();
        LogEntryList<T> result = new LogEntryList<>();
        BufferedLogEntry first;
        try {
            first = pending.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LatchQException("interrupted while waiting for queue entries", e);
        }
        if (first == null) {
            return result;
        }
        result.add(deserialize(first));
        drainUpTo(result, count - 1);
        totalReadCount.add(result.size());
        return result;
    }

    private void drainUpTo(LogEntryList<T> result, int maxAdditional) {
        for (int i = 0; i < maxAdditional; i++) {
            BufferedLogEntry entry = pending.poll();
            if (entry == null) {
                return;
            }
            result.add(deserialize(entry));
        }
    }

    private LogEntry<T> deserialize(BufferedLogEntry entry) {
        try {
            T data = objectMapper.readValue(entry.data(), type);
            return new LogEntry<>(data, entry.index(), entry.nextIndex());
        } catch (IOException e) {
            throw new LatchQException(
                    "cannot deserialize payload at index "
                            + entry.index()
                            + " as "
                            + type.getName(),
                    e);
        }
    }

    // ------------------------------------------------------------------
    // progress (implemented in milestone M2)
    // ------------------------------------------------------------------

    @Override
    public void commit(Collection<Position> positions) {
        Objects.requireNonNull(positions, "positions");
        ensureUsable();
        throw new UnsupportedOperationException("commit is implemented in milestone M2");
    }

    @Override
    public void forceCommitGap(long gapStart, long gapEnd) {
        ensureUsable();
        throw new UnsupportedOperationException("forceCommitGap is implemented in milestone M2");
    }

    // ------------------------------------------------------------------
    // export (implemented in milestone M3)
    // ------------------------------------------------------------------

    @Override
    public ExportResult export(
            String targetDirectory, long fromIndex, Long toIndex, int entriesPerFile) {
        ensureUsable();
        throw new UnsupportedOperationException("export is implemented in milestone M3");
    }

    // ------------------------------------------------------------------
    // introspection
    // ------------------------------------------------------------------

    @Override
    public long firstIndex() {
        ensureUsable();
        return safeFirstIndex();
    }

    @Override
    public long lastIndexAppended() {
        ensureUsable();
        if (queue == null) {
            return -1;
        }
        long first = safeFirstIndex();
        if (first < 0) {
            return -1;
        }
        long last = queue.lastIndex();
        return last < 0 ? -1 : last;
    }

    @Override
    public LatchQueueMetrics metrics() {
        long first = queue == null || closed ? -1 : safeFirstIndex();
        long last = queue == null || closed ? -1 : lastIndexAppended();
        return new LatchQueueMetrics(
                totalWriteCount.sum(),
                totalReadCount.sum(),
                totalCommittedRanges.sum(),
                currentGapCount.get(),
                largestGapSize.get(),
                0, // completed-range count arrives with the complete task in M2
                truncateBeforeIndex.get(),
                first,
                last);
    }

    private long safeFirstIndex() {
        if (queue == null) {
            return -1;
        }
        try {
            long first = queue.firstIndex();
            // M0 finding: an empty queue returns Long.MAX_VALUE instead of throwing
            return first == Long.MAX_VALUE ? -1 : first;
        } catch (RuntimeException e) {
            LOG.warn("LatchQueue '{}' failed to read firstIndex, treating as empty", name, e);
            return -1;
        }
    }

    private void ensureUsable() {
        if (closed) {
            throw new LatchQException("queue '" + name + "' is closed");
        }
        if (!initialized) {
            throw new LatchQNotInitializedException(
                    "queue '" + name + "' is not initialized; call initialize() first");
        }
    }
}
