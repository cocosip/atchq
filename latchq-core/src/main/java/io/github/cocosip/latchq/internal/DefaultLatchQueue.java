package io.github.cocosip.latchq.internal;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.cocosip.latchq.ExportResult;
import io.github.cocosip.latchq.LatchQueue;
import io.github.cocosip.latchq.LatchQueueMetrics;
import io.github.cocosip.latchq.LogEntry;
import io.github.cocosip.latchq.LogEntryList;
import io.github.cocosip.latchq.Position;
import io.github.cocosip.latchq.config.LatchQueueConfiguration;
import io.github.cocosip.latchq.config.LatchQueueOptions;
import io.github.cocosip.latchq.exception.LatchQDeserializationException;
import io.github.cocosip.latchq.exception.LatchQException;
import io.github.cocosip.latchq.exception.LatchQNotInitializedException;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.StoreFileListener;
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

    /** Folded absolute path this instance registered in {@link #OPEN_QUEUE_DIRECTORIES}. */
    private String openDirectoryKey;

    private final Object lifecycleLock = new Object();
    private volatile boolean initialized;
    private volatile boolean closed;

    private ChronicleQueue queue;
    private Thread scanThread;
    private Thread completeThread;
    private Thread checkpointThread;
    private Thread cleanupThread;
    private Thread syncThread;
    private volatile boolean running;

    /**
     * Close marker posted into {@link #pending} on {@link #close()}: wakes consumers blocked in a
     * blocking read so they fail with a clear error instead of hanging until interrupted. A reader
     * that takes the marker re-posts it, so one marker wakes every waiting consumer.
     */
    private static final BufferedLogEntry CLOSE_MARKER = new BufferedLogEntry(new byte[0], -1, -1);

    /**
     * Queue directories currently held by live instances in this JVM, keyed by absolute path folded
     * to lower case. Two live instances on one directory would interleave checkpoints and cleanup
     * on the same storage; the fold also catches distinct configurations that collapse onto one
     * directory on case-insensitive filesystems (Windows/macOS), e.g. fileNames differing only in
     * case (the configured fileName is normalized to lower case for the directory name).
     */
    private static final java.util.concurrent.ConcurrentHashMap<String, Boolean>
            OPEN_QUEUE_DIRECTORIES = new java.util.concurrent.ConcurrentHashMap<>();

    /** Appenders created by this session, synced periodically per {@code syncIntervalMillis}. */
    private final Set<ExcerptAppender> appenders = ConcurrentHashMap.newKeySet();

    /**
     * Guards cycle cleanup against in-flight exports. An export may legitimately read cycles behind
     * the persisted checkpoint (disaster recovery), and deleting those files mid-scan would
     * truncate the export silently. An export holds the read lock for the whole scan; cleanup only
     * deletes under a try-locked write lock, so a long export merely skips cleanup ticks. A lock
     * (not a counter check) is required: checking a counter leaves a window in which cleanup has
     * already passed the check while the export has not yet registered itself.
     */
    private final java.util.concurrent.locks.ReentrantReadWriteLock cleanupSuspensionLock =
            new java.util.concurrent.locks.ReentrantReadWriteLock();

    /** Bumped whenever the empty-gap corrections change, so idle checkpoints can be skipped. */
    private final AtomicLong correctionsVersion = new AtomicLong();

    private final AtomicLong persistedCorrectionsVersion = new AtomicLong();

    /**
     * Files opened by this session, keyed by cycle (populated through the store file listener).
     * Cleanup deletes entries whose cycle is entirely behind the persisted checkpoint.
     */
    private final java.util.concurrent.ConcurrentHashMap<Integer, java.io.File> acquiredCycleFiles =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Truncate index as of the last successful checkpoint write; cleanup must only rely on this
     * persisted value, never on the in-memory truncate index.
     */
    private final AtomicLong persistedTruncate = new AtomicLong(-1);

    private final ArrayBlockingQueue<BufferedLogEntry> pending;
    private final ReadAheadStamper stamper = new ReadAheadStamper();
    private final ThreadLocal<ExcerptAppender> appenderThreadLocal = new ThreadLocal<>();

    /**
     * Provisional nextIndex handed out with the tail entry while the queue was caught up, or -1.
     * When the real next message arrives at a different index (a roll happened while idle), an
     * empty-gap correction is recorded so the merge skips the empty range immediately instead of
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

    /** Committed ranges waiting to be merged; guarded by {@link #rangesLock}. */
    private final java.util.TreeSet<CompletedRange> completedRanges =
            new java.util.TreeSet<>(CompletedRange.naturalOrder());

    /** Guards {@link #completedRanges} and the gap-tracking state. */
    private final java.util.concurrent.locks.ReentrantLock rangesLock =
            new java.util.concurrent.locks.ReentrantLock();

    // Gap tracking; all fields guarded by rangesLock.
    private long lastGapStart = -1;
    private long lastGapEnd = -1;
    private long lastGapDetectedMillis = 0;
    private long lastGapWarnedMillis = 0;

    /** Seeded restore index of the current scan session; drives the seed correction. */
    private long seedIndex = -1;

    private boolean seedPending;

    private CheckpointStore checkpointStore;

    // Metrics.
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
     * must not reject large (but admitted) payloads on read. Unknown JSON properties are ignored so
     * payloads can evolve (a message written by a newer schema stays readable for an older payload
     * class); unknown-property drift that changes types still surfaces as a {@link
     * LatchQDeserializationException}.
     */
    private static ObjectMapper newObjectMapper() {
        JsonFactory factory =
                JsonFactory.builder()
                        .streamReadConstraints(
                                StreamReadConstraints.builder()
                                        .maxStringLength(Integer.MAX_VALUE)
                                        .build())
                        .build();
        return new ObjectMapper(factory).disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
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
            // refuse a second live instance on the same storage directory (checkpoints and
            // cleanup would interleave); see OPEN_QUEUE_DIRECTORIES for why the key is folded
            String directoryKey =
                    queueDirectory.toAbsolutePath().normalize().toString().toLowerCase(Locale.ROOT);
            if (OPEN_QUEUE_DIRECTORIES.putIfAbsent(directoryKey, Boolean.TRUE) != null) {
                throw new LatchQException(
                        "queue directory "
                                + queueDirectory
                                + " is already opened by another LatchQueue instance in this JVM;"
                                + " share the instance via LatchQueueFactory or LatchQueueHolder"
                                + " instead");
            }
            this.openDirectoryKey = directoryKey;
            try {
                RollCycle rollCycle = RollCycleResolver.resolve(configuration.getRollCycle());
                // M1 finding: Chronicle caps a single write at blockSize/2 - 4 bytes, so the block
                // size is derived from maxMessageSizeBytes to make the configured bound
                // authoritative
                long blockSize =
                        Math.max(
                                DEFAULT_BLOCK_SIZE_BYTES,
                                configuration.getMaxMessageSizeBytes() * 2 + 128);
                var builder =
                        ChronicleQueue.singleBuilder(queueDirectory.toString())
                                .rollCycle(rollCycle)
                                .blockSize(blockSize)
                                .storeFileListener(
                                        new StoreFileListener() {
                                            @Override
                                            public void onAcquired(int cycle, java.io.File file) {
                                                acquiredCycleFiles.put(cycle, file);
                                            }

                                            @Override
                                            public void onReleased(int cycle, java.io.File file) {
                                                // kept in the map: released files are still
                                                // deletable
                                                // by
                                                // the cleanup rule below the persisted checkpoint
                                            }
                                        });
                if (configuration.getBuilderCustomizer() != null) {
                    configuration.getBuilderCustomizer().accept(builder);
                }
                this.queue = builder.build();

                // Restore the consumer position from the persisted checkpoint (clamped by
                // firstIndex so a stale checkpoint cannot point into cleaned-up territory).
                this.checkpointStore = new CheckpointStore(queueDirectory);
                CheckpointStore.Checkpoint checkpoint = checkpointStore.read();
                long restore;
                if (checkpoint != null) {
                    restore = checkpoint.truncate();
                    emptyGapCorrections.putAll(checkpoint.corrections());
                    correctionsVersion.incrementAndGet();
                    long first = safeFirstIndex();
                    if (first >= 0 && restore < first) {
                        LOG.warn(
                                "LatchQueue '{}' checkpoint index {} is below the earliest existing message {}; resuming from {}",
                                name,
                                restore,
                                first,
                                first);
                        restore = first;
                    }
                } else {
                    restore = safeFirstIndex();
                }
                truncateBeforeIndex.set(restore);
                persistedTruncate.set(checkpoint != null ? checkpoint.truncate() : -1);
                // seed correction: the first real read after a restart that lands past the seed
                // index proves the range [seed, index) holds no messages, bridging a provisional
                // checkpoint across an idle roll
                this.seedIndex = restore;
                this.seedPending = restore >= 0;

                final long restoreIndex = restore;
                this.running = true;
                // The tailer is created, used and closed entirely on the scan thread: Chronicle
                // resources are single-threaded (ThreadingIllegalStateException otherwise).
                this.scanThread =
                        Thread.ofVirtual()
                                .name("latchq-scan-" + name)
                                .start(() -> scanLoop(restoreIndex));
                this.completeThread =
                        Thread.ofVirtual()
                                .name("latchq-complete-" + name)
                                .start(this::completeLoop);
                this.checkpointThread =
                        Thread.ofVirtual()
                                .name("latchq-checkpoint-" + name)
                                .start(this::checkpointLoop);
                this.cleanupThread =
                        Thread.ofVirtual().name("latchq-cleanup-" + name).start(this::cleanupLoop);
                if (configuration.getSyncIntervalMillis() > 0) {
                    this.syncThread =
                            Thread.ofVirtual().name("latchq-sync-" + name).start(this::syncLoop);
                }
                this.initialized = true;
                LOG.info(
                        "LatchQueue '{}' for type {} initialized, directory={}, restoreIndex={}, rollCycle={}",
                        name,
                        type.getName(),
                        queueDirectory,
                        restore,
                        rollCycle);
            } catch (RuntimeException e) {
                // release the directory guard so a retried initialize() is not locked out, and
                // stop a queue that was already built so it cannot leak file handles and
                // background threads
                this.running = false;
                if (queue != null) {
                    try {
                        queue.close();
                    } catch (RuntimeException closeError) {
                        LOG.warn(
                                "LatchQueue '{}' failed to close the queue of a failed"
                                        + " initialize()",
                                name,
                                closeError);
                    }
                    this.queue = null;
                }
                OPEN_QUEUE_DIRECTORIES.remove(directoryKey);
                this.openDirectoryKey = null;
                throw e;
            }
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
        for (Thread thread :
                new Thread[] {
                    scanThread, completeThread, checkpointThread, cleanupThread, syncThread
                }) {
            if (thread != null) {
                thread.interrupt();
                try {
                    thread.join(TimeUnit.SECONDS.toMillis(5));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        // wake consumers blocked in a read: they see the marker, re-post it for the next waiter
        // and fail with a clear closed error. The offer can transiently fail while the hand-off
        // queue is still full; nothing refills it once the scan thread has stopped, so a bounded
        // retry outlasts the drain and also reaches a consumer that passed its usability check
        // before close() but has not reached take() yet. Giving up strands at most a consumer
        // blocked on an empty take(), which only an interrupt can then release - hence the log.
        long wakeDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toMillis(2000);
        while (true) {
            try {
                if (pending.offer(CLOSE_MARKER, 10, TimeUnit.MILLISECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (System.nanoTime() - wakeDeadline >= 0) {
                LOG.warn(
                        "LatchQueue '{}' hand-off queue stayed full during shutdown; a consumer"
                                + " waiting for entries may remain blocked until interrupted",
                        name);
                break;
            }
        }
        // final checkpoint after the scan stopped: everything scanned is now reflected in the
        // truncate index, and the queue is still open for the write
        if (checkpointStore != null) {
            try {
                checkpointStore.write(
                        truncateBeforeIndex.get(), java.util.Map.copyOf(emptyGapCorrections));
            } catch (IOException e) {
                LOG.error("LatchQueue '{}' failed to write the final checkpoint", name, e);
            }
        }
        if (queue != null) {
            // best-effort flush of the tail data before the mappings are released
            for (ExcerptAppender appender : appenders) {
                try {
                    appender.sync();
                } catch (Exception e) {
                    LOG.warn("LatchQueue '{}' final appender sync failed", name, e);
                }
            }
            // The scan thread already closed its tailer; this also closes appenders registered
            // with the queue.
            try {
                queue.close();
            } finally {
                if (openDirectoryKey != null) {
                    OPEN_QUEUE_DIRECTORIES.remove(openDirectoryKey);
                    openDirectoryKey = null;
                }
            }
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
            appenders.add(appender);
        }
        return appender;
    }

    /**
     * Background sync task: periodically flushes every live appender's tail data to the OS/file
     * ({@code appender.sync()} is an msync on the shared mapping with no thread affinity, so
     * calling it from this thread while the owning writer keeps appending is safe; a racy position
     * read only means a slightly smaller flushed prefix, caught up on the next tick). 0 disables.
     */
    private void syncLoop() {
        long interval = configuration.getSyncIntervalMillis();
        while (running && !closed) {
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            for (ExcerptAppender appender : appenders) {
                try {
                    appender.sync();
                } catch (Exception e) {
                    if (closed) {
                        return; // shutdown raced the sweep, the final sync in close() covers it
                    }
                    LOG.warn(
                            "LatchQueue '{}' appender sync failed, retrying next interval",
                            name,
                            e);
                }
            }
        }
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
            recordSeedCorrection(index);
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
            correctionsVersion.incrementAndGet();
            LOG.info(
                    "LatchQueue '{}' recorded empty-gap correction [{}, {}) after an idle roll",
                    name,
                    provisional,
                    realIndex);
        }
    }

    /**
     * Called on the first real read of a scan session. When the seeded restore index points at a
     * provisional (non-existent) position after an idle roll, the first real message proves the
     * range between the seed and itself is empty; recording that as a correction lets the merge
     * advance immediately instead of waiting for the force-skip timeout.
     */
    private void recordSeedCorrection(long firstReadIndex) {
        if (!seedPending) {
            return;
        }
        seedPending = false;
        if (seedIndex >= 0 && firstReadIndex != seedIndex) {
            emptyGapCorrections.put(seedIndex, firstReadIndex);
            correctionsVersion.incrementAndGet();
            LOG.info(
                    "LatchQueue '{}' recorded seed correction [{}, {}) on restore",
                    name,
                    seedIndex,
                    firstReadIndex);
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
        BufferedLogEntry first = takeOrThrow();
        try {
            result.add(deserialize(first, result));
            drainUpTo(result, count - 1);
        } catch (LatchQDeserializationException e) {
            // the prefix travels inside the exception; it was read from the queue either way
            totalReadCount.add(result.size());
            throw e;
        }
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
        first = checkCloseMarker(first);
        try {
            result.add(deserialize(first, result));
            drainUpTo(result, count - 1);
        } catch (LatchQDeserializationException e) {
            // the prefix travels inside the exception; it was read from the queue either way
            totalReadCount.add(result.size());
            throw e;
        }
        totalReadCount.add(result.size());
        return result;
    }

    /** Takes the next entry, failing with a clear error when the close marker arrives. */
    private BufferedLogEntry takeOrThrow() {
        BufferedLogEntry entry;
        try {
            entry = pending.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LatchQException("interrupted while waiting for queue entries", e);
        }
        return checkCloseMarker(entry);
    }

    /**
     * Re-posts the close marker for the next waiting consumer and fails; re-posting succeeds
     * without blocking because taking the marker just freed a slot.
     */
    private BufferedLogEntry checkCloseMarker(BufferedLogEntry entry) {
        if (entry == CLOSE_MARKER) {
            pending.offer(CLOSE_MARKER);
            throw new LatchQException("queue '" + name + "' was closed while waiting for entries");
        }
        return entry;
    }

    private void drainUpTo(LogEntryList<T> result, int maxAdditional) {
        for (int i = 0; i < maxAdditional; i++) {
            BufferedLogEntry entry = pending.poll();
            if (entry == null) {
                return;
            }
            if (entry == CLOSE_MARKER) {
                // leave the marker for other consumers; this batch simply ends here
                pending.offer(CLOSE_MARKER);
                return;
            }
            result.add(deserialize(entry, result));
        }
    }

    private LogEntry<T> deserialize(BufferedLogEntry entry, List<LogEntry<T>> successfullyRead) {
        try {
            T data = objectMapper.readValue(entry.data(), type);
            return new LogEntry<>(data, entry.index(), entry.nextIndex());
        } catch (IOException e) {
            // loud, even if the consumer swallows the exception: this entry needs a decision
            LOG.error(
                    "LatchQueue '{}' cannot deserialize message at index {} ({} bytes); skip it"
                            + " via forceCommitGap({}, {}) or fix the payload class",
                    name,
                    entry.index(),
                    entry.data().length,
                    entry.index(),
                    entry.nextIndex(),
                    e);
            throw new LatchQDeserializationException(
                    "cannot deserialize payload at index "
                            + entry.index()
                            + " as "
                            + type.getName(),
                    entry.index(),
                    entry.nextIndex(),
                    e,
                    List.copyOf(successfullyRead));
        }
    }

    // ------------------------------------------------------------------
    // commit and progress merging
    // ------------------------------------------------------------------

    @Override
    public void commit(Collection<Position> positions) {
        Objects.requireNonNull(positions, "positions");
        ensureUsable();
        int invalid = 0;
        int stale = 0;
        int overlap = 0;
        int duplicate = 0;
        int added = 0;
        rangesLock.lock();
        try {
            long truncate = truncateBeforeIndex.get();
            for (Position position : positions) {
                // validation rules mirror the FASTER reference implementation
                if (position == null || !position.isValid()) {
                    invalid++;
                    continue;
                }
                if (position.nextIndex() <= truncate) {
                    stale++; // already merged or abandoned by a force-skip
                    continue;
                }
                if (position.index() < truncate) {
                    overlap++; // crosses the truncate boundary, cannot be applied safely
                    continue;
                }
                if (completedRanges.add(
                        new CompletedRange(position.index(), position.nextIndex()))) {
                    totalCommittedRanges.increment();
                    added++;
                } else {
                    duplicate++;
                }
            }
            if (invalid > 0) {
                LOG.warn(
                        "LatchQueue '{}' skipped {} invalid positions during commit",
                        name,
                        invalid);
            }
            if (stale > 0) {
                LOG.warn(
                        "LatchQueue '{}' ignored {} stale positions during commit (truncate={})",
                        name,
                        stale,
                        truncate);
            }
            if (overlap > 0) {
                LOG.warn(
                        "LatchQueue '{}' ignored {} boundary-crossing positions during commit (truncate={})",
                        name,
                        overlap,
                        truncate);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "LatchQueue '{}' committed {} positions ({} duplicates, {} invalid, {} stale, {} overlapping)",
                        name,
                        added,
                        duplicate,
                        invalid,
                        stale,
                        overlap);
            }
        } finally {
            rangesLock.unlock();
        }
    }

    @Override
    public void forceCommitGap(long gapStart, long gapEnd) {
        ensureUsable();
        if (gapStart < 0) {
            throw new IllegalArgumentException("gapStart must be non-negative, got: " + gapStart);
        }
        if (gapEnd <= gapStart) {
            throw new IllegalArgumentException(
                    "gapEnd must be greater than gapStart, start: "
                            + gapStart
                            + ", end: "
                            + gapEnd);
        }
        rangesLock.lock();
        try {
            if (completedRanges.add(new CompletedRange(gapStart, gapEnd))) {
                LOG.error(
                        "LatchQueue '{}' manually filled gap [{}, {}) ({} messages); DATA IN THIS RANGE IS ABANDONED",
                        name,
                        gapStart,
                        gapEnd,
                        gapEnd - gapStart);
                resetGapTracking();
            } else {
                LOG.warn(
                        "LatchQueue '{}' gap [{}, {}) was already recorded",
                        name,
                        gapStart,
                        gapEnd);
            }
        } finally {
            rangesLock.unlock();
        }
    }

    /** Background complete task: periodically merges committed ranges into the truncate index. */
    private void completeLoop() {
        while (running && !closed) {
            try {
                mergeAndAdvance();
            } catch (Exception e) {
                if (closed) {
                    return; // shutdown interrupt landed mid-work, the final checkpoint covers it
                }
                LOG.error("LatchQueue '{}' range merging failed", name, e);
            }
            try {
                Thread.sleep(configuration.getCompleteIntervalMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * Two-phase merge (mirrors the FASTER reference implementation): compute the continuous range
     * and the removals under the lock, then advance the truncate index and remove the merged ranges
     * - the lock is never held across the state transition.
     */
    private void mergeAndAdvance() {
        long currentEnd = truncateBeforeIndex.get();
        if (currentEnd < 0) {
            return; // nothing scanned yet
        }
        long newEnd = currentEnd;
        boolean gapDetected = false;
        List<CompletedRange> merged = new ArrayList<>();
        rangesLock.lock();
        try {
            // sorted snapshot; an index loop lets a bridged gap re-process the same range
            List<CompletedRange> snapshot = new ArrayList<>(completedRanges);
            for (int i = 0; i < snapshot.size(); i++) {
                CompletedRange range = snapshot.get(i);
                if (range.start() <= newEnd) {
                    // connects to (or overlaps) the continuous range
                    newEnd = Math.max(newEnd, range.end());
                    merged.add(range);
                    continue;
                }
                // gap detected at [newEnd, range.start)
                gapDetected = true;
                Long bridged = emptyGapCorrections.get(newEnd);
                if (bridged != null && bridged == range.start()) {
                    // the correction proves the gap is empty (idle roll): skip it at once
                    emptyGapCorrections.remove(newEnd);
                    correctionsVersion.incrementAndGet();
                    newEnd = range.start();
                    gapDetected = false;
                    i--; // re-process the same range, which now connects
                    continue;
                }
                long now = System.currentTimeMillis();
                long gapSize = range.start() - newEnd;
                largestGapSize.accumulateAndGet(gapSize, Math::max);
                if (lastGapStart != newEnd || lastGapEnd != range.start()) {
                    lastGapStart = newEnd;
                    lastGapEnd = range.start();
                    lastGapDetectedMillis = now;
                    lastGapWarnedMillis = 0;
                }
                long gapAge = now - lastGapDetectedMillis;
                long gapTimeout = configuration.getGapTimeoutMillis();
                if (gapTimeout > 0
                        && gapAge >= gapTimeout
                        && (lastGapWarnedMillis == 0 || now - lastGapWarnedMillis >= gapTimeout)) {
                    LOG.warn(
                            "LatchQueue '{}' gap [{}, {}) ({} messages) has persisted for {} ms; a consumer may be stuck",
                            name,
                            newEnd,
                            range.start(),
                            gapSize,
                            gapAge);
                    lastGapWarnedMillis = now;
                }
                long forceTimeout = configuration.getForceCompleteGapTimeoutMillis();
                boolean timeoutReached = forceTimeout > 0 && gapAge >= forceTimeout;
                boolean rangesOverflow =
                        configuration.getMaxCompletedRanges() > 0
                                && completedRanges.size() > configuration.getMaxCompletedRanges();
                if (timeoutReached || rangesOverflow) {
                    LOG.error(
                            "LatchQueue '{}' FORCING COMPLETION past gap [{}, {}) ({} messages, age {} ms, {} ranges); "
                                    + "DATA IN THE GAP IS ABANDONED",
                            name,
                            newEnd,
                            range.start(),
                            gapSize,
                            gapAge,
                            completedRanges.size());
                    newEnd = Math.max(newEnd, range.end());
                    merged.add(range);
                    resetGapTracking();
                    gapDetected = false;
                    continue; // keep merging behind the skipped gap
                }
                // ordinary gap: stop merging until it is filled; refresh the gap metric so it
                // reflects the persisted stall even when no progress is made
                currentGapCount.set(countGaps(newEnd));
                break;
            }
        } finally {
            rangesLock.unlock();
        }

        long before = truncateBeforeIndex.get();
        if (newEnd > before) {
            final long advancedTo = newEnd;
            truncateBeforeIndex.updateAndGet(current -> Math.max(current, advancedTo));
            rangesLock.lock();
            try {
                completedRanges.removeAll(merged);
                currentGapCount.set(countGaps(advancedTo));
            } finally {
                rangesLock.unlock();
            }
            LOG.debug(
                    "LatchQueue '{}' advanced truncate index to {} ({} ranges merged)",
                    name,
                    newEnd,
                    merged.size());
        } else if (!gapDetected) {
            rangesLock.lock();
            try {
                resetGapTracking();
            } finally {
                rangesLock.unlock();
            }
        }
        // corrections below the truncate point are consumed forever
        if (emptyGapCorrections.keySet().removeIf(key -> key < truncateBeforeIndex.get())) {
            correctionsVersion.incrementAndGet();
        }
    }

    /** Counts discontinuities between the truncate point and the committed ranges. */
    private long countGaps(long truncate) {
        long gaps = 0;
        long currentEnd = truncate;
        for (CompletedRange range : completedRanges) {
            if (range.start() > currentEnd) {
                gaps++;
            }
            currentEnd = Math.max(currentEnd, range.end());
        }
        return gaps;
    }

    private void resetGapTracking() {
        lastGapStart = -1;
        lastGapEnd = -1;
        lastGapDetectedMillis = 0;
        lastGapWarnedMillis = 0;
    }

    // ------------------------------------------------------------------
    // checkpoint persistence
    // ------------------------------------------------------------------

    /** Background checkpoint task: periodically persists the truncate index atomically. */
    private void checkpointLoop() {
        while (running && !closed) {
            long truncate = truncateBeforeIndex.get();
            long version = correctionsVersion.get();
            // an fsync-per-tick on an idle queue is pure wear; write only on real progress
            if (truncate != persistedTruncate.get()
                    || version != persistedCorrectionsVersion.get()) {
                try {
                    checkpointStore.write(truncate, java.util.Map.copyOf(emptyGapCorrections));
                    persistedTruncate.set(truncate);
                    persistedCorrectionsVersion.set(version);
                } catch (Exception e) {
                    if (closed) {
                        return; // shutdown interrupt landed mid-write, the final checkpoint covers
                        // it
                    }
                    LOG.error("LatchQueue '{}' checkpoint write failed", name, e);
                }
            }
            try {
                Thread.sleep(configuration.getCheckpointIntervalMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    // ------------------------------------------------------------------
    // cycle file cleanup
    // ------------------------------------------------------------------

    /** Background cleanup task: deletes cycle files fully behind the persisted checkpoint. */
    private void cleanupLoop() {
        while (running && !closed) {
            try {
                cleanupCycleFiles();
            } catch (Exception e) {
                if (closed) {
                    return; // shutdown interrupt, no cleanup work matters any more
                }
                LOG.error("LatchQueue '{}' cycle cleanup failed", name, e);
            }
            try {
                Thread.sleep(configuration.getCleanupIntervalMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * Deletes {@code .cq4} cycle files whose messages are all behind the persisted checkpoint. A
     * whole cycle is deletable once its cycle number is below the checkpoint's cycle; the
     * checkpoint - not the in-memory truncate index - is authoritative so that "delete then crash"
     * can never lose data beyond the persisted progress. Failures (e.g. Windows holds a file handle
     * briefly after close) are tolerated and retried on the next tick.
     */
    private void cleanupCycleFiles() {
        if (!cleanupSuspensionLock.writeLock().tryLock()) {
            return; // an export may be reading behind the checkpoint; never delete under it
        }
        try {
            long persisted = persistedTruncate.get();
            if (persisted < 0) {
                return; // nothing persisted yet: never delete based on memory state alone
            }
            int checkpointCycle = rollCycle().toCycle(persisted);
            for (Map.Entry<Integer, java.io.File> entry : acquiredCycleFiles.entrySet()) {
                int cycle = entry.getKey();
                if (cycle >= checkpointCycle) {
                    continue;
                }
                java.io.File file = entry.getValue();
                try {
                    Files.delete(file.toPath());
                    acquiredCycleFiles.remove(cycle);
                    LOG.info(
                            "LatchQueue '{}' deleted consumed cycle file {} (cycle {})",
                            name,
                            file,
                            cycle);
                } catch (IOException e) {
                    // transient lock, retried on the next tick
                    LOG.debug("LatchQueue '{}' could not delete cycle file {} yet", name, file, e);
                }
            }
            cleanupUnopenedFiles(checkpointCycle);
        } finally {
            cleanupSuspensionLock.writeLock().unlock();
        }
    }

    /**
     * Sweeps cycle files that were never opened in this session (e.g. written by an earlier run and
     * never re-read). Their cycle number is derived from the file name relative to an acquired
     * anchor file: parsing both names with any fixed timezone cancels the unknown timezone offset,
     * so the cycle difference is exact without depending on Chronicle's naming internals (see the
     * M0 findings in docs/spike-notes.md).
     */
    private void cleanupUnopenedFiles(int checkpointCycle) {
        if (acquiredCycleFiles.isEmpty()) {
            return;
        }
        Map.Entry<Integer, java.io.File> anchor =
                java.util.Collections.max(
                        acquiredCycleFiles.entrySet(), Map.Entry.comparingByKey());
        java.text.SimpleDateFormat parser = new java.text.SimpleDateFormat(rollCycle().format());
        parser.setLenient(false);
        long anchorTime;
        try {
            anchorTime = parser.parse(stripCq4Extension(anchor.getValue().getName())).getTime();
        } catch (java.text.ParseException e) {
            LOG.debug(
                    "LatchQueue '{}' cannot parse anchor file name {}, skipping sweep",
                    name,
                    anchor.getValue());
            return;
        }
        java.io.File[] candidates =
                queueDirectory.toFile().listFiles((dir, fileName) -> fileName.endsWith(".cq4"));
        if (candidates == null) {
            return;
        }
        for (java.io.File file : candidates) {
            if (acquiredCycleFiles.containsValue(file)) {
                continue;
            }
            long fileTime;
            try {
                fileTime = parser.parse(stripCq4Extension(file.getName())).getTime();
            } catch (java.text.ParseException e) {
                LOG.debug("LatchQueue '{}' ignoring unparseable file name {}", name, file);
                continue;
            }
            long cycle =
                    anchor.getKey()
                            + Math.round(
                                    (double) (fileTime - anchorTime)
                                            / rollCycle().lengthInMillis());
            // one-cycle safety margin: the name-based derivation can be off by one cycle when
            // a DST transition lies between the anchor and the target file, and deleting the
            // checkpoint cycle itself would abandon unprocessed messages
            if (cycle >= 0 && cycle < checkpointCycle - 1) {
                try {
                    Files.delete(file.toPath());
                    LOG.info(
                            "LatchQueue '{}' deleted consumed cycle file {} (cycle {})",
                            name,
                            file,
                            cycle);
                } catch (IOException e) {
                    LOG.debug("LatchQueue '{}' could not delete cycle file {} yet", name, file, e);
                }
            }
        }
    }

    private static String stripCq4Extension(String fileName) {
        return fileName.substring(0, fileName.length() - ".cq4".length());
    }

    // ------------------------------------------------------------------
    // export
    // ------------------------------------------------------------------

    @Override
    public ExportResult export(
            String targetDirectory, long fromIndex, Long toIndex, int entriesPerFile) {
        ensureUsable();
        Objects.requireNonNull(targetDirectory, "targetDirectory");
        if (entriesPerFile <= 0) {
            throw new IllegalArgumentException(
                    "entriesPerFile must be positive, got: " + entriesPerFile);
        }
        // clamp to the earliest surviving message (FASTER reference clamps to BeginAddress)
        long first = safeFirstIndex();
        long actualFrom = fromIndex;
        if (first >= 0 && fromIndex < first) {
            LOG.warn(
                    "LatchQueue '{}' export fromIndex {} is below the earliest surviving message {}; "
                            + "data in [{}, {}) was cleaned up and cannot be exported",
                    name,
                    fromIndex,
                    first,
                    fromIndex,
                    first);
            actualFrom = first;
        }
        long toExclusive = toIndex == null ? Long.MAX_VALUE : toIndex;
        List<String> filePaths = new ArrayList<>();
        if (actualFrom >= toExclusive) {
            return new ExportResult(filePaths, 0, actualFrom, actualFrom);
        }
        // an export may be reading cycles behind the persisted checkpoint (disaster recovery);
        // hold the read lock for the whole scan so cleanup cannot delete those files mid-scan
        cleanupSuspensionLock.readLock().lock();
        try {
            try {
                Files.createDirectories(Path.of(targetDirectory));
            } catch (IOException e) {
                throw new LatchQException("cannot create export directory " + targetDirectory, e);
            }
            String filePrefix =
                    TypeStorageNaming.sanitize(type.getName()) + "_" + actualFrom + "_part";
            String fileSuffix =
                    "_"
                            + DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS")
                                    .format(LocalDateTime.now())
                            + "_"
                            + Long.toHexString(System.nanoTime() & 0xFFFFF)
                            + ".jsonl";

            long count = 0;
            long lastReadIndex = -1;
            int fileIndex = 0;
            int entriesInFile = 0;
            java.io.OutputStream out = null;
            try (ExcerptTailer tailer =
                    queue.createTailer()) { // isolated from the consumer position
                if (actualFrom >= 0) {
                    tailer.moveToIndex(actualFrom);
                }
                while (true) {
                    try (DocumentContext ctx = tailer.readingDocument()) {
                        if (!ctx.isPresent()) {
                            break;
                        }
                        long index = ctx.index();
                        if (index >= toExclusive) {
                            break;
                        }
                        byte[][] holder = new byte[1][];
                        ctx.wire().readBytes(in -> holder[0] = in.toByteArray());
                        if (holder[0].length == 0) {
                            continue; // phantom document at a cleaned-up position, skip forward
                        }
                        if (out == null) {
                            Path file =
                                    Path.of(targetDirectory, filePrefix + fileIndex++ + fileSuffix);
                            // buffered: without it every entry costs two unbuffered write syscalls
                            out = new BufferedOutputStream(Files.newOutputStream(file), 1 << 16);
                            filePaths.add(file.toString());
                            entriesInFile = 0;
                        }
                        out.write(holder[0]);
                        out.write('\n');
                        count++;
                        entriesInFile++;
                        lastReadIndex = index;
                        if (entriesInFile >= entriesPerFile) {
                            out.close();
                            out = null;
                        }
                    }
                }
            } catch (IOException e) {
                throw new LatchQException("export failed after " + count + " entries", e);
            } finally {
                if (out != null) {
                    try {
                        out.close();
                    } catch (IOException e) {
                        LOG.warn("LatchQueue '{}' failed to close the last export file", name, e);
                    }
                }
            }
            LOG.info(
                    "LatchQueue '{}' exported {} entries from index {} into {} file(s)",
                    name,
                    count,
                    actualFrom,
                    filePaths.size());
            // exclusive upper bound for the next incremental export; an empty export keeps
            // toIndex == fromIndex (mirroring the FASTER reference result)
            long resultTo = count == 0 ? actualFrom : lastReadIndex + 1;
            return new ExportResult(filePaths, count, actualFrom, resultTo);
        } finally {
            cleanupSuspensionLock.readLock().unlock();
        }
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
        return lastIndexInternal();
    }

    /**
     * Reads the last index without a lifecycle check: {@link #metrics()} promises to be callable in
     * any lifecycle state, so it must not race a {@code close()} into a usability exception.
     */
    private long lastIndexInternal() {
        long first = safeFirstIndex();
        if (first < 0) {
            return -1;
        }
        long last;
        try {
            last = queue.lastIndex();
        } catch (RuntimeException e) {
            // close() racing the read releases the underlying queue; report unknown, mirroring
            // the tolerant firstIndex handling
            LOG.warn("LatchQueue '{}' failed to read lastIndex, treating as empty", name, e);
            return -1;
        }
        return last < 0 ? -1 : last;
    }

    @Override
    public LatchQueueMetrics metrics() {
        // deliberately callable in any lifecycle state (health endpoints call it): storage fields
        // read as -1 while uninitialized or closed. The initialized flag - not queue != null - is
        // the right guard, because initialize() assigns the queue field before flipping the flag.
        boolean usable = initialized && !closed;
        long first = usable ? safeFirstIndex() : -1;
        long last = usable ? lastIndexInternal() : -1;
        rangesLock.lock();
        int rangeCount;
        try {
            rangeCount = completedRanges.size();
        } finally {
            rangesLock.unlock();
        }
        return new LatchQueueMetrics(
                totalWriteCount.sum(),
                totalReadCount.sum(),
                totalCommittedRanges.sum(),
                currentGapCount.get(),
                largestGapSize.get(),
                rangeCount,
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
