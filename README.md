# LatchQ

**LatchQ** is a Java 21 persistent queue library built on [OpenHFT Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue).
It is a Java port of the semantics of [SharpAbp.Abp.Faster](https://github.com/sharpabp/sharp-abp)
(the .NET/FASTER reference implementation), with the storage engine swapped to Chronicle Queue.

Documentation (Simplified Chinese): [docs/](./docs/) - design, verification notes and benchmark results.

## Features

- Multi-threaded concurrent **writes** (per-thread appenders) and multi-threaded concurrent
  **batch reads** (a single scan thread fanning out through a bounded queue with backpressure).
- **Out-of-order commits** with interval merging: consumers process batches independently and
  commit positions in any order; the library merges them into the safe consumption point.
- **Gap detection and recovery**: persistent gaps are warned about and force-skipped after a
  configurable timeout (data in a gap is abandoned - business processing must be idempotent).
- **Durable consumption checkpoints** (atomic temp + fsync + move writes) so restarts resume
  exactly at the safe point - committed messages are never re-delivered.
- **Automatic cleanup** of consumed cycle files, strictly gated by the persisted checkpoint.
- **Index-range export** to JSONL files for business-owned download/inspection APIs.
- Multiple queues of different payload types coexisting in one JVM
  (`factory.getOrCreate(name, Type)`), or a single queue via `LatchQueueBuilder`.
- **Static holder** (`LatchQueueHolder`) sharing one factory process-wide for plain-Java apps
  without a DI container.

## Quick start (plain Java)

```java
LatchQueue<Order> queue = LatchQueueBuilder.create("orders", Order.class)
        .rootPath("/data/latchq")
        .configuration(c -> c.setFileName("orders"))
        .build();

// producer threads
long index = queue.write(new Order(1, "item"));

// consumer threads: read -> process (idempotently!) -> commit
LogEntryList<Order> batch = queue.read(100, Duration.ofMillis(500));
if (!batch.isEmpty()) {
    processAll(batch); // must be idempotent, see "Gaps" below
    queue.commit(batch.getPositions());
}

queue.close(); // graceful shutdown, final checkpoint
```

`read(count)` blocks until at least one entry is available; `read(count, timeout)` returns an
empty list when nothing arrives in time. Both drain up to `count` entries without extra waiting.

### Static holder (plain Java, usage across multiple classes)

When usage is spread over several classes and no DI container shares the factory, initialize the
static holder once at startup and reach the same factory from any class - creating a factory (or
builder queue) per class would put multiple queue instances on the same files:

```java
// startup, once
LatchQueueHolder.init(options -> options
        .setRootPath("/data/latchq")
        .configure("orders", c -> c.setFileName("orders")));

// anywhere, in any class
LatchQueue<Order> queue = LatchQueueHolder.getOrCreate("orders", Order.class);

// shutdown, once: closes every queue and performs the final checkpoint
LatchQueueHolder.reset();
```

`init` fails fast when called twice, `getOrCreate` before `init` throws
`LatchQNotInitializedException`, and `reset` is idempotent and allows re-initializing. The holder
does not register a JVM shutdown hook - the application owns the lifecycle. Spring Boot
applications should rely on the injected `LatchQueueFactory` bean instead; the holder keeps its
own independent factory instance.

### Spring Boot

```xml
<dependency>
    <groupId>io.github.cocosip</groupId>
    <artifactId>latchq-spring-boot-starter</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

```yaml
latchq:
  root-path: /data/latchq
  configurations:
    audit:
      file-name: audit
      sync-interval-millis: 1000
```

Inject `LatchQueueFactory` and call `factory.getOrCreate("audit", AuditEvent.class)` - the
queue auto-initializes and is cached per (name, type). On application shutdown the starter
closes every queue, which performs the final checkpoint. See
[samples/README.md](./samples/README.md) for runnable examples.

## Runtime requirement (JDK 21)

Chronicle Queue on JDK 21 needs these JVM flags for the application **and** any forked/test JVM
(the Maven Surefire configuration in this repository already carries them):

```
--add-opens java.base/java.lang=ALL-UNNAMED
--add-opens java.base/java.lang.reflect=ALL-UNNAMED
--add-opens java.base/java.io=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
--add-exports java.base/jdk.internal.ref=ALL-UNNAMED
```

## Configuration

Global options (`latchq.*` in Spring Boot):

| Property | Required | Meaning |
|---|---|---|
| `rootPath` | yes | root directory below which every queue stores its files |
| `configurations.<name>.*` | - | per-queue settings below; unknown queue names fail fast at `getOrCreate` |

Per-queue configuration (`LatchQueueConfiguration`), defaults mirror the FASTER reference where
the semantics survive the Chronicle migration:

| Property | Default | Meaning |
|---|---|---|
| `fileName` | - (required) | queue directory name below `rootPath/<type>/` |
| `maxMessageSizeBytes` | 20 MiB (20971520) | per-message serialized size cap; larger writes are rejected with a clear error before touching storage. Raise it to admit bigger messages (each appender allocates a derived buffer, so very large values multiply with writer threads and may need `-XX:MaxDirectMemorySize`) |
| `rollCycle` | `DEFAULT` | Chronicle roll cycle name (resolved from the `RollCycles` constants, e.g. `FAST_DAILY`, `TWO_HOURLY`); carries the capacity semantics |
| `syncIntervalMillis` | 2000 | periodic `appender.sync()` flush; controls the crash-durability window (0 disables periodic syncing) |
| `completeIntervalMillis` | 3000 | interval of the background range-merging task |
| `checkpointIntervalMillis` | 2000 | interval of the atomic checkpoint persistence task |
| `cleanupIntervalMillis` | 300000 | interval of the consumed-cycle-file cleanup task |
| `preReadCapacity` | 5000 | bounded queue capacity between the scan thread and consumers |
| `gapTimeoutMillis` | 600000 | delay before the first gap warning and between repeated warnings |
| `forceCompleteGapTimeoutMillis` | 120000 | gap force-skip timeout; 0 disables timeout-driven skipping (range overflow still forces) |
| `maxCompletedRanges` | 10000 | completed-range set bound; exceeding it force-skips the oldest gap |
| `builderCustomizer` | - | code-only hook applied to the Chronicle queue builder before it is built |

## Gaps and idempotency

With concurrent consumers, commits arrive out of order and the merge can stall behind a range
that was never committed (a consumer crashed or stalled). After
`forceCompleteGapTimeoutMillis` (or when the range set overflows) the library skips the gap and
logs an ERROR - **the messages inside the gap are abandoned**. Business processing must
therefore be idempotent. Empty gaps (idle roll boundaries) are bridged automatically via
recorded corrections and never need a force-skip.

## Poison messages and payload evolution

An entry that no longer deserializes into the configured payload type (typically a schema that
drifted from stored JSON) makes `read` throw a `LatchQDeserializationException` carrying the
entry's index range. The failed entry is gone for the running session; skip it explicitly to
keep consuming durably:

```java
try {
    batch = queue.read(100);
} catch (LatchQDeserializationException e) {
    LOG.error("poison message at {}", e.getIndex(), e);
    queue.forceCommitGap(e.getIndex(), e.getNextIndex()); // abandon it, logged as ERROR
}
```

Without that call the entry is re-delivered after a restart (it was never committed), so a
crash loop is possible until the payload class is fixed or the message is skipped. Known
unknown JSON properties are ignored on read, so adding fields to a payload stays compatible in
both directions; only type drift on existing fields poisons. Messages written by `batchWrite`
are not atomic as a batch - if a write fails mid-batch, the earlier payloads are already
durable but their indexes are lost.

## Shutdown behaviour

`close()` (the starter calls it via the factory bean's destroy method) stops the background
tasks, wakes consumers blocked in a read with a `LatchQException`, writes the final checkpoint
and releases storage. Prefer the timed `read(count, timeout)` variant in consumer loops so
shutdown stays responsive; consumers using the blocking `read(count)` are woken, but any
thread still busy processing must stop on its own (e.g. a `@PreDestroy` flag like in the
Spring sample).

## Benchmarks

Measured on Windows 10 x64, JDK Temurin 21.0.12.1, Chronicle Queue 5.27ea5, ~120-byte JSON
messages, single thread (JMH, 2x1s warmup + 3x2s measurement); full methodology and the
reproduction commands in [docs/benchmark-notes.md](./docs/benchmark-notes.md):

| Benchmark | Throughput | Notes |
|---|---|---|
| single write | ~1.2M msgs/s | `write()` returning the assigned index |
| batch write | ~1.0M msgs/s | `batchWrite()` with 100 messages per call |
| read + commit pipeline | ~25k msgs/s | single consumer draining 100-entry batches; scales with consumer parallelism |

## Building and CI

```bash
./mvnw test     # tests (Surefire already carries the JDK 21 opens)
./mvnw verify   # tests + jacoco + spotless check
```

CI (`.github/workflows/ci.yml`) runs `./mvnw verify` on every push/PR to master and can run the
JMH benchmarks on demand (`workflow_dispatch`).

## Modules

| Module | Purpose |
|---|---|
| `latchq-core` | the queue implementation (no framework bindings, SLF4J 2.x facade only) |
| `latchq-spring-boot-starter` | `@ConfigurationProperties` binding + factory bean + graceful shutdown + optional health indicator |
| `samples/*` | runnable console and Spring Boot examples |
| `latchq-benchmarks` | JMH benchmarks (shaded executable jar, run manually) |
