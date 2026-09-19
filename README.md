# LatchQ

[中文说明](#中文说明) below. Documentation: [docs/](./docs/)(简体中文)。

**LatchQ** is a Java 21 persistent queue library built on [OpenHFT Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue).
It is a Java port of the semantics of [SharpAbp.Abp.Faster](https://github.com/sharpabp/sharp-abp)
(the .NET/FASTER reference implementation), with the storage engine swapped to Chronicle Queue.

## Features

- Multi-threaded concurrent **writes** (per-thread appenders) and multi-threaded concurrent
  **batch reads** (single scan thread fanning out through a bounded queue with backpressure).
- **Out-of-order commits** with interval merging: consumers process batches independently and
  commit positions in any order; the library merges them into the safe consumption point.
- **Gap detection and recovery**: persistent gaps are warned about and force-skipped after a
  configurable timeout (data in a gap is abandoned - business processing must be idempotent).
- **Durable consumption checkpoints** (atomic temp+fsync+move writes) so restarts resume exactly
  at the safe point - committed messages are never re-delivered.
- **Automatic cleanup** of consumed cycle files, strictly gated by the persisted checkpoint.
- **Index-range export** to JSONL files for business-owned download/inspection APIs.
- Multiple queues of different payload types coexisting in one JVM
  (`factory.getOrCreate(name, Type)`), or a single queue via `LatchQueueBuilder`.

## Quick start (plain Java)

```java
LatchQueue<Order> queue = LatchQueueBuilder.create("orders", Order.class)
        .rootPath("/data/latchq")
        .configuration(c -> c.setFileName("orders"))
        .build();

long index = queue.write(new Order(1, "item"));

// consumer threads: read -> process (idempotently) -> commit
var batch = queue.read(100, Duration.ofMillis(500));
queue.commit(batch.stream().map(LogEntry::position).toList());

queue.close(); // graceful shutdown, final checkpoint
```

## Quick start (Spring Boot)

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
```

Inject `LatchQueueFactory` and call `factory.getOrCreate("audit", AuditEvent.class)`.
See [samples/README.md](./samples/README.md) for runnable examples.

## Runtime requirement (JDK 21)

Chronicle Queue on JDK 21 needs these JVM flags for the application **and** the test JVM:

```
--add-opens java.base/java.lang=ALL-UNNAMED
--add-opens java.base/java.lang.reflect=ALL-UNNAMED
--add-opens java.base/java.io=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
--add-exports java.base/jdk.internal.ref=ALL-UNNAMED
```

## Key configuration

| Property | Default | Meaning |
|---|---|---|
| `maxMessageSizeBytes` | 20 MiB | per-message serialized size cap; larger writes are rejected before touching storage |
| `rollCycle` | `DEFAULT` | Chronicle roll cycle name; carries the capacity semantics |
| `syncIntervalMillis` | 2000 | periodic `appender.sync()` flush; the crash-durability window |
| `preReadCapacity` | 5000 | bounded queue between scan thread and consumers |
| `forceCompleteGapTimeoutMillis` | 120000 | gap force-skip timeout (0 = off; range overflow still forces) |
| `checkpointIntervalMillis` | 2000 | atomic checkpoint persistence period |

Full model: [docs/latchq-design.md](./docs/latchq-design.md)(简体中文)。

## Building

```bash
./mvnw test     # tests (Surefire already carries the JDK 21 opens)
./mvnw verify   # tests + jacoco + spotless
```

## Modules

| Module | Purpose |
|---|---|
| `latchq-core` | the queue implementation (no framework bindings) |
| `latchq-spring-boot-starter` | `@ConfigurationProperties` binding + factory bean + graceful shutdown |
| `samples/*` | runnable console and Spring Boot examples |
| `latchq-benchmarks` | JMH benchmarks (run manually) |

## 中文说明

LatchQ 是基于 Chronicle Queue 的 Java 21 持久化队列库,语义移植自 SharpAbp.Abp.Faster:
多线程并发写入/并发批量读取、乱序提交与区间合并、gap 检测/告警/强制跳过、消费进度原子
checkpoint 持久化、旧分段文件自动清理、按 index 范围导出 JSONL。业务处理需保证幂等
(强制跳过 gap 时该区间数据将被放弃)。设计与验证结论见 `docs/` 目录。
