# LatchQ 设计文档

## 1. 文档状态

| 项 | 值 |
|---|---|
| 状态 | 设计草案,待用户确认 |
| 版本 | v0.1 |
| 更新日期 | 2026-09-18 |
| 参考实现 | [SharpAbp.Abp.Faster](https://github.com/sharpabp/sharp-abp)(基于 Microsoft FASTER 的 .NET 持久化日志库) |
| 底层依赖 | [OpenHFT Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue) |

## 2. 目标与边界

### 2.1 要解决的问题

业务上需要一个**持久化、支持多线程并发生产/消费**的日志型队列,具体要求:

1. 支持多线程并发写入数据。
2. 支持多线程并发批量读取数据。
3. 消费进度不是"读到即确认",而是等一批数据**真正处理完成**(可能成功也可能失败)后才推进进度,避免异常、重启导致数据丢失或消费进度错误前移。
4. 必须落盘持久化,不能用纯内存队列(进程重启/异常退出不能丢数据)。

这与 `SharpAbp.Abp.Faster` 解决的问题完全一致——只是底层从 Microsoft FASTER 换成 Chronicle Queue,语言从 C# 换成 Java 21。

### 2.2 v1 范围内

- 单进程内、多线程并发写入与批量读取(同一 JVM 内)。
- 乱序提交(commit)+ 区间合并(interval merging)算法,推进"安全截断位置"。
- Gap(提交空洞)自动检测、告警、超时强制跳过(数据可能丢失,需业务幂等处理兜底)。
- 消费进度的持久化(独立文件,原子写),保证重启后从安全位置恢复。
- 旧数据分段的自动清理。
- 按 index 范围导出为 JSONL 文件(供业务方自建 API 暴露给前端下载/排查)。
- 一个 JVM 进程内支持多个不同类型(不同泛型 `T`)、不同名称的队列并存,类似 FASTER 版的 `GetOrCreate<T>(name)`。
- `latchq-core`(纯 Java,无框架绑定,代码式配置)+ `latchq-spring-boot-starter`(Spring Boot 自动装配)双模块。

### 2.3 v1 明确不做(Non-goals)

- **跨进程/跨主机访问**:不支持多个 JVM 进程同时读写同一份队列文件。如未来需要,需要新增分布式协调层,是一个独立的后续设计。
- **多 consumer group**:同一个队列在 v1 只有一条消费进度,多个工作线程共享同一个 checkpoint,不支持像 Kafka 那样多个独立消费组各自维护进度。
- **可插拔序列化器**:v1 固定使用 Jackson JSON,不提供可替换的序列化器接口。
- **Chronicle Queue Enterprise 特性**(复制、二级索引等付费能力):只使用开源版 `net.openhft:chronicle-queue`。

## 3. 项目标识与发布坐标

| 项 | 值 |
|---|---|
| groupId | `io.github.cocosip` |
| 根 artifactId | `latchq-parent`(packaging=pom) |
| 核心模块 | `latchq-core` |
| Spring Boot Starter | `latchq-spring-boot-starter` |
| 示例模块 | `samples/latchq-sample-console`、`samples/latchq-sample-spring-boot` |
| 基准测试模块 | `benchmarks` |
| Java 基线 | 21(本机已验证:Temurin 21.0.12.1) |
| Maven 基线 | 3.9+(本机已验证:Apache Maven 3.9.16) |

Maven 工程结构与插件配置(enforcer 强制 Java/Maven 版本范围、spotless 格式化、jacoco 覆盖率、flatten-maven-plugin 处理 `${revision}` CI-friendly 版本号)参考同一开发者的 `stow` 项目(`D:\Code\java\stow\pom.xml`)的成熟配置,保持风格一致。

## 4. 总体架构

```
                         ┌─────────────────────────────┐
   多个业务写线程 ───────▶│  ChronicleQueue (Appender)   │──▶ 落盘(memory-mapped 文件)
                         └─────────────────────────────┘
                                      │
                                      ▼ (后台 scan 虚拟线程,唯一持有 Tailer)
                         ┌─────────────────────────────┐
                         │ 有界阻塞队列 (ArrayBlockingQueue) │  <-- 单写多读
                         └─────────────────────────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                 ▼
              业务消费线程1      业务消费线程2       业务消费线程N
                    │                 │                 │
                    └─────── commit(positions) ─────────┘
                                      │
                                      ▼
                         ┌─────────────────────────────┐
                         │ TreeSet<CompletedRange>       │ (区间合并 + gap 检测)
                         └─────────────────────────────┘
                                      │
                          后台 complete 虚拟线程周期性推进
                                      ▼
                         truncateBeforeIndex (内存原子变量)
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                                   ▼
        后台 checkpoint 虚拟线程                后台 cleanup 虚拟线程
        (原子写入本地 checkpoint 文件)          (清理已安全处理的旧分段文件)
```

### 4.1 与 FasterLogger 的核心差异

| 维度 | FasterLogger(FASTER) | LatchQueue(Chronicle Queue) |
|---|---|---|
| 底层存储 | FASTER Log(自有 append-only 文件格式) | Chronicle Queue(memory-mapped `.cq4` 分段文件) |
| 位置标识 | 字节地址(long,连续字节偏移) | index(long,高位编码 roll cycle、低位编码序号,**不是**连续字节偏移) |
| 区间合并容差 | `AddressMatchTolerance`(字节容差,因为字节地址可能有对齐间隙) | **不需要容差**,直接用 tailer 读取时给出的精确 `(index, nextIndex)` 做相等匹配,取值更精确 |
| 消费进度持久化 | FASTER 内建"命名 iterator + recover"自动持久化 `CompletedUntilAddress` | Chronicle Queue 无此内建能力,LatchQ **自行实现**:独立小文件 + 原子写(temp+rename) |
| 多线程写入 | `Log.EnqueueAsync`,FASTER 内部线程安全 | Chronicle Queue 的 appender 获取方式(每线程独立 appender 实例)保证线程安全,由 LatchQ 封装 |
| 并发模型 | C# `async`/`await` + `Task` | Java 21 同步阻塞 API + `Thread.ofVirtual()` 承载后台任务 |

## 5. Java 包结构(latchq-core)

```
io.github.cocosip.latchq
├── LatchQueue.java                 # 核心接口
├── LatchQueueFactory.java          # 工厂接口:getOrCreate(name, Class<T>)
├── LatchQueueBuilder.java          # 纯代码构建入口(无 DI 场景手动装配)
├── Position.java                   # [address, nextAddress) 位置,语义对应 FASTER 版
├── LogEntry.java                   # 单条读出的记录(反序列化后的 T + 位置信息)
├── LogEntryList.java               # 批量读取结果,提供 getPositions() 便捷方法
├── LatchQueueMetrics.java          # 只读指标快照(写入数、读取数、gap 数量等)
├── ExportResult.java               # 导出结果(文件路径列表、导出条数、范围)
│
├── config/
│   ├── LatchQueueOptions.java          # 全局配置:rootPath + 多个命名 LatchQueueConfiguration
│   └── LatchQueueConfiguration.java    # 单队列配置(见第 8 节)
│
├── internal/
│   ├── DefaultLatchQueue.java          # 核心实现类
│   ├── DefaultLatchQueueFactory.java   # 工厂实现,按 (name, type) 缓存单例
│   ├── CompletedRange.java             # 区间合并数据结构
│   ├── BufferedLogEntry.java           # scan 线程写入有界队列的内部载体(原始字节 + 位置)
│   ├── CheckpointStore.java            # checkpoint 文件的原子读写
│   └── TypeStorageNaming.java          # 按类型/名称生成存储目录名(对应 FASTER 的 GetTypeStorageDirectoryName)
│
└── exception/
    ├── LatchQException.java            # 基础运行时异常
    ├── LatchQNotInitializedException.java
    └── LatchQInvalidConfigurationException.java
```

`latchq-spring-boot-starter` 只做胶水:`@ConfigurationProperties` 绑定 `LatchQueueOptions`、注册 `LatchQueueFactory` Bean、应用关闭时调用 `close()` 做优雅停机,不包含任何核心业务逻辑。

## 6. 核心 API 设计

```java
public interface LatchQueue<T> extends AutoCloseable {

    void initialize();

    boolean isInitialized();

    long write(T entity);

    List<Long> batchWrite(List<T> values);

    /** 阻塞直到读取到至少 1 条,或达到 count 上限;不做反序列化以外的额外等待 */
    LogEntryList<T> read(int count);

    /** 阻塞读取,支持超时;超时返回已读到的部分(可能为空列表) */
    LogEntryList<T> read(int count, Duration timeout);

    /** 乱序提交完成的位置区间,仅记录,不立即推进/持久化进度 */
    void commit(Collection<Position> positions);

    /** 手动跳过持久化的 gap,数据可能丢失,需谨慎调用 */
    void forceCommitGap(long gapStart, long gapEnd);

    LatchQueueMetrics metrics();

    ExportResult export(String targetDirectory, long fromIndex, Long toIndex, int entriesPerFile);

    @Override
    void close();
}

public interface LatchQueueFactory {
    <T> LatchQueue<T> getOrCreate(String name, Class<T> type);
}
```

`Position`、`LogEntry<T>`、`LogEntryList<T>` 的字段语义与 FASTER 版本(见 README 中的 `Position`/`LogEntry<T>` 定义)保持一致,仅把 `Address`/`NextAddress` 概念替换为 `Index`/`NextIndex`。

### 6.1 并发模型

- `write`/`batchWrite`/`read`/`commit` 是**普通同步阻塞方法**,不返回 `Future`/`CompletableFuture`。业务方自行决定用平台线程池还是虚拟线程池并发调用。
- 库内部所有后台任务(scan / complete / checkpoint / cleanup)运行在 `Thread.ofVirtual().name(...).start(...)` 创建的虚拟线程上,天然适配 Chronicle Queue 的阻塞 I/O 模型,避免阻塞平台线程池。
- `read()` 内部从有界阻塞队列(`ArrayBlockingQueue`,`BlockingQueue.put`/`poll` 语义,满时阻塞写入、不丢数据)中拉取;多个消费者线程可安全并发调用。
- `commit()` 内部用 `synchronized` 块或显式锁保护 `TreeSet<CompletedRange>`,允许多线程乱序调用。

## 7. 消费进度与 Gap 处理

完全复用 FasterLogger 已验证的算法,细节适配到 Chronicle 的 index 模型:

1. **区间记录**:`commit(positions)` 把每个 `Position(index, nextIndex)` 转换为 `CompletedRange`,加入 `TreeSet<CompletedRange>`(按 `index` 排序)。位置校验规则与 FASTER 版一致:无效区间跳过、已被截断的过期区间忽略、跨越 truncate 边界的区间忽略。
2. **区间合并**(后台 complete 任务,周期性执行):从当前 `truncateBeforeIndex` 开始,遍历有序区间集合,只要下一个区间的 `start == currentEnd`(**精确匹配,不需要容差**,因为 Chronicle 的 index 由 tailer 直接给出,不存在字节对齐间隙),就把它合并进连续段,推进 `currentEnd`。
3. **Gap 检测**:一旦某个区间的 `start > currentEnd`,说明前面有未完成的区间(gap),记录 gap 起止、首次检测时间,按 `GapTimeoutMillis` 周期性告警。
4. **强制跳过**:gap 持续时间超过 `ForceCompleteGapTimeoutMillis`(默认 120000ms,可配置为 0 关闭)时,记录 ERROR 日志并强制合并跳过该 gap,推进 truncate——**此时 gap 区间内未提交的数据会被跳过**,如果之后又收到该区间的迟到提交,会被当作"已早于 truncate 的过期提交"而忽略。这要求业务处理逻辑必须是幂等的(与 FASTER 版本的要求完全一致)。
5. **checkpoint 持久化**(后台 checkpoint 任务,周期性执行):把最新的 `truncateBeforeIndex` 原子写入本地小文件(先写临时文件,`fsync`,再 `Files.move` 做原子替换),避免写到一半崩溃导致 checkpoint 文件损坏。
6. **启动恢复**:`initialize()` 时读取 checkpoint 文件得到上次安全推进的 `truncateBeforeIndex`,创建 Tailer 后 `moveToIndex()` 定位到该位置开始扫描;若 checkpoint 文件不存在(首次启动),从队列起始位置开始。

## 8. 配置模型

```java
public class LatchQueueOptions {
    private String rootPath;                                   // 根目录,必填
    private Map<String, LatchQueueConfiguration> configurations = new HashMap<>();
    // configure(String name, Consumer<LatchQueueConfiguration>) 便捷方法
}

public class LatchQueueConfiguration {
    private String fileName;                     // 必填
    private String iteratorName = "default";
    private long capacity = 4L * 1024 * 1024 * 1024;   // 单个分段/总容量上限,具体语义对应 Chronicle 的 rollCycle+文件大小策略
    private int commitIntervalMillis = 2000;           // Chronicle 侧 appender flush/sync 间隔
    private int completeIntervalMillis = 3000;         // 区间合并任务周期
    private int checkpointIntervalMillis = 2000;        // checkpoint 落盘周期
    private int cleanupIntervalMillis = 300000;          // 旧分段清理周期(默认 5 分钟)
    private int preReadCapacity = 5000;                   // 有界队列容量
    private int gapTimeoutMillis = 600000;                // gap 告警周期(默认 10 分钟)
    private int forceCompleteGapTimeoutMillis = 120000;   // gap 强制跳过超时(默认 2 分钟,0=关闭)
    private int maxCompletedRanges = 10000;               // 区间集合内存保护上限
    // ...
}
```

命名与默认值刻意与 FASTER 版 `AbpFasterOptions`/`AbpFasterConfiguration` 保持对应关系,便于熟悉 FASTER 版的开发者直接迁移心智模型。存储目录规则同样按类型(泛型 `T` 的全限定名)+ 名称分子目录,同一 JVM 内可以并存多个不同类型、不同名称的队列实例(对应需求"支持多个不同类型的日志")。

`latchq-spring-boot-starter` 把 `LatchQueueOptions` 暴露为 `@ConfigurationProperties(prefix = "latchq")`,支持 YAML/Properties 配置。

## 9. 导出能力

```java
ExportResult export(String targetDirectory, long fromIndex, Long toIndex, int entriesPerFile);
```

行为对应 FASTER 版的 `ExportAsync`:使用一个**独立的临时 Tailer**(不影响主消费 Tailer 的位置和 checkpoint),按 index 范围扫描,每条记录的原始 JSON 字节 + 换行符追加写入文件,达到 `entriesPerFile` 条数后切分新文件。返回值包含实际导出的条数、文件路径列表、实际的 `fromIndex`/`toIndex`。

**该方法是纯 Java 库方法,LatchQ 本身不提供 HTTP/REST 接口**——按你的确认,由使用方(业务程序)自行在自己的 Controller 层包一层 API,调用这个方法把数据导出后返回给前端。

## 10. 技术基线与依赖

| 依赖 | 说明 |
|---|---|
| `net.openhft:chronicle-queue` | 持久化队列底层实现,开源版(`net.openhft:chronicle-bom` 做版本对齐) |
| `com.fasterxml.jackson.core:jackson-databind` | JSON 序列化,版本对齐参考 stow 项目的 `2.20.0` |
| `org.slf4j:slf4j-api` | 日志门面 |
| JUnit5 + AssertJ + Mockito + Awaitility | 测试栈,`Awaitility` 专门用于验证后台任务(如"gap 在超时后被自动跳过""checkpoint 文件最终被更新")这类异步收敛断言 |

**⚠️ 需要在实现阶段用实际引入的 Chronicle Queue 版本核实的技术细节**(以下是设计阶段基于对 Chronicle Queue 已发布 API 的认知给出的工作假设,不是凭空捏造,但精确的方法签名/是否存在自动化辅助 API 需要在写代码前对照当时拉取到的 `chronicle-queue` 版本的 Javadoc/源码逐一确认,并把确认结果补充回本文档):

1. **多线程写入的线程安全获取方式**:预期通过 `queue.acquireAppender()` 按线程获取独立的 `ExcerptAppender` 实例(Chronicle 官方文档描述的推荐用法是"每个写线程一个 appender 实例",`acquireAppender()` 内部按线程池化)。需要在实现阶段确认该方法是否可以直接从多个线程安全调用,或是否需要 LatchQ 自己维护 `ThreadLocal<ExcerptAppender>`。
2. **获取写入后的 index**:预期通过 `appender.writingDocument()` 返回的 `DocumentContext.index()`,或写入后调用 `appender.lastIndexAppended()`。
3. **Tailer 定位恢复**:预期 `ExcerptTailer.moveToIndex(long index)` 可用于从 checkpoint 恢复读取位置(这是 Chronicle Queue 长期稳定的公开 API,置信度较高)。
4. **index 编码与 cycle 边界**:Chronicle 的 index 由 `RollCycle` 把"滚动周期(cycle)"编码在高位、"序号(sequence)"编码在低位;同一 cycle 内序号连续递增,跨 cycle 边界时不能用 `index + 1` 做算术推导下一个 index——LatchQ 的设计**不依赖**跨边界算术,所有位置都直接从 `DocumentContext`/`Tailer` 读出,因此这一点主要影响"如何理解 index 语义",不影响正确性,但仍需在实现阶段验证行为符合预期。
5. **旧分段文件清理**:预期可以通过 `SingleChronicleQueueBuilder.storeFileListener(...)` 得到文件释放的回调,并在确认某个分段的所有数据都已经安全落后于 `truncateBeforeIndex` 后,手动删除对应的 `.cq4` 文件。是否存在更高层的官方封装 API(等价于 FASTER 的 `TruncateUntilPageStart`)需要在实现阶段确认。
6. **JDK 21 兼容性**:Chronicle Queue(经由 `chronicle-core`/`chronicle-bytes`)使用 `sun.misc.Unsafe` 等底层能力,在较新 JDK 上运行通常需要追加 `--add-opens`/`--add-exports` JVM 参数。需要在实现阶段拉取实际依赖后,通过实际运行确定所需的完整参数列表,并写入 `latchq-core`/示例模块的运行说明和测试插件配置(`maven-surefire-plugin`/`maven-failsafe-plugin` 的 `argLine`)。

## 11. 异常模型

- `LatchQException`:所有 LatchQ 自定义异常的基类(非受检异常,`RuntimeException` 子类,符合 Java 库的常见风格,也与 stow 项目一致)。
- `LatchQNotInitializedException`:调用 `write`/`read`/`commit` 前未 `initialize()`。
- `LatchQInvalidConfigurationException`:`rootPath`/`fileName` 等必填配置缺失或非法。
- 后台任务(scan/complete/checkpoint/cleanup)内部异常统一捕获、记录 ERROR 日志并重试,不让单次异常终止整个后台任务循环(行为对应 FasterLogger 的每个后台任务的 try/catch + 短暂 delay 重试)。

## 12. 测试与验证要求

- **单元测试**:区间合并算法(`CompletedRange` 合并逻辑)、checkpoint 原子写入/恢复、配置校验。
- **并发测试**:多线程并发 `write`、多线程并发 `read`+乱序 `commit`,验证最终 `truncateBeforeIndex` 收敛到正确值,且不重复、不丢失(在无 gap 强制跳过的前提下)。
- **崩溃恢复测试**:模拟进程在 checkpoint 落盘前/后强制退出(`kill -9` 或直接终止 JVM 子进程),重启后验证不多丢数据、不重复推进超出实际已处理范围。
- **Gap 超时测试**:使用 `Awaitility` 验证 gap 在配置的超时后被自动跳过、`forceCompleteGapTimeoutMillis=0` 时永不自动跳过。
- **构建门禁**:参考 stow 项目,`mvn verify` 阶段跑通 `jacoco` 覆盖率、`spotless` 格式检查。

## 13. 里程碑划分(概览,详细任务见实现计划文档)

1. **M1 核心读写**:`latchq-core` 基础骨架、配置模型、Chronicle Queue 写入/顺序扫描、有界队列扇出、同步 `read`/`write` API 跑通。
2. **M2 进度与 Gap**:区间合并算法、gap 检测/告警/强制跳过、checkpoint 原子持久化与恢复。
3. **M3 清理与导出**:旧分段清理、`export()` 能力。
4. **M4 Spring Boot Starter**:自动配置、示例项目跑通。
5. **M5 测试与基准**:补齐并发/崩溃恢复测试,`benchmarks` 模块跑通吞吐量基准。

## 14. 完成定义(Definition of Done,针对本设计阶段)

- [x] 明确问题边界与 v1 范围。
- [x] 确定核心架构方案(单一 scan 线程 + 有界队列扇出)。
- [x] 确定模块划分、包结构、命名风格。
- [x] 确定消费进度持久化与 gap 处理策略。
- [ ] Chronicle Queue 具体 API 方法签名在实现阶段逐一核实并回填本文档第 10 节。
- [ ] 用户审阅本文档并确认可进入实现计划(implementation plan)编写阶段。
