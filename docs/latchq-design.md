# LatchQ 设计文档

## 1. 文档状态

| 项 | 值 |
|---|---|
| 状态 | 设计冻结(M0 技术验证完成,待核实项已全部回填) |
| 版本 | v0.3 |
| 更新日期 | 2026-09-18 |
| 参考实现 | [SharpAbp.Abp.Faster](https://github.com/sharpabp/sharp-abp)(基于 Microsoft FASTER 的 .NET 持久化日志库) |
| 底层依赖 | [OpenHFT Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue) |

> **v0.2 修订记录(2026-09-18,对照 `SharpAbp.Abp.Faster` 源码逐条评审后)**:
> 1. 新增 7.1 节:Chronicle 下 `nextIndex` 的推导方式与 roll 边界陷阱——本次评审发现的最重要问题,原稿"tailer 直接给出 (index, nextIndex)"的假设不成立。
> 2. 清理改为只依据**已持久化 checkpoint** 删文件;启动恢复增加 `firstIndex` 钳制(第 7 节)。
> 3. 补齐 `maxCompletedRanges` 超限同样触发强制跳过的语义(与 FASTER 版对齐)。
> 4. 厘清 Chronicle 无 commit 概念,`commitIntervalMillis` 重命名为 `syncIntervalMillis`(周期性刷盘)。
> 5. 删除无意义的 `iteratorName` 配置;容量类参数收敛为 `rollCycle`;新增 builder 调优钩子;配置查找改为 fail-fast(第 8 节)。
> 6. 工厂 `getOrCreate` 自动初始化;补 `firstIndex()`/`lastIndexAppended()` API 与指标定义(第 6 节)。
> 7. 版本基线定稿 `chronicle-queue 5.27ea5`,待核实清单扩充(第 10 节)。

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
| 区间合并容差 | `AddressMatchTolerance`(字节容差,因为字节地址可能有对齐间隙) | **不需要容差**,用精确的 `(index, nextIndex)` 相等匹配;但 Chronicle 不直接给出 nextIndex,必须按 RollCycle 规则推导(见 7.1 节),不能简单 `index + 1` |
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

    /** 队列中现存最早一条记录的 index(被清理的数据不可见),对应 FASTER 版 BeginAddress */
    long firstIndex();

    /** 最近一次写入的 index,对应 FASTER 版 CommittedUntilAddress,可作 export 的上界 */
    long lastIndexAppended();

    LatchQueueMetrics metrics();

    ExportResult export(String targetDirectory, long fromIndex, Long toIndex, int entriesPerFile);

    @Override
    void close();
}

public interface LatchQueueFactory {
    <T> LatchQueue<T> getOrCreate(String name, Class<T> type);
}
```

`Position`、`LogEntry<T>`、`LogEntryList<T>` 的字段语义与 FASTER 版本(见 README 中的 `Position`/`LogEntry<T>` 定义)保持一致,仅把 `Address`/`NextAddress` 概念替换为 `Index`/`NextIndex`。注意:`Position(index, nextIndex)` 中的 `nextIndex` 是"下一条记录的 index",由库内部按 RollCycle 规则推导(见 7.1 节),业务方拿到的 Position 永远首尾相接、可直接用于 `commit()`。

`LatchQueueMetrics` 为只读快照,字段对齐 FASTER 版:`totalWriteCount`、`totalReadCount`、`totalCommittedRanges`、`currentGapCount`、`largestGapSize`(**单位为条数**;FASTER 版按字节统计,Chronicle 下没有稳定的字节宽度概念)、`completedRangeCount`、`truncateBeforeIndex`、`firstIndex`、`lastIndexAppended`。

工厂行为对齐 FASTER 版 `CreateLogger`:`LatchQueueFactory.getOrCreate(name, type)` 内部**自动完成 `initialize()`**,调用方无需手动初始化;显式 `initialize()` 仅保留给 `LatchQueueBuilder` 手动装配场景。

### 6.1 并发模型

- `write`/`batchWrite`/`read`/`commit` 是**普通同步阻塞方法**,不返回 `Future`/`CompletableFuture`。业务方自行决定用平台线程池还是虚拟线程池并发调用。
- 库内部所有后台任务(scan / complete / checkpoint / cleanup)运行在 `Thread.ofVirtual().name(...).start(...)` 创建的虚拟线程上,天然适配 Chronicle Queue 的阻塞 I/O 模型,避免阻塞平台线程池。
- `read()` 内部从有界阻塞队列(`ArrayBlockingQueue`,`BlockingQueue.put`/`poll` 语义,满时阻塞写入、不丢数据)中拉取;多个消费者线程可安全并发调用。
- `commit()` 内部用 `synchronized` 块或显式锁保护 `TreeSet<CompletedRange>`,允许多线程乱序调用。

## 7. 消费进度与 Gap 处理

完全复用 FasterLogger 已验证的算法,细节适配到 Chronicle 的 index 模型。

### 7.1 nextIndex 的推导与 roll 边界(评审修正,最关键的适配点)

Chronicle 的 index 高位编码 cycle、低位编码序号:**同一 cycle 内每条消息的序号递增 1,但跨 cycle 边界(滚动)时 index 不连续**。由此:

- **不能**用 `index + 1` 作为 nextIndex——否则每次 roll(按天/按小时滚动)都会制造一个"假 gap":区间合并推不过边界,gap 挂起 2 分钟后被强制跳过,**每次滚动都静默丢一批数据**。这比 FASTER 版的容差问题严重,属于正确性陷阱。
- Chronicle 的 `DocumentContext`/`ExcerptTailer` **并不直接提供 nextIndex**(只给当前 index),必须自行推导。**M0 已定案:采用路线 2(前瞻推导)**,并补充两条投递规则(M1 实现定案):
  1. **前瞻回填**:scan 线程读出第 n+1 条时,把其实际 index 回填为第 n 条的 nextIndex(scan 线程内部持有 1 条 read-ahead 缓冲)——交付给消费者的位置永远链到真实下一条。
  2. **追平临时投递**:队列追平(生产者空闲)时,被扣住的尾部条目以"同 cycle 内 `toIndex(cycle, seq+1)` 的临时 nextIndex"立即投递,避免空闲生产者饿死消费者;scan 线程记住该临时值,当真实下一条落点与之不同(空闲期间发生了 roll)时记录一条**空隙修正 `[provisional → real]`**(空隙内没有任何消息),M2 的合并算法遇中与修正完全匹配的 gap 立即跳过(不丢数据、不等强制跳过超时),修正随 checkpoint 持久化。
- 推导逻辑收敛在独立的纯函数类(如 `IndexCodec`)中,单测必须覆盖跨 cycle 边界用例。

### 7.2 算法流程

1. **区间记录**:`commit(positions)` 把每个 `Position(index, nextIndex)` 转换为 `CompletedRange`,加入 `TreeSet<CompletedRange>`(按 `index` 排序)。位置校验规则与 FASTER 版一致:无效区间跳过、`nextIndex <= truncateBeforeIndex` 的过期区间忽略、跨越 truncate 边界的区间忽略,重复区间由集合去重。
2. **区间合并**(后台 complete 任务,周期性执行):从当前 `truncateBeforeIndex` 开始,遍历有序区间集合,只要下一个区间的 `start == currentEnd`(**精确相等,不需要 FASTER 的 `AddressMatchTolerance` 容差**,前提是 7.1 的 nextIndex 推导正确),就把它合并进连续段,推进 `currentEnd`。实现保持 FASTER 版的两阶段加锁:锁内合并计算 → 锁外推进/持久化 → 锁内移除已合并区间,避免跨 I/O 持锁。
3. **Gap 检测**:一旦某个区间的 `start > currentEnd`,说明前面有未完成的区间(gap),记录 gap 起止、首次检测时间;`gapTimeoutMillis` 既是**首次告警延迟**也是**重复告警周期**(与 FASTER 版行为一致)。
4. **强制跳过**:满足任一条件即记录 ERROR 日志并强制合并跳过该 gap、推进 truncate——(a) gap 持续时间超过 `forceCompleteGapTimeoutMillis`(默认 120000ms,可配置为 0 关闭);(b) 区间数量超过 `maxCompletedRanges`(**内存保护,与超时独立生效,即使超时配置为 0 也会触发**——与 FASTER 版一致)。跳过后,gap 区间内未提交的数据会被放弃;之后迟到的提交会被当作"已早于 truncate 的过期提交"忽略。这要求业务处理逻辑必须是幂等的(与 FASTER 版要求一致)。
5. **checkpoint 持久化**(后台 checkpoint 任务,周期性执行):把最新的 `truncateBeforeIndex` 原子写入本地小文件(先写临时文件,`fsync`,再 `Files.move` 原子替换),避免写到一半崩溃导致 checkpoint 文件损坏。`close()` 优雅停机时,先停 scan、再做**最终一次 checkpoint**,然后才关闭 queue。
6. **启动恢复**:`initialize()` 时读取 checkpoint 文件得到上次安全推进的 `truncateBeforeIndex`,创建 Tailer 后 `moveToIndex()` 定位到该位置开始扫描;恢复位置取 `max(checkpointIndex, queue.firstIndex())`(对应 FASTER 版 `Math.Max(CompletedUntilAddress, BeginAddress)`),防止 checkpoint 指向已被清理的数据;若 checkpoint 文件不存在(首次启动),从 `queue.firstIndex()` 开始。
7. **清理安全顺序**(评审新增):cleanup 只允许删除"**整个 cycle 的全部消息都早于已持久化 checkpoint index**"的 `.cq4` 文件,绝不依据内存中的 `truncateBeforeIndex` 删除——否则"删文件 → 崩溃 → 从旧 checkpoint 恢复"会造成数据静默丢失。顺序上 checkpoint 先行、清理后行。

## 8. 配置模型

```java
public class LatchQueueOptions {
    private String rootPath;                                   // 根目录,必填
    private Map<String, LatchQueueConfiguration> configurations = new HashMap<>();
    // configure(String name, Consumer<LatchQueueConfiguration>) 便捷方法
}

public class LatchQueueConfiguration {
    private String fileName;                              // 必填,队列目录名
    private long maxMessageSizeBytes = 20L * 1024 * 1024; // 单条消息序列化后大小上限(默认 20MB,覆盖 1MB 以内的常规消息与偶发大消息),超限写入前置拒绝(库内部换算 Chronicle blockSize,可用写空间 = blockSize/2 - 4)
    private String rollCycle = "DEFAULT";                 // Chronicle 滚动策略(按名称从 RollCycles 常量反射解析),承担 FASTER 版 Capacity/SegmentSizeBits 的容量语义
    private int syncIntervalMillis = 2000;                // 周期性 appender.sync() 刷盘间隔,见下方字段对应说明
    private int completeIntervalMillis = 3000;            // 区间合并任务周期
    private int checkpointIntervalMillis = 2000;          // checkpoint 落盘周期
    private int cleanupIntervalMillis = 300000;           // 旧分段清理周期(默认 5 分钟)
    private int preReadCapacity = 5000;                   // 有界队列容量
    private int gapTimeoutMillis = 600000;                // gap 首次告警延迟与重复告警周期(默认 10 分钟)
    private int forceCompleteGapTimeoutMillis = 120000;   // gap 强制跳过超时(默认 2 分钟,0=关闭)
    private int maxCompletedRanges = 10000;               // 区间集合内存保护上限,超限同样触发强制跳过(见 7.2-4)
    // builderCustomizer:Consumer<SingleChronicleQueueBuilder>,高级调优钩子(在 blockSize/rollCycle 之后应用,可覆盖一切)
}
```

默认值与 FASTER 版 `AbpFasterOptions`/`AbpFasterConfiguration` 对齐,便于熟悉 FASTER 版的开发者迁移心智模型。因底层差异导致的字段映射(评审修正):

| FASTER 版字段 | LatchQ 处理 |
|---|---|
| `CommitIntervalMillis` | 重命名为 `syncIntervalMillis`。Chronicle 写入即时对 tailer 可见(memory-mapped),不存在 FASTER 式"提交后才可见";该参数定义为周期性调用 `appender.sync()` 强制刷盘的间隔,决定**崩溃时尾部可能丢失的数据窗口**(`sync()` 的存在性待 M0 核实) |
| `Capacity`/`PageSizeBits`/`MemorySizeBits`/`SegmentSizeBits` | 收敛为 `rollCycle`:Chronicle 按 cycle 滚动文件,容量语义由 RollCycle(dataBlockSize/indexCount/indexSpacing)承担 |
| `IteratorName` | **删除**:Chronicle 没有 named iterator 概念,消费进度由 LatchQ 自己的 checkpoint 文件持久化 |
| `AddressMatchTolerance` | **删除**:精确匹配,见 7.1 节 |
| `PreallocateFile`/`RecoverDevice`/`DisableFileBuffering`/`UseIoCompletionPort`/`ScanUncommitted`/`AutoRefreshSafeTailAddress` | 不引入,均为 FASTER 设备层参数,Chronicle 无对应物;个别高级需求走 `builderCustomizer` |

配置查找行为与 FASTER 版不同:**fail-fast**——FASTER 版 `GetConfiguration(name)` 找不到时静默回退 default 配置,LatchQ 改为直接抛 `LatchQInvalidConfigurationException`,避免拼错队列名时意外落到错误配置。

存储目录规则:`rootPath/<类型全限定名(非法字符清洗)>/<fileName>/`,与 FASTER 版一致(name 只用于选择配置、不进路径);checkpoint 文件(`checkpoint`)放在队列目录内,随队列一起管理。同一 JVM 内可并存多个不同类型、不同名称的队列实例(对应需求"支持多个不同类型的日志")。

`latchq-spring-boot-starter` 把 `LatchQueueOptions` 暴露为 `@ConfigurationProperties(prefix = "latchq")`,支持 YAML/Properties 配置。

## 9. 导出能力

```java
ExportResult export(String targetDirectory, long fromIndex, Long toIndex, int entriesPerFile);
```

行为对应 FASTER 版的 `ExportAsync`:使用一个**独立的临时 Tailer**(不影响主消费 Tailer 的位置和 checkpoint),按 index 范围扫描,每条记录的原始 JSON 字节 + 换行符追加写入文件,达到 `entriesPerFile` 条数后切分新文件。返回值包含实际导出的条数、文件路径列表、实际的 `fromIndex`/`toIndex`。评审补充:

- `fromIndex` 小于 `firstIndex()`(数据已被清理)时,钳制到 `firstIndex()` 并记录 WARN(对应 FASTER 版按 `BeginAddress` 钳制的防御)。
- 扫描范围为 `[fromIndex, toIndex)`,`toIndex` 为排他上界,缺省时导出全部现存数据(对应 FASTER 版 `toAddress = CommittedUntilAddress`)。
- 并发导出的文件名带毫秒时间戳 + 短随机串防碰撞(与 FASTER 版一致)。

**该方法是纯 Java 库方法,LatchQ 本身不提供 HTTP/REST 接口**——按你的确认,由使用方(业务程序)自行在自己的 Controller 层包一层 API,调用这个方法把数据导出后返回给前端。

## 10. 技术基线与依赖

| 依赖 | 说明 |
|---|---|
| `net.openhft:chronicle-queue` | 持久化队列底层实现,开源版。**版本基线:5.27ea5**(M0 已验证,JDK 21 下核实用例通过;经 `net.openhft:chronicle-bom` 做版本对齐) |
| `com.fasterxml.jackson.core:jackson-databind` | JSON 序列化,版本对齐参考 stow 项目的 `2.20.0`;内部 ObjectMapper 放开 `StreamReadConstraints` 字符串上限(单条大小统一由 `maxMessageSizeBytes` 在写入侧管控) |
| `org.slf4j:slf4j-api` | 日志门面,固定 2.x;**core 及 starter 不携带任何 slf4j 绑定实现**,实现(如 log4j2)由上层应用自行提供 |
| JUnit5 + AssertJ + Mockito + Awaitility | 测试栈,`Awaitility` 专门用于验证后台任务(如"gap 在超时后被自动跳过""checkpoint 文件最终被更新")这类异步收敛断言 |

构建基线:项目自带 **Maven Wrapper(3.9.16)**,统一用 `./mvnw` 构建,与全局 Maven 解耦;`.flattened-pom.xml`(flatten 插件处理 `${revision}` 的产物)不入库。

**✅ 第 10 节待核实项已全部完成核实(M0,2026-09-18,详见 [spike-notes.md](./spike-notes.md))**,实现必须遵守的结论:

1. **多线程写入**:`createAppender()` 多线程各自调用安全(2000 条并发写全局唯一 index),LatchQ 用 `ThreadLocal<ExcerptAppender>` 封装,无需加锁。注意 5.27ea5 **没有** `acquireAppender()`。
2. **写入 index 与刷盘**:`DocumentContext.index()` / `appender.lastIndexAppended()` 均可用;`ExcerptAppender.sync()` 存在,`syncIntervalMillis` 定为周期性调用 `appender.sync()`。
3. **nextIndex 推导与 roll 边界**:陷阱实测成立(空闲跨 cycle 后任何算术推导都猜不中真实下一条 index);同 cycle 内 index 严格 +1 稠密。**已定案采用前瞻推导(7.1 节路线 2)**。
4. **定位恢复**:`moveToIndex(i)` 定位到 i 这条消息本身;空队列 `firstIndex()` 返回 `Long.MAX_VALUE`(不抛异常),钳制逻辑需处理;**指向空 cycle 的 moveToIndex 会先返回 present-but-empty 的幻影文档** → 恢复只允许用真实消息 index,scan 线程需跳过空文档。
5. **脏尾部**:kill -9 写一半后,半条消息不可见、已有消息完整可读、队列可继续写入——Chronicle 自愈,无需自建 FASTER 式恢复分支。
6. **清理**:旧 cycle `.cq4` 文件可在队列打开时删除(Windows 实测成功),删除前该 cycle 的 `onReleased` 已触发;`firstIndex()` 删除后正确前移;**不要按 cycle 反推文件名**,枚举目录处理,删除失败下轮重试。
7. **JDK 21 JVM 参数**(已固化到根 pom surefire argLine,应用运行说明同样要求):
   `--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.base/jdk.internal.ref=ALL-UNNAMED`
8. **其他实现约束**(M0 附带发现):scan 线程必须用**无名 tailer**(同名 tailer 会恢复 Chronicle 记住的位置);`RollCycle.defaultIndexCount` 必须 ≥ 单 cycle 消息量;`Bytes` 必须显式 `releaseLast()`;`queue.lastIndex()` 存在,用于 `lastIndexAppended()`。

## 11. 异常模型

- `LatchQException`:所有 LatchQ 自定义异常的基类(非受检异常,`RuntimeException` 子类,符合 Java 库的常见风格,也与 stow 项目一致)。
- `LatchQNotInitializedException`:调用 `write`/`read`/`commit` 前未 `initialize()`。
- `LatchQInvalidConfigurationException`:`rootPath`/`fileName` 等必填配置缺失或非法。
- 后台任务(scan/complete/checkpoint/cleanup)内部异常统一捕获、记录 ERROR 日志并重试,不让单次异常终止整个后台任务循环(行为对应 FasterLogger 的每个后台任务的 try/catch + 短暂 delay 重试)。

## 12. 测试与验证要求

- **单元测试**:区间合并算法(`CompletedRange` 合并逻辑)、checkpoint 原子写入/恢复、配置校验。
- **并发测试**:多线程并发 `write`、多线程并发 `read`+乱序 `commit`,验证最终 `truncateBeforeIndex` 收敛到正确值,且不重复、不丢失(在无 gap 强制跳过的前提下)。
- **崩溃恢复测试**:模拟进程在 checkpoint 落盘前/后强制退出(`kill -9` 或直接终止 JVM 子进程),重启后验证不多丢数据、不重复推进超出实际已处理范围。
- **roll 边界专项测试**(评审新增):使用自定义短周期 RollCycle(或手动触发 roll)让队列频繁滚动,验证跨 cycle 的区间合并、checkpoint 恢复、导出均正确——这是 7.1 节陷阱的回归防线。
- **清理安全测试**(评审新增):清理执行后立即模拟崩溃重启,验证已提交数据不因"checkpoint 滞后 + 文件已删"而丢失。
- **Gap 超时测试**:使用 `Awaitility` 验证 gap 在配置的超时后被自动跳过、`forceCompleteGapTimeoutMillis=0` 时永不自动跳过(但区间数超过 `maxCompletedRanges` 仍触发跳过)。
- **构建门禁**:参考 stow 项目,`mvn verify` 阶段跑通 `jacoco` 覆盖率、`spotless` 格式检查。

## 13. 里程碑划分(概览,详细任务分解见 [latchq-progress.md](./latchq-progress.md))

0. **M0 技术验证与设计定稿**:搭建最小 spike 工程引入 `chronicle-queue 5.27ea5`,核实第 10 节全部待核实项(重点:nextIndex 推导与 roll 边界),结论回填本文档后冻结设计。
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
- [x] 对照参考实现(SharpAbp.Abp.Faster 源码)完成一轮设计评审,修正项已回填(v0.2)。
- [x] Chronicle Queue 具体 API 在 M0 技术验证中核实完毕并回填本文档第 10 节(v0.3,详见 spike-notes.md)。
- [ ] 用户审阅本文档(v0.3)并确认进入 M1 实现。
