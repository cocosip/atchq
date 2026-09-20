# LatchQ 开发计划与进度跟踪

更新日期:2026-09-20

## 当前阶段

**全部里程碑(M0~M5)已完成,四轮代码走查完成** — 79 core + 7 starter 用例全绿,`mvnw verify`(测试 + jacoco + spotless)通过。

## 版本与基线(定稿)

| 项 | 值 |
|---|---|
| JDK | 21(OpenJDK/Temurin,固定) |
| Maven | 3.9.16,**项目自带 Maven Wrapper,统一用 `./mvnw` 构建** |
| Chronicle Queue | `net.openhft:chronicle-queue` **5.27ea5**(M0 实测通过;经 chronicle-bom 对齐) |
| Jackson | `jackson-databind` 2.20.0(对齐 stow) |
| 测试栈 | JUnit5 + AssertJ + Mockito + Awaitility |
| 构建 | 多模块结构、enforcer/spotless/jacoco/flatten 插件配置参照 stow 项目;JDK 21 运行需携带 M0 确定的 `--add-opens` 参数(已固化到 surefire argLine) |

## 里程碑总览

| 里程碑 | 内容 | 前置依赖 | 状态 |
|---|---|---|---|
| M0 | 技术验证(spike)与设计冻结 | 设计评审通过 | **已完成(2026-09-18,10/10 用例通过)** |
| M1 | 核心读写(latchq-core 骨架、配置、写入、scan 扇出、read API) | M0 | **已完成(2026-09-19,38 用例)** |
| M2 | 进度与 Gap(区间合并、gap 检测/跳过、checkpoint 持久化与恢复) | M1 | **已完成(2026-09-19,55 用例全绿)** |
| M3 | 清理与导出(cycle 文件清理、export API、metrics 补全) | M2 | **已完成(2026-09-19,61 用例全绿)** |
| M4 | Spring Boot Starter 与示例(自动配置、两个 sample) | M2 | **已完成(2026-09-19,64 用例全绿,示例实测跑通)** |
| M5 | 测试、基准与发布准备(并发/崩溃/roll 边界测试、JMH、文档) | M1~M4 | **已完成(2026-09-19,67 用例全绿)** |

> 单人开发按 M0→M1→M2→M3→M4→M5 顺序推进;M3 与 M4 无相互依赖,如有多人可并行。

---

## 任务分解

每个任务都有明确的产出与完成标准(DoD);任务编号在提交信息中引用(如 `T1.5`)。

### M0 技术验证与设计定稿(预计 0.5~1 天)

目的:在设计冻结前,用最小 spike 工程把设计文档第 10 节的全部"待核实项"变成"已确认项",避免实现期架构返工。spike 代码只求结论、不追求质量,验证完即丢弃或归档到 `docs/spike-notes.md`。

| # | 任务 | 产出 / DoD |
|---|---|---|
| T0.1 | 搭建最小 spike 工程,引入 `chronicle-queue 5.27ea5` | 依赖解析通过;确认 chronicle-bom 存在对应版本 |
| T0.2 | 验证写入侧:多线程 `acquireAppender()` 安全性;`DocumentContext.index()`/`lastIndexAppended()` 取值;`ExcerptAppender.sync()` 是否存在及语义 | 结论写入设计文档第 10 节 |
| T0.3 | 验证 index 语义(最高优先级):同一 cycle 内 index 连续性、跨 cycle 边界行为、`toIndex/indexToSequence/indexToCycle` 精确语义;实测设计文档 7.1 节两条 nextIndex 推导路线 | **确定 LatchQ 的 nextIndex 推导实现路线** |
| T0.4 | 验证读取侧:`moveToIndex()` 恢复;指向已清理数据时的行为;空队列上 `queue.firstIndex()` 的行为;kill -9 后脏尾部的自愈 | 结论回填设计文档 |
| T0.5 | 验证清理:`storeFileListener` 回调时机;删除旧 `.cq4` 文件对活跃 tailer 的影响 | 确定清理实现方案 |
| T0.6 | 确定 JDK 21 所需 `--add-opens`/`--add-exports` 完整清单 | 清单写入 surefire/failsafe `argLine` 与运行说明 |
| T0.7 | 回填设计文档第 10 节,冻结设计(v0.3) | 用户确认进入 M1 |

### M1 核心读写(预计 2~3 天)

| # | 任务 | 产出 / DoD |
|---|---|---|
| T1.1 | 搭建多模块工程骨架(parent + `latchq-core` + `latchq-spring-boot-starter` + `samples/*` + `benchmarks`),`${revision}` CI-friendly 版本 | `mvn verify` 空骨架全绿(enforcer/spotless/jacoco 生效) |
| T1.2 | 异常模型(`LatchQException` 等)+ 配置模型(含 fail-fast 校验、`builderCustomizer`) | 配置校验单测(缺 rootPath/fileName/未知队列名均抛异常) |
| T1.3 | `TypeStorageNaming`(类型名清洗、目录布局 `rootPath/<类型>/<fileName>/`) | 单测覆盖 Windows/Linux 非法字符 |
| T1.4 | `DefaultLatchQueueFactory`:按 (name, type) 缓存单例、`getOrCreate` 自动 `initialize()`、失败移除缓存(对应 FASTER 版 Lazy 语义) | 并发 `getOrCreate` 单测(多线程拿到同一实例) |
| T1.5 | 写入侧:ThreadLocal appender 封装,`write()`/`batchWrite()` 同步阻塞实现 | 单写多线程写单测通过,返回 index 单调 |
| T1.6 | `IndexCodec`:nextIndex 推导器(M0 定案的路线,纯函数) | **单测覆盖跨 cycle 边界**;这是全库正确性的地基 |
| T1.7 | scan 虚拟线程 + `ArrayBlockingQueue`(put/poll,满时阻塞不丢数据)扇出 | scan 停止/重启、队列背压单测 |
| T1.8 | `Position`/`LogEntry`/`LogEntryList` + `read(count)`(阻塞到至少 1 条)/`read(count, timeout)` | 多消费者并发 read 单测,不重不漏 |
| T1.9 | 优雅停机顺序:停 scan → 排空 → close queue;`close()` 幂等 | close 后再调 API 抛 `LatchQNotInitializedException`/状态异常 |

**M1 DoD**:demo(写线程 + 多读线程)跑通;T1.2~T1.8 单测全绿;spotless/jacoco 门禁通过。

### M2 进度与 Gap(预计 2~3 天)

| # | 任务 | 产出 / DoD |
|---|---|---|
| T2.1 | `CompletedRange` 区间合并算法(纯函数化) | 单测覆盖:相连/重叠/含容差为 0 的相邻判定/乱序插入 |
| T2.2 | `commit()` 校验规则(无效/过期 `nextIndex<=truncate`/跨界/重复,各自计数告警) | 单测逐条对应 FASTER 版行为 |
| T2.3 | complete 后台任务,两阶段加锁(锁内合并 → 锁外推进 → 锁内移除) | 周期推进的 Awaitility 测试 |
| T2.4 | gap 检测/告警/强制跳过(超时触发 + `maxCompletedRanges` 超限触发,两条件独立) | Awaitility:超时跳过、`=0` 不自动跳过、超限强制跳过 |
| T2.5 | `forceCommitGap()`(参数校验 + ERROR 日志) | 单测覆盖非法参数与重复填充 |
| T2.6 | `CheckpointStore`:temp + fsync + `Files.move` 原子写 | 单测:崩溃残留 temp 不影响恢复、损坏文件报错 |
| T2.7 | 启动恢复 + `max(checkpointIndex, firstIndex)` 钳制 | 单测:checkpoint 存在/缺失/落后于 firstIndex 三种场景 |
| T2.8 | `LatchQueueMetrics` 完整字段 | 指标数值与断言场景一致 |

**M2 DoD**:并发乱序 commit 收敛测试(多线程 read+乱序 commit,最终 truncate 收敛到正确值、不重不漏)、gap 系列测试、重启恢复测试全部通过。

### M3 清理与导出(预计 1~2 天)

| # | 任务 | 产出 / DoD |
|---|---|---|
| T3.1 | cycle 文件清理:整 cycle 粒度,只删"全部消息早于**已持久化 checkpoint index**"的 `.cq4` 文件 | 单测:内存 truncate 已推进但 checkpoint 未落盘时不删文件 |
| T3.2 | 清理安全测试:清理后立即模拟崩溃重启 | 已提交数据不因 checkpoint 滞后 + 文件已删而丢失 |
| T3.3 | `export()`:临时 tailer、`fromIndex` 钳制、`[from, to)` 语义、按 `entriesPerFile` 分文件、文件名防碰撞 | 单测:空范围/已清理范围/边界 index/round-trip(导出内容可重新反序列化) |
| T3.4 | `firstIndex()`/`lastIndexAppended()` 公开 API | 与 metrics 数值一致 |

**M3 DoD**:T3.1~T3.4 测试全绿;导出文件可直接被业务方按行 JSON 解析。

### M4 Spring Boot Starter 与示例(预计 1~2 天)

| # | 任务 | 产出 / DoD |
|---|---|---|
| T4.1 | starter:`@ConfigurationProperties(prefix = "latchq")` 绑定 `LatchQueueOptions`、注册 factory Bean、应用关闭时优雅停机 | starter 自身单测(ApplicationContext 启动 + 关闭钩子) |
| T4.2 | `latchq-sample-console`:纯 Java 手动装配示例 | README 步骤可复现 |
| T4.3 | `latchq-sample-spring-boot`:YAML 配置示例 + 演示"业务 Controller 包一层 export"的推荐用法 | README 步骤可复现 |
| T4.4 | 两个示例在 JDK 21 + M0 得出的完整 JVM 参数下运行验证 | 停机时已提交进度不丢(日志验证最终 checkpoint) |

**M4 DoD**:Spring Boot 应用仅靠 YAML 即可用;优雅停机验证通过。

### M5 测试、基准与发布准备(预计 2~3 天)

| # | 任务 | 产出 / DoD |
|---|---|---|
| T5.1 | 并发压力长跑:多写 + 多读 + 乱序 commit(含消费线程故意延迟制造 gap 再恢复) | 无数据重复/丢失;gap 恢复后 truncate 继续推进 |
| T5.2 | 崩溃恢复矩阵:kill -9 子进程(checkpoint 前/后 × 清理前/后 × sync 开/关) | 每种组合重启后行为符合第 7 节语义 |
| T5.3 | roll 边界专项:自定义短周期 RollCycle 频繁滚动 | 跨 cycle 合并/恢复/导出全部正确(7.1 陷阱的回归防线) |
| T5.4 | `benchmarks`(JMH):写吞吐、读扇出吞吐、sync 开关对比 | 结果记录到 `docs/benchmark-notes.md` |
| T5.5 | 文档:README、使用手册、配置参考、gap/幂等最佳实践(参考 FASTER 版 README 结构) | 覆盖"业务必须幂等"的核心契约 |
| T5.6 | 发布准备:Maven Central 发布坐标/签名流程(按需) | 可执行发布清单 |

**M5 DoD**:`mvn verify` 全绿;基准数据入档;文档齐备。

---

## 关键实现决策记录(评审后新增/修订)

- **nextIndex 推导**:候选两条路线(见设计文档 7.1),M0 T0.3 定案;逻辑收敛在 `IndexCodec` 纯函数类。绝不允许 `index + 1` 直推。
- **清理依据**:只看**已持久化 checkpoint index**(整 cycle 粒度),不看内存 `truncateBeforeIndex`。
- **启动恢复**:`max(checkpointIndex, queue.firstIndex())` 钳制。
- **强制跳过触发**:gap 超时 **或** 区间数超 `maxCompletedRanges`,两条件独立(与 FASTER 版对齐)。
- **配置查找 fail-fast**:未知队列名直接抛异常(有意偏离 FASTER 版的静默回退 default)。
- **`syncIntervalMillis`** 替代 FASTER `CommitIntervalMillis` 语义:Chronicle 写入即时可见,该参数只管刷盘窗口。
- **工厂自动初始化**:`getOrCreate` 内部 `initialize()`(对齐 FASTER 版 `CreateLogger`)。
- **对外 API 不暴露 Chronicle 类型**:`ExcerptAppender`/`ExcerptTailer`/`Wire` 等全部封装在 `internal` 包,隔离 ea 版本 API 变动风险。

## 风险登记册

| 风险 | 等级 | 缓解措施 |
|---|---|---|
| roll 边界 index 不连续导致永久假 gap → 每次滚动丢数据(设计陷阱) | 高 | M0 T0.3 专项验证;`IndexCodec` 单测覆盖跨 cycle;M5 T5.3 专项回归 |
| 清理与 checkpoint 竞争 → 重启后恢复位置指向已删文件 | 高 | 清理只依据已持久化 checkpoint(整 cycle 粒度)+ 启动钳制;T3.2 专项测试 |
| JDK 21 下 Chronicle 需要的 `--add-opens` 不全 → 运行期 `InaccessibleObjectException` | 中 | M0 T0.6 实测清单,固化到 surefire/failsafe `argLine` 与示例运行说明 |
| 5.27ea 为 early-access 版本,存在未知缺陷 | 中 | M0 spike 全量验证;阻断问题时降级 5.26ea 线;Chronicle 类型不外泄到公共 API |
| ea 版本间 API 变动导致后续升级成本 | 低 | 底层类型全部封装在 `internal` 包;升级只需回归,不破坏使用方 |
| 脏尾部(kill -9 半条消息)行为与预期不符 | 中 | M0 T0.4 验证;必要时参照 FASTER 版"Uninitialized page"恢复分支,在 scan 线程加对应的自愈逻辑 |

## 进度日志

- 2026-09-18:设计文档 v0.1 完成;对照 `SharpAbp.Abp.Faster` 源码完成评审,设计文档修订至 v0.2;本文档重写为任务分解式开发计划;版本基线定稿(`chronicle-queue 5.27ea5`,JDK 21)。
- 2026-09-18:**M0 完成**。T0.1~T0.6 全部执行完毕,10/10 验证用例通过(`ChronicleApiVerificationTest`),结论归档 [spike-notes.md](./spike-notes.md) 并回填设计文档第 10 节(v0.3,设计冻结)。关键定案:nextIndex 采用前瞻推导(7.1 路线 2);scan 线程用无名 tailer;JDK 21 需 4 个 `--add-opens` + 1 个 `--add-exports`;旧 cycle 文件可在队列打开时安全删除(`onReleased` 先行)。工程骨架(parent + latchq-core + Maven Wrapper 3.9.16)随本里程碑建立。下一步:M1 核心读写。
- 2026-09-19:**M1 完成**。核心读写落地(`LatchQueue` API、配置模型、工厂自动初始化、ThreadLocal appender 写入、scan 虚拟线程 + 有界队列扇出、优雅停机)。实现期两项重要发现并修复:① Chronicle 资源单线程约束——tailer 必须在 scan 线程内创建/使用/关闭;② 单条消息上限 = blockSize/2 - 4(默认 ~16MB)→ 新增 `maxMessageSizeBytes` 配置(默认按业务预期定 20MB)前置校验,Jackson 字符串读取限制同步放开。38 用例全绿。
- 2026-09-19:**M2 完成**。`commit()` 校验(无效/过期/跨界忽略)、区间合并(两阶段加锁)、gap 告警与强制跳过(超时与区间数超限两条件独立)、`forceCommitGap()`、`CheckpointStore` 原子写(temp+fsync+atomic move)、启动恢复(checkpoint 钳制 firstIndex + 修正随 checkpoint 持久化 + 种子修正)。修复一处对照 FASTER 源码发现的合并 bug:强制跳过应推进到 `range.end()` 而非 `range.start()`。55 用例全绿。下一步:M3 清理与导出。
- 2026-09-19:**M3 完成**。cycle 文件清理落地:只删"整 cycle 早于**已持久化 checkpoint**"的文件(persistedTruncate 由 checkpoint 任务成功写盘后更新),未打开过的旧文件用文件名差值法换算 cycle(时区偏移在差值中抵消,规避 M0 命名陷阱);删除失败容忍重试。`export()` 落地:独立 tailer、fromIndex 钳制、`[from, to)` 语义、按 entriesPerFile 分文件、文件名防碰撞、空结果 toIndex==fromIndex。新发现:`queue.firstIndex()` 会话内缓存,清理后滞后直至重开(已记录文档)。61 用例全绿。下一步:M4 Spring Boot Starter。
- 2026-09-19:**M4 完成**。`latchq-spring-boot-starter` 落地:`@ConfigurationProperties(prefix="latchq")` 绑定、AutoConfiguration.imports 注册、factory Bean(destroyMethod=close)随应用关闭做最终 checkpoint、`latchq.enabled` 开关;工厂补"关闭后 getOrCreate 抛异常"语义(测试暴露的真实缺陷)。两个示例落地并实测:console(100 写/100 读/100 提交收敛)与 spring-boot(YAML 配置 + Controller 包 export);示例运行需 MAVEN_OPTS 携带 JVM opens(已在 samples/README.md 记录)。修复:后台循环在 shutdown interrupt 落在文件 I/O 中间时静默退出,不再刷 ERROR。64 用例全绿。下一步:M5 测试、基准与文档。
- 2026-09-19:**M5 完成**。专项测试:并发长跑(3 写 4 读 + 故意停顿制造真实 gap 后恢复,不重不漏)、roll 边界专项(40 条跨多个 1 秒 cycle 位置单调不回退)、LatchQ 层崩溃矩阵(子进程经 LatchQueue API 写 30 条并 commit、checkpoint 落盘后 kill -9,重开不重复投递且可继续写);`latchq-benchmarks` JMH 模块(write/batchWrite/read,手动运行);根 README(快速上手 + JDK 21 参数 + 配置表)。67 用例全绿。
- 2026-09-19:**最终代码走查完成**。逐类审查核心并发代码后修复 2 处:① 工厂 `getOrCreate` 与 `close()` 的创建竞态——并发时新建队列漏关,改为创建后复查、已关闭则补关并抛异常;② 清理目录扫描的文件名差值法在夏令时切换时 cycle 换算可能偏差 ±1,极端情况会误删 checkpoint 所在 cycle,增加一个 cycle 的安全余量(精确映射分支不受影响)。审查确认的关键正确性点:合并的两阶段加锁无跨 I/O 持锁;commit/merge 的 stale 判定竞态无害(多出的 range 下轮合并消化);清理的 cycle 单调性依据成立(高 cycle ⇒ 高 index);checkpoint 原子写(temp+fsync+move)与最终 checkpoint 时序正确;export 的幻影文档防御与空结果语义正确。已知可接受限制:ThreadLocal appender 在短命写线程反复创建时资源累积至队列关闭才释放(长命线程池场景无影响)。修复后全量 verify 通过。
- 2026-09-19:**二轮完善(基准/README/CI/全量走查)**。① 基准测试落地并产出真实结果(write ≈ 1.21M msgs/s、batchWrite ≈ 1.03M msgs/s、read+commit 管线 ≈ 25.5k msgs/s,单消费者乒乓形态的下限值),修复 JMH fork 的类路径问题(改用 shade uber-jar 运行,fork JVM 自动携带 --add-opens);② README.md 全英文重写,含快速上手、完整配置表、gap/幂等说明与基准结果;③ CI/CD:GitHub Actions(`.github/workflows/ci.yml`)push/PR 触发 `mvnw verify`(测试 + jacoco + spotless:check 门禁,spotless 已绑入 verify),workflow_dispatch 可手动触发 JMH 基准(uber-jar);④ 全量走查二遍:遗留占位检查(无 TODO/Unsupported 残留)、starter 配置拷贝完整性(11 字段全覆盖)、构建警告修复(示例 jar 插件版本号)。全量 verify 通过(64 core + 3 starter)。
- 2026-09-19:**补充跨类共享入口 `LatchQueueHolder`(评审反馈)**。现状确认:starter 已支持依赖注入(`LatchQueueAutoConfiguration` 注册 `LatchQueueFactory` Bean,业务类构造器注入,spring-boot 示例即此用法);缺口在仅依赖 `latchq-core` 的非 DI 场景——多个业务类各自 `LatchQueueBuilder.build()` 会在同一队列文件上产生多个独立实例,checkpoint 与清理互相干扰。新增 `LatchQueueHolder`:`init(options|configurer)` 一次性初始化进程级共享工厂(重复初始化 fail-fast)、`getOrCreate(name, type)`/`getInstance()` 供任意类静态访问、`reset()` 关闭共享工厂并允许重新初始化(幂等);不注册 JVM shutdown hook,生命周期由应用负责;与 Spring 容器管理的 Bean 相互独立。新增 5 个单测(init 前访问/重复 init/缓存共享/并发首建/reset 后重开),全量 verify 通过(69 core + 3 starter)。
- 2026-09-19:**三轮走查(功能/性能/日志/Spring 集成)修复落地**。① P1 `syncIntervalMillis` 此前无任何实现(配置静默失效):按 spike 定案补齐后台 sync 线程,周期性对会话内全部 appender 调 `sync()`(已核实 `StoreAppender.sync`→`MappedBytesStore.syncUpTo` 为无线程亲和的 msync,跨线程调用安全),close 时补最终 sync,0 仍表示禁用;② 毒消息:`read` 反序列化失败改抛 `LatchQDeserializationException`(携带 index/nextIndex),配套 `forceCommitGap` 可显式跳过,未跳过则重启重投;同时 mapper 关闭 `FAIL_ON_UNKNOWN_PROPERTIES`,payload 加字段双向兼容,仅类型漂移会中毒;③ `close()` 现在唤醒阻塞在 `read` 的消费者(关闭标记自复制机制),不再挂死停机;④ 性能:export 加 64KB 缓冲(此前每条 2 次裸 write 系统调用),checkpoint 仅在进度或 corrections 变化时落盘(此前空闲也每 2s temp+fsync+move);⑤ 日志:truncate 推进 INFO→DEBUG(此前每 3s/队列刷屏),`currentGapCount` 在 gap 持续期间每轮合并刷新(此前只在有进展时更新,卡住时报旧值);⑥ Spring 集成:starter 补 `spring-boot-configuration-processor`(IDE 配置元数据)、配置映射收敛为 `LatchQueueProperties.toOptions()` 单一来源(附全字段拷贝断言测试)、新增条件化 `LatchQueueHealthIndicator`(actuator 在类路径时自动注册,工厂关闭报 DOWN;`LatchQueueFactory` 增 `isActive()` 默认方法);⑦ 文档:README 新增毒消息/payload 演进/停机行为三节,`batchWrite` 非原子语义写入 javadoc。评估项:scan 线程追平后 1ms 轮询为延迟/CPU 的既定取舍(与 provisional tail 投递耦合),保留;spring-boot 3.4.1 暂不升 3.5.x(非缺陷,单独 chore 处理)。新增 10 个测试(毒消息 2、关闭唤醒 2、gap 指标 1、sync 1、starter 映射 2、health 2),全量 verify 通过(75 core + 7 starter)。
- 2026-09-20:**四轮走查(全量复审)修复落地**。① P2 export 与 cleanup 竞态:export 钳制 `fromIndex` 后 cleanup 仍可能删掉其正在扫描的 cycle,恢复导出会静默缺失;新增 `activeExports` 计数,export 进行期间挂起清理(含未打开文件的 sweep);② P3 关闭标记满队列丢失:`close()` 的 `pending.offer(CLOSE_MARKER)` 在 hand-off 队列满时静默失败,存在消费者永久阻塞的理论窗口(注释"满即无 take 等待者"的假设有缝),改为有界重试(2s 宽限,超限告警);③ P3 `metrics()` 在 `initialize()` 窗口(已建 queue、未翻 initialized 标志)会抛 `LatchQNotInitializedException`,与自身防御性判空矛盾,改用 `initialized && !closed` 守卫,任意生命周期可调;顺带清理 `lastIndexAppended()` 死分支并为 `queue.lastIndex()` 补并发 close 容错(与 `safeFirstIndex` 一致);④ P3 fileName 大小写碰撞:`TypeStorageNaming` 小写化使 `"Audit"`/`"audit"` 两配置在大小写不敏感文件系统上共用同一目录,数据互写;新增 JVM 级目录守卫(`OPEN_QUEUE_DIRECTORIES`,折叠绝对路径为键),同目录第二实例 fail-fast,close 释放,同时覆盖"多个 builder 在同一目录重复建实例"的既有隐患,目录名布局不变;⑤ API 边界:`builderCustomizer` 是公共 API 中唯一 Chronicle 类型,确认为有意豁免并写入 AGENTS.md 与设计文档第 8 章;⑥ Spring 示例补毒消息处理(catch `LatchQDeserializationException` + 日志 + `forceCommitGap`),此前 worker 异常被 Future 吞掉、静默丢消息,与 README 指引相悖;⑦ `LatchQueueBuilder.create(name, type, options)` javadoc 与行为对齐(名称自动注册,未知名称不报错)。评估项保留:CheckpointStore rename 后不 fsync 目录(惯例可接受)、工厂 `computeIfAbsent` 内重初始化(功能正确)、启动首拍 checkpoint 冗余写(无害)。新增 4 个测试(目录守卫 2、满队列关闭 1、metrics 生命周期 1),全量 verify 通过(79 core + 7 starter)。
