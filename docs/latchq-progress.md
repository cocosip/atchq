# LatchQ 开发计划与进度跟踪

更新日期:2026-09-18

## 当前阶段

**设计定稿阶段** — 设计文档已对照 `SharpAbp.Abp.Faster` 源码完成一轮评审并回填修正(v0.2),待用户确认后进入 M0 技术验证。

## 版本与基线(定稿)

| 项 | 值 |
|---|---|
| JDK | 21(OpenJDK/Temurin,固定) |
| Maven | 3.9+ |
| Chronicle Queue | `net.openhft:chronicle-queue` **5.27ea5**(2026-09 Maven Central 最新社区版,经 chronicle-bom 对齐;M0 spike 若发现阻断性问题则降级 5.26ea 线并更新此表) |
| Jackson | `jackson-databind` 2.20.0(对齐 stow) |
| 测试栈 | JUnit5 + AssertJ + Mockito + Awaitility |
| 构建 | 多模块结构、enforcer/spotless/jacoco/flatten 插件配置参照 stow 项目 |

## 里程碑总览

| 里程碑 | 内容 | 前置依赖 | 状态 |
|---|---|---|---|
| M0 | 技术验证(spike)与设计冻结 | 设计评审通过 | **未开始(下一步)** |
| M1 | 核心读写(latchq-core 骨架、配置、写入、scan 扇出、read API) | M0 | 未开始 |
| M2 | 进度与 Gap(区间合并、gap 检测/跳过、checkpoint 持久化与恢复) | M1 | 未开始 |
| M3 | 清理与导出(cycle 文件清理、export API、metrics 补全) | M2 | 未开始 |
| M4 | Spring Boot Starter 与示例(自动配置、两个 sample) | M2 | 未开始 |
| M5 | 测试、基准与发布准备(并发/崩溃/roll 边界测试、JMH、文档) | M1~M4 | 未开始 |

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
