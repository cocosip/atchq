# M0 技术验证结论(spike notes)

| 项 | 值 |
|---|---|
| 验证日期 | 2026-09-18 |
| 环境 | JDK Temurin 21.0.12.1 / Windows 10 x64 / Maven 3.9.16(项目内 wrapper) |
| 依赖版本 | chronicle-queue **5.27ea5**(经 Maven Central 解析成功) |
| 验证代码 | `latchq-core/src/test/java/io/github/cocosip/latchq/chronicle/ChronicleApiVerificationTest.java`(10 个用例全部通过,同时作为后续回归基线) |

## 1. 5.27ea5 实际 API 与设计阶段假设的差异(实现必须按此)

| 设计假设 | 5.27ea5 实际 |
|---|---|
| `SingleChronicleQueueBuilder`(顶层包) | 在 `net.openhft.chronicle.queue.impl.single` 包;通过 `ChronicleQueue.singleBuilder(path)` 获取 |
| `queue.acquireAppender()`(按线程池化) | **不存在**,使用 `queue.createAppender()`;测试验证了多线程各自 createAppender 并发写入安全(2000 条全局唯一 index) |
| `RollCycle.indexCount()/indexSpacing()/indexToCycle()/indexToSequence()/maxMessages()/current_cycle(epoch,time)`,`format()` 返回 SimpleDateFormat | 实际接口:`defaultIndexCount()/defaultIndexSpacing()/toCycle(long)/toSequenceNumber(long)/maxMessagesPerCycle()`,`current(TimeProvider, long)`,**`format()` 返回日期 pattern 字符串**;`defaultEpoch()` 为 default 方法 |
| `RollCycles.XLARGE_DAILY` 常量按名直接使用 | 该名称在此版本不存在;实现层用**反射按名称从 `RollCycles` 解析**,避免对常量名硬编码 |
| `wire().writeBytes(byte[])` / `readBytes()` | 实际签名:`writeBytes(WriteBytesMarshallable)`(lambda `out -> out.write(data)`,长度前缀存储)、`readBytes(Bytes)`(读入可复用 buffer) |

其余关键 API 均与假设一致并验证通过:`ExcerptAppender.writingDocument()` + `DocumentContext.index()`、`ExcerptTailer.readingDocument()/moveToIndex(long)`、`queue.firstIndex()/lastIndex()`、`appender.lastIndexAppended()`、`appender.sync()`、`builder.epoch(long)/storeFileListener(StoreFileListener)`。

## 2. 逐项结论(对应设计文档第 10 节)

### T0.2 写入侧
- 多线程各自 `createAppender()` 并发写 4×500 条:全局 index 唯一、线程内稠密、全量 readback 完整。LatchQ 用 `ThreadLocal<ExcerptAppender>` 封装即可,**无需额外加锁**。
- `appender.lastIndexAppended()` == 最后一条写入的 `DocumentContext.index()`。
- `appender.sync()` 存在且可用;sync 后重开队列数据完整 → `syncIntervalMillis` 的实现方式定为周期性调用 `appender.sync()`。

### T0.3 index 语义与 roll 边界(核心陷阱,已实测确认)
- 同一 cycle 内消息 index **严格 +1 稠密**(无空洞,含回滚场景未见空洞)。
- 空闲 ≥ 2 个 cycle 后再写入:真实下一条 index 既不等于 `index+1` 也不等于 `toIndex(cycle+1, 0)`(实测 routeA 偏差跨越多个 cycle)。**设计文档 7.1 节的陷阱成立**。
- 无空闲的普通边界:新 cycle 首条 `toSequenceNumber == 0`,cycle 内算术推导正确。
- **定案:nextIndex 采用 7.1 节路线 2(前瞻推导)**——scan 线程读出第 n+1 条时回填第 n 条的 nextIndex(scan 线程内部持有 1 条 read-ahead 缓冲);未读到下一条之前,该条不投递给消费者。路线 1(算术推导)在任何"空闲跨 cycle"场景都会制造假 gap,弃用。

### T0.4 定位恢复 / firstIndex / lastIndex
- `moveToIndex(i)` 语义 = **定位到 i 这条消息本身**,下一次 `readingDocument()` 返回 i(恢复设计:`moveToIndex(truncateBeforeIndex)` 后第一条即"第一条未处理消息",与设计一致)。
- 空队列 `queue.firstIndex()` **返回 `Long.MAX_VALUE`(9223372036854775807),不抛异常**;启动恢复钳制 `max(checkpointIndex, firstIndex)` 必须处理空队列分支(首条消息读出前 truncate 保持未定,由 scan 线程 CAS 写入首条 index)。
- `queue.lastIndex()` 存在且 == 最后一条写入 index → `LatchQueue.lastIndexAppended()` 用它实现。
- **指向空 cycle 的 moveToIndex 会先返回一个"present 但无数据"的幻影文档**(index 恰为请求位置),之后才前跳到真实数据 → 恢复定位只允许使用真实消息 index(Route B 保证 truncateBeforeIndex 总是真实消息 index);scan 线程对 present-but-empty 文档需防御性跳过。

### T0.4b 脏尾部自愈
- 子进程写入 20 条正常消息后,在写一条 3MB 消息的中途 `Runtime.halt(7)`(等价 kill -9)。
- 重新打开队列:20 条完整可读,**半条消息不可见**;随后可正常追加新消息并全部读回。Chronicle 对脏尾部自愈,无需 LatchQ 实现 FASTER 式"Uninitialized page"恢复分支(保留防御性日志)。

### T0.5 清理
- 旧 cycle 的 `.cq4` 文件在**队列打开状态下可删除**(Windows 实测成功),且删除前该 cycle 的 `StoreFileListener.onReleased` 回调已触发 → 清理策略:只删 `onReleased` 已触发且 cycle 早于已持久化 checkpoint 的文件,删除失败(偶发句柄延迟)则下轮重试。
- 删除后 `queue.firstIndex()` 正确前移到存活数据,读写不受影响。
- **不要自行按 cycle 反推文件名**(Chronicle 内部命名约定与时区相关,实测与本地时区格式化结果不一致);清理时枚举目录内 `.cq4` 文件处理。

### T0.6 JDK 21 所需 JVM 参数(已固化到根 pom 的 surefire argLine)
```
--add-opens java.base/java.lang=ALL-UNNAMED
--add-opens java.base/java.lang.reflect=ALL-UNNAMED
--add-opens java.base/java.io=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
--add-exports java.base/jdk.internal.ref=ALL-UNNAMED
```
缺失时的症状:`SingleChronicleQueueBuilder` 静态初始化失败(`IllegalAccessException: module java.base does not open java.lang.reflect`),所有队列操作不可用。子进程/示例应用运行时需携带同样参数。

## 3. 附带发现(影响实现细节)

1. **同名 tailer 位置持久化**:`createTailer(name)` 用相同名字创建会恢复 Chronicle 记住的读取位置(测试中第二次 readAll 只读到增量)。LatchQ 的 scan 线程必须使用**无名 tailer**(`createTailer()`),位置完全由 LatchQ 自己的 checkpoint 管理,避免双重进度来源。
2. **`RollCycle.defaultIndexCount` 必须 ≥ 单 cycle 内消息量**,否则写入报 `IllegalStateException: Unable to index 64, ...`(indexCount=8 时 64 条即触顶)。配置模型中 rollCycle 的选择必须提示该约束;默认 `XLARGE_DAILY`(可索引约 800 万条/cycle)。
3. **`Bytes` 参与引用计数**,必须 `releaseLast()`,否则资源泄漏告警并可能干扰关闭。
4. **Windows 下队列 close 后 .cq4/metadata.cq4t 句柄释放有短暂延迟**,测试目录清理偶发失败 → 测试数据放 `target/test-data`,由 `mvn clean` 兜底;生产清理逻辑需容忍删除失败并重试。
5. 项目自带 Maven Wrapper(3.9.16),构建统一使用 `./mvnw`,与全局 Maven 解耦;`.flattened-pom.xml` 已加入 .gitignore。
