# 基准测试结果(benchmark results)

| 项 | 值 |
|---|---|
| 测试日期 | 2026-09-19 |
| 环境 | Windows 10 x64, JDK Temurin 21.0.12.1+1-LTS, Chronicle Queue 5.27ea5 |
| 消息形态 | `{"id":n,"pad":"p...p"}`,序列化后约 120 字节 |
| JMH 配置 | Throughput 模式,2×1s 预热 + 3×2s 测量,单 fork,单线程 |

## 结果

| 基准 | 吞吐 | 说明 |
|---|---|---|
| `write` | **≈ 1.21M ops/s** | 单线程逐条 `write()`,返回 index |
| `batchWrite` | **≈ 1.03M msgs/s** | `batchWrite()`,每批 100 条(10.3k ops/s × 100) |
| `read` | **≈ 25.5k msgs/s** | 读取 + 提交管线(255 ops/s × 每批至多 100 条),自产自销单消费者形态 |

注意事项:
- `write`/`batchWrite` 的 JMH 误差区间较大(迭代间波动),但多轮结果稳定在 1M msgs/s 量级。
- `read` 为单消费者乒乓形态(排空后同步补写 2000 条),受 scan 线程追平轮询间隔(1ms)与 50ms 读超时影响,是**延迟敏感的下限值**;多消费者并发场景(见集成测试)吞吐随消费并行度扩展。
- 基准通过 `mvnw -pl latchq-benchmarks package` 构建 uber-jar 后 `java -jar latchq-benchmarks/target/latchq-benchmarks.jar` 运行;JMH fork 的 JVM 由基准 main 方法自动追加 JDK 21 所需的 `--add-opens` 参数(见 docs/spike-notes.md)。

## 复现方式

```bash
./mvnw -pl latchq-benchmarks package
java -jar latchq-benchmarks/target/latchq-benchmarks.jar            # 全部基准
java -jar latchq-benchmarks/target/latchq-benchmarks.jar write      # 单个基准
```
