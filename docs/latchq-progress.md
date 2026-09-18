# LatchQ 进度跟踪

更新日期:2026-09-18

## 当前阶段

**设计阶段** — 设计文档已完成初稿,等待用户审阅确认。

## 里程碑状态

| 里程碑 | 说明 | 状态 |
|---|---|---|
| M0 | 需求梳理与设计文档 | 进行中(文档已写,待用户审阅) |
| M1 | 核心读写(latchq-core 骨架、配置、Chronicle 读写、有界队列扇出) | 未开始 |
| M2 | 进度与 Gap(区间合并算法、gap 检测/跳过、checkpoint 持久化) | 未开始 |
| M3 | 清理与导出(旧分段清理、export API) | 未开始 |
| M4 | Spring Boot Starter(自动配置、示例项目) | 未开始 |
| M5 | 测试与基准(并发/崩溃恢复测试、JMH 基准) | 未开始 |

## 已完成的设计决策

- 参考实现:`SharpAbp.Abp.Faster`(`FasterLogger<T>` + README)。
- 底层存储:Chronicle Queue 开源版。
- 项目形态:纯 Java 库(`latchq-core`,无 DI)+ Spring Boot Starter(`latchq-spring-boot-starter`),模块划分参照 `stow` 项目。
- groupId:`io.github.cocosip`;Java 21 + Maven 3.9+ 基线。
- 并发模型:同步阻塞 API + 内部虚拟线程承载后台任务。
- 架构:单一后台 scan 线程持有唯一 Tailer,通过有界阻塞队列扇出给多个消费线程(方案 A,已用户确认)。
- 消费进度:单一消费进度(非多 consumer group),独立小文件 + 原子写(temp+rename)持久化 checkpoint。
- Gap 处理:与 FASTER 版一致,默认 2 分钟超时自动跳过,可配置/可关闭。
- 序列化:Jackson JSON,不做可插拔抽象。
- 多类型支持:一个 JVM 进程内可并存多个不同泛型类型、不同名称的队列(`LatchQueueFactory.getOrCreate(name, Class<T>)`)。
- 导出能力:仅提供 Java 方法(`export(...)`),不内置 HTTP API,由业务方自行包一层。
- 详细设计见 [latchq-design.md](./latchq-design.md)。

## 待办事项(Next Steps)

1. 用户审阅 `latchq-design.md`,确认或提出修改意见。
2. 确认后,基于设计文档编写详细实现计划(implementation plan),细化到 M1~M5 每个里程碑的具体任务。
3. 实现阶段开始后,回填设计文档第 10 节中标记为"待核实"的 Chronicle Queue 具体 API 细节。
4. 搭建 Maven 多模块工程骨架(parent pom + `latchq-core` + `latchq-spring-boot-starter` + `samples/*` + `benchmarks`)。

## 风险与未决问题

- Chronicle Queue 部分 API 细节(见设计文档第 10 节)尚未在真实依赖版本上核实,需要在实现第一步(引入依赖后)尽快验证,避免影响后续架构假设。
- JDK 21 下 Chronicle Queue 所需的 `--add-opens`/`--add-exports` 参数列表待实测确认。
