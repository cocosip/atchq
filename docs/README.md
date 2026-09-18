# LatchQ 文档索引

LatchQ 是一个基于 [OpenHFT Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue) 的 Java 持久化日志/队列库,
功能上对标 [SharpAbp.Abp.Faster](https://github.com/sharpabp/sharp-abp)(基于 Microsoft FASTER 的 .NET 实现):
支持多线程并发写入、多线程并发批量读取、乱序提交与延迟确认、真实持久化(非内存队列)。

## 文档列表

| 文档 | 说明 |
|---|---|
| [latchq-design.md](./latchq-design.md) | 设计文档:问题背景、架构、模块划分、核心 API、gap 处理算法、配置模型、技术基线 |
| [latchq-progress.md](./latchq-progress.md) | 开发计划与进度跟踪:版本基线、里程碑任务分解(含 DoD)、关键决策记录、风险登记 |

## 阅读顺序

1. 先读 `latchq-design.md` 了解整体设计与架构决策。
2. 再看 `latchq-progress.md` 了解版本基线、里程碑任务分解和当前进度。
3. 按里程碑顺序推进,M0(spike 技术验证)的结论回填设计文档第 10 节后冻结设计。
