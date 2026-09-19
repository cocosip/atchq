# LatchQ 示例

## latchq-sample-console(纯 Java 手动装配)

演示 `LatchQueueBuilder` 手动装配:1 个生产者写 100 条订单,3 个消费者线程并发处理并乱序提交。

运行(JDK 21 需要携带 Chronicle 所需的 JVM opens 参数,`MAVEN_OPTS` 方式):

```bash
# Windows (Git Bash / PowerShell 需相应调整 env 语法)
export MAVEN_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens java.base/java.io=ALL-UNNAMED \
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
  --add-exports java.base/jdk.internal.ref=ALL-UNNAMED"

./mvnw -pl samples/latchq-sample-console org.codehaus.mojo:exec-maven-plugin:3.5.0:java \
  -Dexec.mainClass=io.github.cocosip.sample.console.ConsoleSample
```

预期输出:100 条 `processed Order[...]`,最后打印 `final metrics`(totalWriteCount=100、totalReadCount=100、truncateBeforeIndex 推进到队尾)。

核心代码见 [`ConsoleSample.java`](./src/main/java/io/github/cocosip/sample/console/ConsoleSample.java):
- `LatchQueueBuilder.create(name, type).rootPath(...).configuration(...).build()` 手动装配;
- 消费循环 `read(count, timeout)` → 业务处理 → `commit(positions)`;
- 结束调用 `queue.close()` 做最终 checkpoint。

## latchq-sample-spring-boot(YAML 配置 + Controller 暴露 export)

演示 starter 的标准用法:
- `application.yml` 中 `latchq.root-path` + `latchq.configurations.<name>.*` 直接绑定;
- 注入 `LatchQueueFactory`,`getOrCreate("audit", AuditEvent.class)` 自动初始化并缓存;
- 应用关闭时 starter 注册的 destroy 钩子关闭工厂 → 每个队列做最终 checkpoint;
- `AuditEventController` 演示"业务方在自己的 Controller 层包一层 export API"的推荐模式(LatchQ 本身不内置 HTTP):
  - `POST /audit/events {"id":1,"action":"login"}` 写入
  - `GET /audit/metrics` 查看指标
  - `POST /audit/export` 导出 JSONL 文件(返回文件路径列表)

运行:

```bash
export MAVEN_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens java.base/java.io=ALL-UNNAMED \
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
  --add-exports java.base/jdk.internal.ref=ALL-UNNAMED"

./mvnw -pl samples/latchq-sample-spring-boot spring-boot:run
# 端口 18080
```
