# 多租户配额 + 加权公平调度器

纯 JDK 8 标准库实现，无 Maven/Gradle/JUnit。源码在 `src/`，测试在 `test/`。

## 运行测试

```bash
./run-tests.sh
```

只需要 JDK 8+（`javac` / `java`）。脚本编译 `src/` 与 `test/` 并运行
`com.gsb.quota.TestMain`，全部通过时退出码为 0。

## 对外 API（`com.gsb.quota.Scheduler`）

```java
Scheduler scheduler = new Scheduler(clock);          // 或无参：真实时钟
scheduler.register("tenant-a", 100, 1);              // 100 tokens/s, 权重 1
scheduler.start();
String taskId = scheduler.submit("tenant-a", task);  // 超配额则排队，不丢弃
scheduler.shutdown();
```

## 设计

- **加权公平（WFQ）**：任务入队时打虚拟完成标签
  `tag = max(租户上次 tag, 系统虚拟时间) + 1/weight`；调度线程每次在“有令牌”
  的租户中选出队首 tag 最小者放行。长期完成量收敛到权重比，且任何有积压
  且有令牌的租户不会被跳过（无饥饿）。
- **配额（令牌桶）**：每租户独立令牌桶，速率与容量都等于 `tokensPerSecond`，
  初始为空、按注入时钟惰性补充。令牌不足的任务留在该租户的等待队列里，
  绝不丢弃；任何一秒窗口的放行量不超过配额。
- **可注入时钟**：所有时间读取都经过 `com.gsb.quota.Clock`
  （`millis()` + `await(deadline)`）。实现里不使用
  `System.currentTimeMillis` / `Thread.sleep`；真实时钟适配器
  `SystemClock` 基于 `System.nanoTime()` + `LockSupport.parkNanos`。
  测试用 `ManualClock` 手动推进虚拟时间，全程无需真实等待。
- **提交不串行化**：`submit` 只做 `ConcurrentHashMap` 查找 + 租户级小锁
  打标签 + 无锁队列入队，然后通过中断唤醒调度线程；不存在一把全局锁
  把所有租户的提交串起来。

## 测试用例（`test/com/gsb/quota/`）

- `FairnessTest`：权重 1:2:3 三租户各积压 3000 个任务，虚拟时间推进约
  9 秒；断言每 600 个连续完成的窗口内完成量比例在目标的 15% 以内，
  且任何租户相邻两次完成的虚拟时间间隔不超过 1 秒（无饥饿）。
- `QuotaWindowTest`：配额 5/s、持续积压，逐秒推进虚拟时间，断言每个
  按秒窗口放行量 ≤ 5，超配额部分留在队列中并最终全部完成（不丢弃）。
- `TokenBucketBoundaryTest`：桶满时恰好放行等于配额的个数、一个不多；
  第 N+1 个任务排队等待；长时间空闲后桶也不会累积超过容量。
- `SchedulerBasicTest`：启动前排队、参数校验、计数器、shutdown 语义。
