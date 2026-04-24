# BifroMQ 深度性能优化分析报告

> 分析范围：全代码库（base-*, bifromq-*, 第三方依赖）
> 当前分支：feature/perf-optimization
> 已实施优化：Batch Timeout Wheel (P2-1)、StampedLock (P3-1/P3-2)、Group Commit (P0-1)、RocksDB blob threshold

---

## 执行摘要

通过静态代码扫描和架构分析，识别出 **50+ 个** 可优化点，按影响力和实施复杂度分为四个优先级：

| 优先级 | 数量 | 特征 | 典型收益 |
|--------|------|------|----------|
| **P0** | 4 | 高频分配风暴、数据结构选择错误 | 10-30% 吞吐量提升 |
| **P1** | 6 | 内存拷贝、不必要的 boxing、锁竞争 | 5-15% 延迟降低 |
| **P2** | 5 | 算法复杂度、可重用的 builder/iterator | 3-10% GC 压力降低 |
| **P3+** | 20+ | JVM 调参、微优化、启动时间 | 1-5% 综合提升 |

---

## P0 优先级：立即实施（高性能收益，中低复杂度）

### P0-1 DistWorkerCoProc.batchDist：分配风暴
- **位置**：`bifromq-dist-worker/src/main/java/.../DistWorkerCoProc.java:511-548`
- **问题**：每条 topic 创建 `ConcurrentHashMap` + `AtomicInteger` + lambda closure + `CompletableFuture` chain
- **方案**：
  1. 用 thread-local 的 `Long2IntOpenHashMap` 替换 `AtomicInteger` 计数
  2. 批量提交 `deliverExecutorGroup.submit()` 而非逐条
  3. 用 primitive array 聚合 fanout，最后统一更新
- **影响**：高（消息分发是核心路径）

### P0-2 BatchDeliveryCall：每批次全量集合重建
- **位置**：`bifromq-deliverer/src/.../BatchDeliveryCall.java:58-77`
- **问题**：`reset(true)` 每次分配 `HashMap<LinkedHashMap<HashSet>>`，嵌套 lambda 闭包
- **方案**：
  1. 使用扁平化的 `Object2ObjectOpenHashMap`（fastutil）
  2. abort 路径改为 `clear()` 而非重建
  3. 用 `ArrayList` + 线性扫描替代 `HashSet`（MatchInfo 数量通常 < 10）
- **影响**：高（delivery 批次每秒可达数万）

### P0-3 InboxStoreCoPro.fetchFromInbox：Optional 装箱
- **位置**：`bifromq-inbox-store/src/.../InboxStoreCoProc.java:398-451`
- **问题**：循环中 `reader.get()` 返回 `Optional<ByteString>`，每次分配 Optional 对象
- **方案**：API 改为返回 `ByteString` + null sentinel，或复用 thread-local Optional
- **影响**：高（inbox fetch 是消息拉取热点）

### P0-4 RocksDBKVSpaceWriterHelper：singleDelete+put 双倍写放大
- **位置**：`base-kv-local-engine-rocksdb/src/.../RocksDBKVSpaceWriterHelper.java:74-92`
- **问题**：每次 put/metadata 都先 `singleDelete` 再 `put`，WriteBatch 膨胀一倍
- **方案**：
  1. insert-only 场景（已知 key 不存在）跳过 singleDelete
  2. 用脏 key 追踪，仅在确认旧值存在时才发 tombstone
  3. 利用 Group Commit 的 batch append 合并相邻 tombstone+put 为 overwrite
- **影响**：高（直接影响 RocksDB compaction 和 WAL 放大）

---

## P1 优先级：短期实施（显著收益，中等复杂度）

### P1-1 NativeKVBatchEncoder：每批分配 byte[] + ByteBuffer
- **位置**：`bifromq-native-binding/src/.../NativeKVBatchEncoder.java:109-220`
- **问题**：FlatBuffer 编解码每次新建 `byte[inputSize]` 和 `ByteBuffer.wrap()`
- **方案**：`NativeArenaPool` 已提供 `MemorySegment`，直接写入 segment，跳过中间 byte[]
- **影响**：高（KV 编码每次 batch 触发）

### P1-2 NativeTopicTrie：UTF-8 byte[] 每层级分配
- **位置**：`bifromq-native-binding/src/.../NativeTopicTrie.java:216`
- **问题**：`String.getBytes(UTF_8)` 为每个 topic level 分配 byte[]
- **方案**：
  1. 常见 topic level 缓存 UTF-8 byte[]（LRU）
  2. 或用 pooled `byte[]` + `String.getBytes(CharsetEncoder)` 写入
  3. 最佳方案：Rust 侧直接读取 Java String 的 UTF-16 char[] 内部转换
- **影响**：高（topic match 是订阅路由核心路径）

### P1-3 MQTT5MessageSizer：每条消息分配临时 record
- **位置**：`bifromq-mqtt-server/src/.../MQTT5MessageSizer.java`
- **问题**：`sizeOf()` 分配 `MqttMessageSize`、`MqttVarHeaderBytes`、`MqttPropertiesBytes`
- **方案**：添加 `sizeOf(MqttMessage, MutableSizeResult)` 重载，使用 thread-local 结果对象
- **影响**：高（每条 MQTT 消息都经过 size 计算）

### P1-4 BatchTimeoutWheel：O(N) 全局扫描
- **位置**：`base-scheduler/src/.../Batcher.java:328-348`
- **问题**：每 5ms 遍历所有 Batcher 实例，每个 Batcher 再遍历 `ConcurrentLinkedQueue`
- **方案**：
  1. 层级时间轮（Hierarchical Timing Wheel）
  2. 或用 `LongAdder` 记录 pending 数量，为零时跳过扫描
  3. 只注册有 pending timeout 的 batcher 到 wheel
- **影响**：高（batch 数量随租户线性增长）

### P1-5 InboxFetchPipeline：synchronized send 串行化
- **位置**：`bifromq-inbox-server/src/.../InboxFetchPipeline.java:124`
- **问题**：`synchronized` 在 `send()` 上串行所有 inbox fetch 的 gRPC 流写入
- **方案**：`ConcurrentLinkedQueue` + flush coalescing，或确认 gRPC StreamObserver 是否已要求单线程（若是则无需 synchronized）
- **影响**：高（同一 inbox 高并发时阻塞）

### P1-6 RocksDB WriteBatch byte[] 拷贝
- **位置**：`base-kv-local-engine-rocksdb/src/.../RocksDBKVSpaceWriterHelper.java:77,86,91`
- **问题**：`ByteString.toByteArray()` 防御性拷贝
- **方案**：使用 `WriteBatch.put(ByteBuffer, ByteBuffer)` 重载（RocksDB JNI 较新版本支持），或 `UnsafeByteOperations` 零拷贝 wrap
- **影响**：中-高（大 value 时翻倍内存占用）

---

## P2 优先级：中期实施（GC/吞吐量优化）

### P2-1 TopicUtil：每 topic 解析都新建 ArrayList + StringBuilder
- **位置**：`bifromq-util/src/.../TopicUtil.java:199-225`
- **方案**：ThreadLocal 复用 `ArrayList<String>` 和 `StringBuilder`
- **影响**：高（topic 解析是 PUBLISH/SUBSCRIBE 必经之路）

### P2-2 TopicUtil：String.replace 正则开销
- **位置**：`bifromq-util/src/.../TopicUtil.java:192-195`
- **方案**：用 char[] 原地转换替代 `String.replace`
- **影响**：中

### P2-3 FastBehaviorSubject：ReentrantReadWriteLock → StampedLock
- **位置**：`base-rpc-client/src/.../FastBehaviorSubject.java:51`
- **方案**：与 P3-2 保持一致，升级为 StampedLock
- **影响**：中

### P2-4 Protobuf builder 重用
- **位置**：多处（DistWorkerCoProc、InboxStoreCoProc 等）
- **方案**：`ThreadLocal<Builder>` + `clear()` 替代 `newBuilder()`
- **影响**：中-高（减少内部数组分配）

### P2-5 ResponsePipeline：递归 lambda 调度
- **位置**：`base-rpc-server/src/.../ResponsePipeline.java:89-105`
- **方案**：单一 periodic task 替代递归 `schedule()`
- **影响**：中

---

## P3+ 优先级：其他优化机会

### 数据结构优化
- `MQTTPersistentSessionHandler` 的 `TreeMap` stagingBuffer → `Long2ObjectOpenHashMap` 或 ring buffer
- `MQTTSessionHandler` 的 `HashSet<Integer>` inUsePacketIds → `IntOpenHashSet` (fastutil)
- `DistWorkerCoProc` 的 `TreeMap`/`HashSet` 嵌套 → fastutil 专用集合

### RocksDB 层
- `RocksDBKVSpaceIterator`：`ReadOptions`/`Slice` 对象池（减少 JNI 分配）
- `RocksDBKVSpace`：compaction 的 `synchronized` → `StampedLock`
- `GroupCommitWriteQueue`：`ReentrantLock` → lock-free MPSC queue (JCTools)

### JVM 层
- **Compact Object Headers**：`-XX:+UseCompactObjectHeaders`（JDK 21+），对小对象密集的 broker 场景收益大
- **ZGC/Shenandoah**：低延迟 GC，适合亚毫秒暂停要求
- **String Deduplication**：`-XX:+UseStringDeduplication`，tenant ID 和 topic 字符串重复度高
- **Large Pages**：`-XX:+UseLargePages`，RocksDB + Netty direct memory 受益

### Metrics/观测
- `EventCollectorManager`：每事件遍历 HashMap + Timer.Sample 分配
- `TenantMeterCache`：`WeakHashMap` + `ConcurrentHashMap` 双层查找

---

## 推荐实施路线图

| 阶段 | 时间 | 优化项 | 预期收益 |
|------|------|--------|----------|
| **阶段 1**（1-2 周） | P0-1, P0-2, P0-3, P0-4 | 消除高频分配风暴 | 20-40% 吞吐量提升 |
| **阶段 2**（2-3 周） | P1-1, P1-2, P1-3, P1-4, P1-5 | Native/JNI 零拷贝 + 锁优化 | 15-25% 延迟降低 |
| **阶段 3**（1-2 周） | P2-1, P2-3, P2-4 | GC 压力缓解 | 10-20% GC 暂停降低 |
| **阶段 4**（1 周） | JVM flags（COH, ZGC, LargePages） | 运行时调优 | 5-15% 综合提升 |
| **阶段 5**（持续） | P3+ 微优化 | 长尾优化 | 累积 5-10% |

---

## 附录：常见模式速查

| 反模式 | 位置示例 | 优化方向 |
|--------|----------|----------|
| `new ArrayList<>()` 无初始容量 | Batcher, BatchDeliveryCall | `new ArrayList<>(expectedSize)` |
| `ByteString.toByteArray()` | RocksDB writer | `ByteBuffer` 零拷贝 |
| `String.getBytes(UTF_8)` | NativeTopicTrie | 缓存 / pooled buffer |
| `Optional` in hot loop | InboxStoreCoProc | null sentinel / thread-local |
| `synchronized` on I/O | InboxFetchPipeline | 队列 + flush / StampedLock |
| `HashMap`/`HashSet` for primitives | MQTTSessionHandler | fastutil 专用集合 |
| `newBuilder()` in loop | DistWorkerCoProc | `ThreadLocal<Builder>` |
| `System.nanoTime()` per task | CallTask, UnaryCaller | batch timestamp / sampling |
