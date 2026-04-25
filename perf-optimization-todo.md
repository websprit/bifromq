# BifroMQ 性能优化 TODO List

优先级从高（P0）到低（P3），标记 `[x]` 表示已完成。

---

## P0 — 核心瓶颈修复（改动极小，收益最高）

- [x] **P0-1: 启用 RocksDB TwoWriteQueues**
  - 文件: `base-kv/base-kv-local-engine-rocksdb/.../RocksDBOptionsUtil.java:82`
  - 改动: `setTwoWriteQueues(false)` → `setTwoWriteQueues(true)`
  - 收益: 写吞吐 +20-30%，memtable 写入和 WAL 写入可并行
  - 风险: 低，RocksDB 原生支持

- [x] **P0-2: 启用 RocksDB MaxSubcompactions**
  - 文件: `base-kv/.../RocksDBOptionsUtil.java` — `buildDBOptions()` 方法
  - 改动: 添加 `setMaxSubcompactions(Math.max(1, availableProcessors / 4))`
  - 收益: Compaction 并行度提升，大范围 tombstone 清理加速 2-4x
  - 风险: 低，CPU 资源允许时总是有利

- [x] **P0-3: AdaptiveCapacityEstimator.maxCapacity 设上限**
  - 文件: `base-scheduler/.../CapacityEstimatorFactory.java:98`
  - 改动: `return Long.MAX_VALUE` → `return 10000L`（或其他合理上限）
  - 收益: 防止单次 batch 过大导致尾部延迟飙升
  - 风险: 低，只是加一个安全边界

## P1 — 配置调优（一行改动或配置暴露）

- [x] **P1-1: 启用 RocksDB Dynamic Level Compaction**
  - 文件: `RocksDBOptionsUtil.java:227`
  - 改动: `setLevelCompactionDynamicLevelBytes(false)` → `true`
  - 收益: 根据实际数据量自动调整 L1~LMax 目标大小，减少空间放大

- [x] **P1-2: 增大 Block Cache 并支持配置**
  - 文件: `RocksDBDefaultConfigs.java:69`
  - 改动: `BLOCK_CACHE_SIZE = 512 * SizeUnit.MB` → 按比例或可配置
  - 建议: 在 `StandaloneConfig` 中暴露 `rocksdbBlockCacheSize` 参数，默认 1GB
  - 收益: 提高读命中率，减少磁盘 I/O

- [x] **P1-3: 增大 RPC EventLoop 线程数**
  - 文件: `build/.../RPCConfig.java:33-34`
  - 改动: `Math.max(4, availableProcessors / 8)` → `Math.max(4, availableProcessors / 4)`
  - 收益: 减少高并发下的 I/O 线程饥饿

- [x] **P1-4: 增大 InboxServer workerThreads**
  - 文件: `build/.../inbox/InboxServerConfig.java`
  - 改动: `Math.max(2, availableProcessors / 4)` → `Math.max(4, availableProcessors / 2)`
  - 收益: 提升 Inbox gRPC 请求处理并行度

- [x] **P1-5: RocksDB 后台线程数上调**
  - 文件: `RocksDBDefaultConfigs.java:75-77`
  - 改动: `max(availableProcessors / 4, 2)` → `max(availableProcessors / 2, 4)`
  - 收益: 加快 flush 和 compaction，减少写停顿

- [x] **P1-6: Rate Limiter 改为可配置**
  - 文件: `RocksDBOptionsUtil.java:84`
  - 改动: 硬编码 `512 * SizeUnit.MB` → 使用配置参数
  - 收益: 适配不同存储设备（NVMe 可以更高，HDD 可以更低）

- [x] **P1-7: Bloom Filter 位宽提升到 20**
  - 文件: `RocksDBOptionsUtil.java:149`
  - 改动: `new BloomFilter(16, false)` → `new BloomFilter(20, false)`
  - 收益: 降低假阳性率 1-2% → ~0.4%，提升点查询性能

- [x] **P1-8: Batcher 指标瘦身**
  - 文件: `Batcher.java:97-132`
  - 改动: 使用 Micrometer `MeterFilter` 聚合同类指标，或移除低价值 Gauge
  - 收益: 减少大量 Batcher 实例的指标注册和更新开销

## P2 — 架构改进（中等改动量）

- [x] **P2-1: BatchQueryCall 子批次并发发射**
  - 文件: `base-kv-client/.../BatchQueryCall.java:84-92`
  - 改动: `thenCompose` 串行链 → `CompletableFuture.allOf()` 并发
  - 条件: 仅对 `isLinearizable=false` 的查询生效
  - 收益: 多版本子批次不再串行等待，查询吞吐提升

- [x] **P2-2: InboxWriter 重试加指数退避 + 抖动**
  - 文件: `bifromq-inbox/.../InboxWriter.java:106-114`
  - 改动: 固定间隔 1000ms → 指数退避 + 随机抖动
  - 收益: 避免 backpressure 恢复时的雷鸣群效应

- [x] **P2-3: KV 元数据缓存到内存**
  - 文件: `base-kv-store-server/.../KVRange.java`
  - 改动: 在 `KVRange` 对象中用 `volatile` 字段缓存 metadata，写入时更新
  - 收益: 每次查询减少 5+ 次 RocksDB 元数据读取

- [x] **P2-4: RPC 客户端 Executor 使用有界队列**
  - 文件: `base-rpc/.../ClientChannel.java`
  - 改动: `LinkedTransferQueue` → 有界队列 + `CallerRunsPolicy`
  - 收益: 防止背压时内存无限增长导致 OOM

- [x] **P2-5: QUIC DataStream 哈希均匀化**
  - 文件: `bifromq-mqtt/.../QUICStreamRouter.java`
  - 改动: `topic.hashCode() % 16` → `MurmurHash3_x86_32(topic) % numStreams`
  - 收益: 避免哈希倾斜导致部分流过载

- [x] **P2-6: Raft 日志条目双序列化消除**
  - 文件: `base-kv-store-server/.../MutatePipeline.java`
  - 改动: RWCoProcInput 序列化到 gRPC 请求后，复用 `byte[]` 直接写入 Raft 日志
  - 收益: 减少大规模 coprocessor input 的重复序列化 CPU 开销

- [x] **P2-7: 添加读路径热点缓存**
  - 文件: `base-kv-store-server/.../KVRangeReader.java`
  - 改动: 使用 Caffeine 做 `(key -> value)` 读缓存，写入时失效
  - 条件: 适用于频繁 exist/get 的热点 key

- [x] **P2-8: 设置合理空闲超时**
  - 文件: `base-rpc/.../ClientChannel.java:111`
  - 改动: 默认一年 → 300 秒 + keepalive
  - 收益: 释放不再使用的 HTTP/2 连接资源

## P3 — 深度优化（改动量大或需仔细验证）

- [ ] **P3-1: Batcher TimeoutWheel 替换为 HashedWheelTimer**
  - 文件: `Batcher.java:331-353`
  - 改动: 单线程每 5ms O(N) 扫描 → Netty HashedWheelTimer O(1)
  - 收益: 大量 Batcher 时超时检查不再是瓶颈

- [ ] **P3-2: CallTask + CompletableFuture 对象池化**
  - 文件: `Batcher.java:148`
  - 改动: 每次 `submit()` 新建对象 → 环形缓冲区/对象池复用
  - 收益: 减少高吞吐下的 GC 压力

- [ ] **P3-3: trigger() 递归改循环**
  - 文件: `Batcher.java:194-207`
  - 改动: `finally` 中的递归调用 → `while` 循环
  - 收益: 消除极端情况下的 StackOverflow 风险

- [ ] **P3-4: GroupCommitWriteQueue follower 协助合并**
  - 文件: `GroupCommitWriteQueue.java:71-137`
  - 改动: follower 线程在等待 leader 的同时帮收集新到达的 WriteBatch
  - 收益: 极高写入并发下的吞吐提升

- [ ] **P3-5: Coprocessor post-mutation Supplier 异步化**
  - 文件: `base-kv-store-server/.../KVRangeFSM.java`
  - 改动: `Supplier<MutationResult>` → `CompletableFuture<MutationResult>`
  - 收益: 避免 cache refresh 等重操作阻塞单线程 fsmExecutor

- [ ] **P3-6: InboxServer 线程池隔离**
  - 文件: `bifromq-inbox/.../InboxService.java`
  - 改动: 不同操作（exist/attach/insert）使用独立的线程池
  - 收益: 防止慢操作（如 insert）阻塞快操作（exist）

- [ ] **P3-7: Raft 心跳中消除不必要的 entryAt 调用**
  - 文件: `base-kv-raft/.../RaftNodeStateLeader.java`
  - 改动: 无新 entry 的心跳跳过 `stateStorage.entryAt()` 调用
  - 收益: 减少心跳处理中的 WAL 读取

---

## 标注说明

- `[ ]` = 待执行
- `[x]` = 已完成
- 优先按 P0 → P1 → P2 → P3 顺序执行
- 每个项目完成后，将 `[ ]` 改为 `[x]` 并提交对应代码
