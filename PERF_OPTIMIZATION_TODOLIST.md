# BifroMQ 深度性能优化 TodoList

> 分支: `feature/perf-optimization`  
> 基线: `feature/mqtt-over-quic`  
> 创建时间: 2026-04-23

---

## P0 — 高投入产出比，建议优先实施

### [P0-1] GroupCommitWriteQueue：真 WriteBatch 合并（减少 JNI 调用次数）

**目标文件**: `base-kv/base-kv-local-engine-rocksdb/src/main/java/org/apache/bifromq/basekv/localengine/rocksdb/GroupCommitWriteQueue.java`

**问题**: 当前 leader 对每个 follower 的 batch 单独调用 `db.write()`，虽然 RocksDB 内部会合并 WAL sync，但 JNI 边界穿越开销、WriteBatch 校验开销、锁获取释放开销均未避免。

**实现方案**:
1. 当 `toWrite.size() > 1` 时，创建一个新的 `WriteBatch merged = new WriteBatch()`
2. 遍历 `toWrite`，对每个 `pw.batch` 调用 `merged.append(pw.batch)` 合并
3. 只调用一次 `db.write(writeOptions, merged)`
4. `merged` 用 try-with-resources 确保关闭

**预期收益**: 并发写高峰下减少 50%-80% JNI 调用次数  
**改动量**: 小 (~10 行)  
**风险**: 低，合并行为不改变语义

---

### [P0-2] RocksDB Blob 文件阈值：提升 MIN_BLOB_SIZE 避免小 value 读放大

**目标文件**: `base-kv/base-kv-local-engine-rocksdb/src/main/java/org/apache/bifromq/basekv/localengine/rocksdb/RocksDBDefaultConfigs.java:73`

**问题**: 当前 `MIN_BLOB_SIZE = 2KB`，BifroMQ KV engine 存储的是元数据（topic filter、session state、路由信息），绝大多数 value 只有几十到几百字节。几乎所有 value 都走 blob 文件路径，引入额外 IO 跳转（LSM 索引 → blob 文件），读放大严重。

**实现方案**:
1. 将 `MIN_BLOB_SIZE` 从 `2 * SizeUnit.KB` 提升到 `32 * SizeUnit.KB`（或更高）
2. 对 CPable（checkpoint）空间可单独禁用 blob：在 CP config 中覆盖为 `Long.MAX_VALUE`
3. 添加配置项暴露到 `standalone.yml`，让用户可按业务场景调整

**预期收益**: 小 value 读延迟降低 20%-40%  
**改动量**: 极小（1-3 行）  
**风险**: 极低，RocksDB blob 是可选优化路径

---

## P1 — 中等投入产出比，建议第二梯队

### [P1-1] GroupCommitWriteQueue：锁内分配外移 + 降低锁竞争

**目标文件**: `base-kv/base-kv-local-engine-rocksdb/src/main/java/org/apache/bifromq/basekv/localengine/rocksdb/GroupCommitWriteQueue.java:72-87`

**问题**:
1. `new ArrayList<>(pendingWrites)` 在 `ReentrantLock` 内分配，list 越大锁持有时间越长
2. `ReentrantLock` 在高并发下有内核态上下文切换

**实现方案（可选路径）**:
- **方案 A（轻量）**: 预计算 `int size = pendingWrites.size()`，lock 内只做 `toWrite = new ArrayList<>(size)`，但分配在 lock 外先做
- **方案 B（激进）**: 用 `ConcurrentLinkedQueue` 或 JCTools `MpscArrayQueue` 替换 `ReentrantLock + ArrayList`，leader 用 `drainTo(Collection)` 一次性取出所有 pending writes

**预期收益**: 高并发写入场景锁竞争显著降低  
**改动量**: 方案 A 小，方案 B 中  
**风险**: 方案 A 极低；方案 B 中，需验证 JCTools 引入的兼容性

---

### [P1-2] MQTT Handler：auth cache key 去字符串拼接，降低 GC 压力

**目标文件**: `bifromq-mqtt/bifromq-mqtt-server/src/main/java/org/apache/bifromq/mqtt/handler/MQTTTransientSessionHandler.java`（publish 方法内）

**问题**: 每次 publish 消息，对每个匹配的 topic filter 拼接字符串 `topicFilter + "\0" + qos` 作为 Caffeine cache key。fanout 大的场景（一个消息匹配几百个订阅）会产生大量短生命周期字符串。

**实现方案**:
1. 定义一个轻量 `CompositeKey` 类，包含 `String topicFilter` 和 `int qos`
2. 重写 `equals()` 和 `hashCode()`
3. 用 `CompositeKey` 替代字符串拼接作为 Caffeine cache key
4. 或使用 `Objects.hash(topicFilter, qos)` 的 `Integer` 结果（有碰撞风险但概率极低）

**预期收益**: fanout 大时 GC 压力显著降低  
**改动量**: 小  
**风险**: 低，需确保 hashCode 正确性

---

### [P1-3] ByteString.toByteArray() 二次内存拷贝优化

**目标文件**: `base-kv/base-kv-local-engine-rocksdb/src/main/java/org/apache/bifromq/basekv/localengine/rocksdb/RocksDBKVSpaceWriterHelper.java:76,86,91,96`

**问题**: Protobuf `ByteString.toByteArray()` 会做防御性拷贝。如果内部已经是 `byte[]`，这是不必要的二次分配。

**实现方案**:
- **方案 A**: 如果 rocksdbjni 版本支持，改用 `WriteBatch.put(ByteBuffer, ByteBuffer)` 版本，配合 `ByteString.asReadOnlyByteBuffer()` 零拷贝写入
- **方案 B**: 维护一个 thread-local `byte[]` buffer pool（针对小 key，< 256B），避免每次 `new byte[]`

**预期收益**: 减少小 value 写入时的内存分配  
**改动量**: 方案 A 小（需确认 rocksdbjni API），方案 B 中  
**风险**: 低

---

## P2 — 长期收益，建议第三梯队

### [P2-1] gRPC BatchDeliveryCall：orTimeout 改批量超时轮

**目标文件**: `bifromq-deliverer/src/main/java/org/apache/bifromq/deliverer/BatchDeliveryCall.java:231`

**问题**: `CompletableFuture.orTimeout()` 每个 batch 创建一个 `ScheduledTask`，高吞吐下（每秒几千 batch）调度器线程成为瓶颈。

**实现方案**:
1. 引入一个全局的 `HashedWheelTimer`（Netty 提供）或自定义批量超时轮
2. `BatchDeliveryCall` 不再调用 `orTimeout()`，而是向超时轮注册一个 `TimeoutTask`
3. 超时轮以固定周期（如 10ms）扫描一次，批量触发超时

**预期收益**: 调度器负载降低，高吞吐下延迟更稳定  
**改动量**: 中  
**风险**: 中，需确保超时精度不劣化

---

### [P2-2] RocksDB：启用 Two Write Queues + walBytesPerSync 调优

**目标文件**: `base-kv/base-kv-local-engine-rocksdb/src/main/java/org/apache/bifromq/basekv/localengine/rocksdb/RocksDBOptionsUtil.java:81-82`

**问题**: 当前 `setTwoWriteQueues(false)`，WAL writer 和 memtable writer 共用一个队列，互斥锁竞争大。

**实现方案**:
1. 将 `setTwoWriteQueues(false)` 改为 `setTwoWriteQueues(true)`
2. 添加 `opts.setWalBytesPerSync(1024 * 1024)`（1MB），配合现有的 `fsync=false` 进一步减少刷盘频率
3. 暴露 `twoWriteQueues` 和 `walBytesPerSync` 到配置层

**预期收益**: 高并发写入吞吐提升 15%-30%  
**改动量**: 极小（2-3 行）  
**风险**: 低，Two Write Queues 是 RocksDB 6.20+ 成熟特性

---

### [P2-3] Native Rust Binding：压缩函数零拷贝改造

**目标文件**: `bifromq-native/src/compressor/ffi.rs:40-54`

**问题**: Rust 侧先把压缩结果写入临时 `Vec`，再拷贝到 JNI 输出 buffer，存在二次分配 + 二次拷贝。

**实现方案**:
1. Java 侧预先分配一个 direct `ByteBuffer`，传入 Rust
2. Rust 侧直接 `write()` 到该 buffer，无需中间 `Vec`
3. 返回实际写入的字节数

**预期收益**: 大 payload 压缩场景减少一次 memcpy  
**改动量**: 小（JNI 签名 + Rust ffi 各改几行）  
**风险**: 低，需确保 buffer 容量足够（可返回错误码让 Java 重试）

---

## P3 — 架构级优化，建议最后实施

### [P3-1] MovingAverage：synchronized 改原子环形缓冲区

**目标文件**: `base-scheduler/src/main/java/org/apache/bifromq/basescheduler/MovingAverage.java:42`

**问题**: `public synchronized void observe(long value)` 是 scheduler 内部统计 batch latency 的热路径，大量线程并发时会串行化。

**实现方案**:
1. 用 `LongAdder` 累加 count + sum，定期（每 N 次）计算一次 EMA
2. 或实现一个无锁环形缓冲区（`AtomicLongArray`），observers 用 CAS 写入
3. 或直接用 Guava `ExponentialMovingAverages`（如果 Guava 版本支持）

**预期收益**: 高并发场景下 scheduler 统计不再成为瓶颈  
**改动量**: 小  
**风险**: 低，需保持 EMA 计算结果与原实现一致

---

### [P3-2] ManagedBiDiStream / ManagedRequestPipeline：粗粒度 synchronized 改 StampedLock

**目标文件**:
- `base-rpc/base-rpc-client/src/main/java/.../ManagedBiDiStream.java:158,184,217,240,285`
- `base-rpc/base-rpc-client/src/main/java/.../ManagedRequestPipeline.java:78,86,93,181,196,214`

**问题**: send / close / error 都用 `synchronized (this)`，RPC 高并发下串行化严重。

**实现方案**:
1. send 路径改用 `StampedLock` 乐观读（`tryOptimisticRead`）
2. close/error 用写锁
3. state 变更用 `AtomicReference` CAS

**预期收益**: RPC 高并发下 send 延迟降低  
**改动量**: 中（涉及状态机重构）  
**风险**: 中，StampedLock 需正确处理乐观读失败后的降级

---

## 附录：JMH Benchmark 补充建议

当前已有的 benchmark：
- `HybridWorkload` (RocksDB)
- `MovingAverageBenchmark`
- `TenantMeterBenchmark`
- `TopicTrieBuilderBenchmark`

**建议补充**:
1. `GroupCommitWriteQueueBenchmark` — 量化合并前后 JNI 调用次数和吞吐
2. `BatchDeliveryCallBenchmark` — 量化 orTimeout vs 批量超时轮的调度器负载
3. `MQTTSessionHandlerBenchmark` — 量化 fanout 大时的 GC 压力和 latency

---

## 执行建议

1. **先跑基准测试**：对 P0 和 P1 的优化点，先写 JMH benchmark 建立基线
2. **逐个实施**：每个优化点单独 commit，方便回滚和 bisect
3. **CI 验证**：每次改动后跑 `./mvnw test -Pbuild-coverage`，确保功能正确
4. **性能回归**：在 k8s 3-pod 环境下用 `regression_test.py` 扩展负载测试
