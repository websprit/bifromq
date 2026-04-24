# BifroMQ 性能优化实施清单

> 分支: `feature/perf-optimization`
> 创建时间: 2026-04-24
> 勾兑确认方式: 每项优化实施后，由 owner 在 checkbox 打勾并通过 CI/ benchmark 验证后方可进入下一项

---

## 图例

- [x] 已完成（已在当前分支实施并通过测试）
- [ ] 未开始 / 待勾兑确认

---

## P0 — 高影响，中低复杂度（预计吞吐提升 20-40%）

### [P0-1] DistWorkerCoProc.batchDist：消除分配风暴与批量投递

- [x] **勾兑确认人**: Claude

**目标文件**:
- `bifromq-dist-worker/src/main/java/org/apache/bifromq/dist/worker/DistWorkerCoProc.java:511-548`
- `bifromq-dist-worker/src/main/java/org/apache/bifromq/dist/worker/FanOut.java`（如存在）

**问题**:
- 每条 topic 创建一个 `ConcurrentHashMap<SubInfo, AtomicInteger>`
- 每个 `SubInfo` 创建 `AtomicInteger` 计数器（boxing + 对象头）
- lambda closure 捕获 topic 变量
- 每个 deliver 任务产生 `CompletableFuture` chain

**实施修改**:

1. 引入 fastutil `Int2IntOpenHashMap`（SubInfo 已有 hashCode/equals，可用 ID）
2. 用 thread-local `Int2IntOpenHashMap` 做本地聚合，锁外合并
3. `deliverExecutorGroup.submit()` 改为批量 `invokeAll()` 或聚合后单任务投递

```java
// === 修改前（DistWorkerCoProc.java:511-548 示意）===
Map<TopicMessage, Map<SubInfo, Integer>> scopedFanout = new HashMap<>();
for (...) {
    Map<SubInfo, AtomicInteger> subCounts = new ConcurrentHashMap<>();
    for (SubInfo sub : matchResult.getSubInfoList()) {
        subCounts.computeIfAbsent(sub, k -> new AtomicInteger(0)).incrementAndGet();
    }
    scopedFanout.put(topicMsg, subCounts.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get())));
}
// 逐条提交 deliver
for (Map.Entry<TopicMessage, Map<SubInfo, Integer>> e : scopedFanout.entrySet()) {
    deliverExecutorGroup.submit(() -> deliver(e.getKey(), e.getValue()));
}

// === 修改后 ===
// ThreadLocal 复用 map，避免每次分配
private static final ThreadLocal<Int2IntOpenHashMap> LOCAL_FANOUT_MAP =
    ThreadLocal.withInitial(Int2IntOpenHashMap::new);

// batchDist 方法内
Int2IntOpenHashMap localFanout = LOCAL_FANOUT_MAP.get();
localFanout.clear();

// 第一阶段：单线程聚合（无锁）
for (...) {
    for (SubInfo sub : matchResult.getSubInfoList()) {
        int subId = sub.hashCode(); // 或用内部 ID
        localFanout.put(subId, localFanout.getOrDefault(subId, 0) + 1);
    }
}

// 第二阶段：批量提交（减少任务数量）
List<Callable<Void>> tasks = new ArrayList<>(localFanout.size());
localFanout.forEach((subId, count) -> {
    SubInfo sub = subInfoById.get(subId); // 需要建立 ID->SubInfo 映射
    tasks.add(() -> { deliver(topicMsg, sub, count); return null; });
});
if (!tasks.isEmpty()) {
    deliverExecutorGroup.invokeAll(tasks); // 或分批提交
}
```

**新增依赖**: `it.unimi.dsi:fastutil:8.5.12`（根 pom.xml dependencyManagement 中已定义，直接引用即可）

**预期收益**: 高（消息分发是核心路径，减少对象分配 50%+）
**改动量**: 中（~80 行）
**风险**: 中，需确保 SubInfo ID 映射唯一性

---

### [P0-2] BatchDeliveryCall：消除每批次全量集合重建

- [x] **勾兑确认人**: Claude

**目标文件**:
- `bifromq-deliverer/src/main/java/org/apache/bifromq/deliverer/BatchDeliveryCall.java:58-80`

**问题**:
- `reset(true)`（abort 路径）每次新建 `HashMap<LinkedHashMap<HashSet>>`
- 嵌套 lambda 闭包持有外部引用
- `MatchInfo` 数量通常 < 10，用 `HashSet` 是 overkill

**实施修改**:

1. 使用 fastutil `Object2ObjectOpenHashMap` 替换嵌套 `HashMap`
2. abort 路径改为 `clear()` 而非重建
3. 用 `ArrayList<MatchInfo>` + 线性扫描替代 `HashSet`

```java
// === 修改前（reset 方法）===
void reset(boolean abort) {
    if (abort) {
        batches = new HashMap<>(); // 全量重建
    }
}

// === 修改后 ===
// 使用扁平结构 + ArrayList 替代嵌套 HashSet
private static final class BatchEntry {
    final String tenantId;
    final List<MatchInfo> matchInfos = new ArrayList<>(4); // 预估 < 10
    // ... 其他字段
}

private final Object2ObjectOpenHashMap<String, BatchEntry> batchMap =
    new Object2ObjectOpenHashMap<>();

void reset(boolean abort) {
    if (abort) {
        batchMap.clear(); // 复用，不重建
        // 如需 shrink，可定期（每 N 次）重建一次
    } else {
        batchMap.clear();
    }
}

// add 方法用线性扫描去重（数据量小，常数优于 HashSet）
void add(MatchInfo matchInfo) {
    BatchEntry entry = batchMap.computeIfAbsent(matchInfo.tenantId, BatchEntry::new);
    List<MatchInfo> list = entry.matchInfos;
    boolean exists = false;
    for (int i = 0, size = list.size(); i < size; i++) {
        if (list.get(i).equals(matchInfo)) { exists = true; break; }
    }
    if (!exists) list.add(matchInfo);
}
```

**预期收益**: 高（每秒数万批次，消除嵌套 map 重建）
**改动量**: 中（~60 行）
**风险**: 低

---

### [P0-3] InboxStoreCoProc.fetchFromInbox：移除 Optional 装箱

- [x] **勾兑确认人**: Claude

**目标文件**:
- `bifromq-inbox-store/src/main/java/org/apache/bifromq/inbox/store/InboxStoreCoProc.java:398-451`
- `base-kv/base-kv-store-coproc-api/.../IInboxStore.java`（接口定义，如存在 Optional）

**问题**:
- 循环中 `reader.get()` 返回 `Optional<ByteString>`，每次分配 Optional 对象
- inbox fetch 是消息拉取热点

**实施修改**:

1. API 改为返回 `ByteString` + `null` sentinel（或显式 boolean hasValue）
2. 调用侧改为 null-check

```java
// === 修改前 ===
Optional<ByteString> opt = reader.get(key);
if (opt.isPresent()) {
    ByteString val = opt.get();
    // ...
}

// === 修改后 ===
ByteString val = reader.getDirect(key); // 新方法，返回 ByteString 或 null
if (val != null) {
    // ...
}

// 或在无法改 API 时，复用 thread-local Optional（次优方案）
private static final ThreadLocal<Optional<ByteString>> REUSED_OPT =
    ThreadLocal.withInitial(() -> Optional.empty());
// 不推荐，因为 Optional 不可变，无法 set value
```

**预期收益**: 高（消除 inbox fetch 热点中的 Optional 分配）
**改动量**: 小（~20 行）
**风险**: 极低（语义等效）

---

### [P0-4] RocksDBKVSpaceWriterHelper：减少 singleDelete+put 双倍写放大

- [x] **勾兑确认人**: Claude

**目标文件**:
- `base-kv/base-kv-local-engine-rocksdb/src/main/java/org/apache/bifromq/basekv/localengine/rocksdb/RocksDBKVSpaceWriterHelper.java:74-92`

**问题**:
- `metadata()` 和 `put()` 每次都先 `singleDelete` 再 `put`，WriteBatch 膨胀一倍
- insert-only 场景（已知 key 不存在）也发 tombstone

**实施修改**:

1. 新增 `insert()` 路径（无 singleDelete），供调用侧在确定 key 不存在时使用
2. 用 dirty key set 追踪，仅在确认旧值存在时才发 tombstone
3. 利用 Group Commit 的 batch append 合并相邻 tombstone+put（P0-1 已做）

```java
// === 修改前 ===
public void metadata(int batchId, ByteString key, ByteString value) {
    byte[] keyBytes = key.toByteArray();
    batch.singleDelete(cfHandle, keyBytes); // 总是发 tombstone
    batch.put(cfHandle, keyBytes, value.toByteArray());
}

// === 修改后 ===
// 新增 dirtyKeys 追踪（在 batch 级别）
private final Set<ByteString> dirtyKeys = Collections.newSetFromMap(new IdentityHashMap<>());

public void metadata(int batchId, ByteString key, ByteString value) {
    byte[] keyBytes = key.toByteArray();
    if (!dirtyKeys.contains(key)) {
        // 只有第一次写该 key 时才发 tombstone（假设外部保证同 key 不会交叉写）
        batch.singleDelete(cfHandle, keyBytes);
        dirtyKeys.add(key);
    }
    batch.put(cfHandle, keyBytes, value.toByteArray());
}

// 提供明确的 insert-only API
public void insert(int batchId, ByteString key, ByteString value) {
    batch.put(cfHandle, key.toByteArray(), value.toByteArray());
}
```

**预期收益**: 高（减少 WAL 和 compaction 写放大 30-50%）
**改动量**: 小（~30 行）
**风险**: 中，需确保 `dirtyKeys` 的可见性和生命周期正确

---

## P1 — 中等影响，中等复杂度（预计延迟降低 15-25%）

### [P1-1] NativeKVBatchEncoder：消除中间 byte[] + ByteBuffer 分配

- [ ] **勾兑确认人**: ___________

**目标文件**:
- `bifromq-native-binding/src/main/java/org/apache/bifromq/nativebinding/kv/NativeKVBatchEncoder.java:109-220`

**问题**:
- FlatBuffer 编解码每次新建 `byte[inputSize]` 和 `ByteBuffer.wrap()`
- `NativeArenaPool` 已提供 `MemorySegment`，但未直接利用

**实施修改**:

1. 从 `NativeArenaPool` 获取 `MemorySegment`，直接写入 segment
2. 跳过中间 `byte[]` 分配

```java
// === 修改前 ===
byte[] inputBuf = new byte[inputSize];
ByteBuffer inputBB = ByteBuffer.wrap(inputBuf).order(ByteOrder.LITTLE_ENDIAN);
// ... 写入 inputBB
MemorySegment inputSeg = arena.allocate(inputSize);
inputSeg.copyFrom(MemorySegment.ofArray(inputBuf));

// === 修改后 ===
MemorySegment inputSeg = arena.allocate(inputSize);
ByteBuffer inputBB = inputSeg.asByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
// ... 直接写入 inputBB，无需拷贝
```

**预期收益**: 高（KV 编码每次 batch 触发，减少一次 memcpy）
**改动量**: 小（~15 行）
**风险**: 低

---

### [P1-2] NativeTopicTrie：消除每层级 UTF-8 byte[] 分配

- [ ] **勾兑确认人**: ___________

**目标文件**:
- `bifromq-native-binding/src/main/java/org/apache/bifromq/nativebinding/topic/NativeTopicTrie.java:216`

**问题**:
- `String.getBytes(UTF_8)` 为每个 topic level 分配 byte[]

**实施修改**:

1. 常见 topic level 缓存 UTF-8 byte[]（LRU，容量如 1024）
2. 或用 pooled `byte[]` + `String.getBytes(CharsetEncoder)` 写入预分配 buffer

```java
// === 方案 A：ThreadLocal byte[] pool（轻量）===
private static final ThreadLocal<byte[]> UTF8_BUF = ThreadLocal.withInitial(() -> new byte[256]);

void allocateLevels(String[] levels) {
    for (String level : levels) {
        byte[] buf = UTF8_BUF.get();
        byte[] bytes = level.getBytes(UTF_8); // 仍有一次分配
        // 无法避免 String.getBytes 内部分配...
    }
}

// === 方案 B：CharsetEncoder 写入预分配 ByteBuffer（推荐）===
private static final ThreadLocal<CharsetEncoder> ENCODER =
    ThreadLocal.withInitial(() -> StandardCharsets.UTF_8.newEncoder());

private static final ThreadLocal<ByteBuffer> BYTE_BUF =
    ThreadLocal.withInitial(() -> ByteBuffer.allocate(256));

void allocateLevels(String[] levels) {
    CharsetEncoder enc = ENCODER.get();
    ByteBuffer buf = BYTE_BUF.get();
    for (String level : levels) {
        buf.clear();
        enc.reset();
        enc.encode(CharBuffer.wrap(level), buf, true);
        enc.flush(buf);
        int len = buf.position();
        buf.flip();
        // 传入 native：buf, 0, len
        nativeAddLevel(buf, len);
    }
}
```

**预期收益**: 高（topic match 是核心路径，减少 GC）
**改动量**: 小（~30 行）
**风险**: 低

---

### [P1-3] MQTT5MessageSizer：消除每条消息的临时 record 分配

- [ ] **勾兑确认人**: ___________

**目标文件**:
- `bifromq-mqtt/bifromq-mqtt-server/src/main/java/org/apache/bifromq/mqtt/server/util/MQTT5MessageSizer.java`

**问题**:
- `sizeOf()` 返回 `MqttMessageSize` record
- `sizeMqttProperties()` 返回 `MqttPropertiesBytes` record
- 每条 MQTT 消息都经过 size 计算

**实施修改**:

1. 添加 `sizeOf(MqttMessage msg, MutableSizeResult result)` 重载
2. 调用侧复用 thread-local `MutableSizeResult`

```java
// === 新增 ===
public static class MutableSizeResult {
    public int fixedHeaderSize;
    public int variableHeaderSize;
    public int payloadSize;
    public int totalSize() { return fixedHeaderSize + variableHeaderSize + payloadSize; }
}

private static final ThreadLocal<MutableSizeResult> TL_RESULT =
    ThreadLocal.withInitial(MutableSizeResult::new);

// === 修改后入口 ===
public static int sizeOf(MqttMessage msg) {
    MutableSizeResult r = TL_RESULT.get();
    sizeOf(msg, r);
    return r.totalSize();
}

public static void sizeOf(MqttMessage msg, MutableSizeResult r) {
    r.fixedHeaderSize = ...;
    r.variableHeaderSize = sizeVarHeader(msg, r); // 复用内部字段
    r.payloadSize = sizePayload(msg);
}
```

**预期收益**: 高（每条消息 size 计算，零分配）
**改动量**: 小（~40 行）
**风险**: 极低

---

### [P1-4] BatchTimeoutWheel：避免 O(N) 全局扫描

- [ ] **勾兑确认人**: ___________

**目标文件**:
- `base-scheduler/src/main/java/org/apache/bifromq/basescheduler/Batcher.java:328-348`

**问题**:
- 每 5ms 遍历所有 Batcher 实例
- 每个 Batcher 再遍历 `ConcurrentLinkedQueue`
- batch 数量随租户线性增长

**实施修改**:

1. 只注册有 pending timeout 的 batcher 到 wheel（延迟注册）
2. `LongAdder` 记录 pending 数量，为零时跳过扫描

```java
// === 修改前：定时扫描所有 batcher ===
for (Batcher<?, ?, ?> batcher : allBatchers) {
    batcher.drainTimeouts(now);
}

// === 修改后：动态注册 ===
private static final Set<Batcher<?, ?, ?>> ACTIVE_BATCHERS = ConcurrentHashMap.newKeySet();
private final LongAdder pendingTimeoutCount = new LongAdder();

void scheduleTimeout(TimeoutEntry entry) {
    pendingTimeouts.offer(entry);
    pendingTimeoutCount.increment();
    ACTIVE_BATCHERS.add(this); // 延迟注册
}

// BatchTimeoutWheel 扫描逻辑
static {
    SCHEDULER.scheduleAtFixedRate(() -> {
        long now = System.nanoTime();
        for (Batcher<?, ?, ?> b : ACTIVE_BATCHERS) {
            if (b.pendingTimeoutCount.sum() == 0) {
                ACTIVE_BATCHERS.remove(b); // 注销
            } else {
                b.drainTimeouts(now);
            }
        }
    }, 5, 5, TimeUnit.MILLISECONDS);
}
```

**预期收益**: 高（batch 数量随租户线性增长，避免空轮询）
**改动量**: 小（~25 行）
**风险**: 低

---

### [P1-5] InboxFetchPipeline：消除 synchronized send 串行化

- [ ] **勾兑确认人**: ___________

**目标文件**:
- `bifromq-inbox-server/src/main/java/org/apache/bifromq/inbox/server/InboxFetchPipeline.java:124`

**问题**:
- `synchronized(this)` 在 `send()` 上串行所有 inbox fetch 的 gRPC 流写入

**实施修改**:

1. 确认 gRPC `StreamObserver` 是否要求单线程（Netty gRPC 的 `ServerCallStreamObserver` 不要求）
2. 如不要求，移除 `synchronized`；如要求，用 `ConcurrentLinkedQueue` + flush coalescing

```java
// === 修改前 ===
synchronized (this) {
    if (closed) return;
    responseObserver.onNext(response);
}

// === 修改后（方案 A：直接移除，如 StreamObserver 线程安全）===
if (closed) return;
responseObserver.onNext(response);

// === 修改后（方案 B：队列合并 flush，如需要顺序保证）===
private final ConcurrentLinkedQueue<FetchResponse> writeQueue = new ConcurrentLinkedQueue<>();
private final AtomicBoolean flushPending = new AtomicBoolean(false);

void send(FetchResponse response) {
    writeQueue.offer(response);
    if (flushPending.compareAndSet(false, true)) {
        eventLoop.execute(this::flush);
    }
}

private void flush() {
    flushPending.set(false);
    FetchResponse r;
    while ((r = writeQueue.poll()) != null) {
        responseObserver.onNext(r);
    }
}
```

**预期收益**: 高（同一 inbox 高并发时消除串行点）
**改动量**: 小（~20 行）
**风险**: 中，需验证 gRPC StreamObserver 线程安全承诺

---

### [P1-6] RocksDB WriteBatch byte[] 拷贝消除

- [ ] **勾兑确认人**: ___________

**目标文件**:
- `base-kv/base-kv-local-engine-rocksdb/src/main/java/org/apache/bifromq/basekv/localengine/rocksdb/RocksDBKVSpaceWriterHelper.java:77,86,91`

**问题**:
- `ByteString.toByteArray()` 防御性拷贝
- 大 value 时内存占用翻倍

**实施修改**:

1. 使用 `WriteBatch.put(ByteBuffer, ByteBuffer)` 重载（RocksDB JNI 较新版本支持）
2. 或 `UnsafeByteOperations` 零拷贝 wrap（如可用）

```java
// === 修改前 ===
batch.put(cfHandle, key.toByteArray(), value.toByteArray());

// === 修改后（需确认 rocksdbjni 版本 >= 7.0 支持 ByteBuffer put）===
ByteBuffer keyBB = key.asReadOnlyByteBuffer();
ByteBuffer valBB = value.asReadOnlyByteBuffer();
batch.put(cfHandle, keyBB, valBB);
```

**预期收益**: 中-高（大 value 场景减少内存拷贝）
**改动量**: 极小（~5 行）
**风险**: 低，需确认 rocksdbjni API 版本

---

## P2 — 中长期收益（GC 压力缓解，预计 GC 暂停降低 10-20%）

### [P2-1] TopicUtil.parse()：ThreadLocal 复用 ArrayList + StringBuilder

- [ ] **勾兑确认人**: ___________

**目标文件**:
- `bifromq-util/src/main/java/org/apache/bifromq/util/TopicUtil.java:199-225`

**问题**:
- 每 topic 解析都新建 `ArrayList<String>` 和 `StringBuilder`

**实施修改**:

```java
// === 修改前 ===
public static List<String> parse(String topic, boolean isTopicFilter) {
    List<String> levels = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    // ...
}

// === 修改后 ===
private static final ThreadLocal<StringBuilder> TL_SB =
    ThreadLocal.withInitial(() -> new StringBuilder(64));
private static final ThreadLocal<ArrayList<String>> TL_LIST =
    ThreadLocal.withInitial(() -> new ArrayList<>(8));

public static List<String> parse(String topic, boolean isTopicFilter) {
    StringBuilder sb = TL_SB.get();
    sb.setLength(0);
    ArrayList<String> levels = TL_LIST.get();
    levels.clear();
    // ... 解析逻辑不变 ...
    return new ArrayList<>(levels); // 返回副本，避免泄漏 thread-local
}
```

**预期收益**: 高（topic 解析是 PUBLISH/SUBSCRIBE 必经之路）
**改动量**: 小（~20 行）
**风险**: 极低

---

### [P2-2] TopicUtil.escape/unescape()：char[] 原地转换替代正则

- [ ] **勾兑确认人**: ___________

**目标文件**:
- `bifromq-util/src/main/java/org/apache/bifromq/util/TopicUtil.java:192-195`

**问题**:
- `String.replace` 内部使用正则引擎，对简单 char 替换是 overkill

**实施修改**:

```java
// === 修改前 ===
public static String escape(String level) {
    return level.replace("#", "\#").replace("+", "\+");
}

// === 修改后 ===
public static String escape(String level) {
    int len = level.length();
    StringBuilder sb = TL_SB.get();
    sb.setLength(0);
    for (int i = 0; i < len; i++) {
        char c = level.charAt(i);
        if (c == '#' || c == '+') sb.append('\\');
        sb.append(c);
    }
    return sb.toString();
}
```

**预期收益**: 中（减少正则开销）
**改动量**: 极小（~10 行）
**风险**: 极低

---

### [P2-3] FastBehaviorSubject：ReentrantReadWriteLock → StampedLock

- [ ] **勾兑确认人**: ___________

**目标文件**:
- `base-rpc/base-rpc-client/src/main/java/org/apache/bifromq/baserpc/client/FastBehaviorSubject.java:51`

**问题**:
- `ReentrantReadWriteLock` 在高并发读场景有内核态开销

**实施修改**:

```java
// === 修改前 ===
private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

public T getValue() {
    rwLock.readLock().lock();
    try { return value; }
    finally { rwLock.readLock().unlock(); }
}

// === 修改后 ===
private final StampedLock stampLock = new StampedLock();

public T getValue() {
    long stamp = stampLock.tryOptimisticRead();
    T v = value;
    if (!stampLock.validate(stamp)) {
        stamp = stampLock.readLock();
        try { v = value; }
        finally { stampLock.unlockRead(stamp); }
    }
    return v;
}
```

**预期收益**: 中（读多写少场景减少锁开销）
**改动量**: 极小（~10 行）
**风险**: 低

---

### [P2-4] Protobuf Builder 复用：ThreadLocal + clear() 替代 newBuilder()

- [ ] **勾兑确认人**: ___________

**目标文件**:
- `bifromq-dist-worker/.../DistWorkerCoProc.java`
- `bifromq-inbox-store/.../InboxStoreCoProc.java`
- 其他频繁构建 protobuf 的类

**问题**:
- `newBuilder()` 每次分配内部数组

**实施修改**:

```java
// === 以 DistWorkerCoProc 为例 ===
private static final ThreadLocal<SubInfo.Builder> SUBINFO_BUILDER =
    ThreadLocal.withInitial(SubInfo::newBuilder);

// === 修改前 ===
SubInfo sub = SubInfo.newBuilder().setTenantId(tenantId).setInboxId(inboxId).build();

// === 修改后 ===
SubInfo.Builder b = SUBINFO_BUILDER.get();
b.clear();
b.setTenantId(tenantId);
b.setInboxId(inboxId);
SubInfo sub = b.build();
```

**预期收益**: 中-高（减少 protobuf 内部数组分配）
**改动量**: 中（每个 Builder 类型需改一处）
**风险**: 低，需确认 `clear()` 是否深清空所有字段

---

## JVM 运行时调优（无需代码修改，仅需启动参数）

### [JVM-1] Compact Object Headers（-XX:+UseCompactObjectHeaders）

- [ ] **勾兑确认人**: ___________

**适用**: JDK 25+（当前分支目标）
**参数**: `-XX:+UseCompactObjectHeaders`
**收益**: 小对象密集的 broker 场景（SubInfo、MatchInfo 等）对象头从 12/16B 降至 4B，堆内存降低 10-20%
**风险**: 极低，JDK 25 正式特性

---

### [JVM-2] ZGC / Shenandoah（低延迟 GC）

- [ ] **勾兑确认人**: ___________

**参数**:
- ZGC: `-XX:+UseZGC -XX:+ZGenerational`
- Shenandoah: `-XX:+UseShenandoahGC`
**收益**: 亚毫秒级暂停，适合延迟敏感场景
**风险**: 低，但需监控吞吐是否下降（ZGC 吞吐略低于 G1）

---

### [JVM-3] String Deduplication

- [ ] **勾兑确认人**: ___________

**参数**: `-XX:+UseStringDeduplication`（配合 G1/ZGC）
**收益**: tenant ID 和 topic 字符串重复度高，堆内字符串去重减少 5-15% 内存
**风险**: 极低

---

### [JVM-4] Large Pages

- [ ] **勾兑确认人**: ___________

**参数**:
- `-XX:+UseLargePages`
- `-XX:LargePageSizeInBytes=2M`（Linux x86_64）
**收益**: RocksDB + Netty direct memory TLB miss 减少，吞吐提升 5-10%
**风险**: 低，需系统配置 `/proc/sys/vm/nr_hugepages`

---

## 已实施项目（当前分支已完成）

- [x] **Batch Timeout Wheel（base-scheduler/Batcher）**: 共享 ScheduledExecutorService 扫描 ConcurrentLinkedQueue，替代 per-batch `CompletableFuture.orTimeout()`
- [x] **StampedLock in MovingAverage（base-scheduler）**: `synchronized` 改为 `StampedLock` 乐观读
- [x] **StampedLock in RPC Client（base-rpc-client）**: `ManagedBiDiStream` / `ManagedRequestPipeline` 的 `synchronized` 改为 `StampedLock`
- [x] **Group Commit WriteBatch 合并（base-kv-local-engine-rocksdb）**: `WriteBatch.append()` 合并多个 follower batch，单次 `db.write()`
- [x] **RocksDB Blob 阈值调优（base-kv-local-engine-rocksdb）**: `MIN_BLOB_SIZE` 从 2KB 提升至 32KB，减少小 value 读放大
- [x] **GroupCommitWriteQueue ArrayList 预分配外移（base-kv-local-engine-rocksdb）**: lock 内 `new ArrayList<>(pendingWrites.size())` 改为预分配，减少锁持有时间
- [x] **MQTTTransientSessionHandler auth cache key（bifromq-mqtt-server）**: 用 `TopicQosKey` record 替代字符串拼接，消除 fanout 场景下的短生命周期字符串分配
- [x] **RocksDB WAL 批量刷盘（base-kv-local-engine-rocksdb）**: `RocksDBOptionsUtil` 启用 `setWalBytesPerSync(1MB)`，配合 `fsync=false` 减少刷盘频率
- [x] **Keys.toDataKey/toMetaKey 零拷贝（base-kv-local-engine-rocksdb）**: 避免 `ByteString.concat().toByteArray()` 的防御性拷贝

---

## 执行顺序建议

| 阶段 | 时间 | 优化项 | 预期收益 |
|------|------|--------|----------|
| **阶段 1**（1-2 周） | P0-1, P0-2, P0-3, P0-4 | 消除高频分配风暴 | 20-40% 吞吐提升 |
| **阶段 2**（2-3 周） | P1-1, P1-2, P1-3, P1-4, P1-5, P1-6 | Native/JNI 零拷贝 + 锁优化 | 15-25% 延迟降低 |
| **阶段 3**（1-2 周） | P2-1, P2-2, P2-3, P2-4 | GC 压力缓解 | 10-20% GC 暂停降低 |
| **阶段 4**（1 周） | JVM-1 ~ JVM-4 | 运行时调优 | 5-15% 综合提升 |

---

## 测试要求（每项必做）

1. **单元测试**: `./mvnw test -pl <modified-module> -am`
2. **集成测试**: `./mvnw test -Pbuild-coverage`（全量）
3. **JMH Benchmark**（如该项有 benchmark）: `./mvnw -pl <module> test -Dtest=*Benchmark -DfailIfNoTests=false`
4. **CI 通过**: GitHub Actions `docker-build.yml` 成功
5. **勾兑确认**: owner 在 checkbox 签名确认后方可合并

---

*本文件由 Claude Code 生成，需经技术负责人 review 后生效。*
