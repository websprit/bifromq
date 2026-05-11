/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bifromq.dist.worker.cache;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toNormalRouteKey;
import static org.apache.bifromq.dist.worker.schema.cache.Matchings.normalMatching;

import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.basekv.store.api.IKVRangeRefreshableReader;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.dist.worker.schema.cache.NormalMatching;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.util.BSUtil;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@Fork(value = 1, jvmArgsAppend = "--enable-native-access=ALL-UNNAMED")
@org.openjdk.jmh.annotations.State(Scope.Thread)
public class TenantRouteMatcherBenchmark {
    private static final String TENANT_ID = "tenantA";
    private static final String TOPIC = "metrics/server1/cpu";
    private static final IEventCollector NOOP_EVENT_COLLECTOR = event -> {
    };

    @Param({"1000", "10000"})
    int routeCount;

    @Param({"sparseTail", "denseSameFilter"})
    String routeShape;

    private SimpleMeterRegistry meterRegistry;
    private TenantRouteMatcher matcher;
    private Set<String> topics;

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
            .include(TenantRouteMatcherBenchmark.class.getSimpleName())
            .addProfiler(GCProfiler.class)
            .shouldDoGC(true)
            .build();
        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setup() {
        meterRegistry = new SimpleMeterRegistry();
        Timer timer = meterRegistry.timer("tenantRouteMatch");
        NavigableMap<ByteString, ByteString> kvData = new TreeMap<>(ByteString.unsignedLexicographicalComparator());
        switch (routeShape) {
            case "sparseTail" -> setupSparseTail(kvData);
            case "denseSameFilter" -> setupDenseSameFilter(kvData);
            default -> throw new IllegalStateException("Unexpected route shape: " + routeShape);
        }
        matcher = new TenantRouteMatcher(TENANT_ID, () -> new TreeMapKVReader(kvData), NOOP_EVENT_COLLECTOR, timer);
        topics = Set.of(TOPIC);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        meterRegistry.close();
    }

    @Benchmark
    public void matchAll(Blackhole bh) {
        Map<String, IMatchedRoutes> matchedRoutes = matcher.matchAll(topics, routeCount + 1, routeCount + 1);
        bh.consume(matchedRoutes.get(TOPIC).routes().size());
    }

    private void setupSparseTail(NavigableMap<ByteString, ByteString> kvData) {
        for (int i = 0; i < routeCount; i++) {
            NormalMatching noise = normalMatching(TENANT_ID, "invalid/" + padded(i), 1,
                "noise" + i, "deliverer" + i, i);
            kvData.put(toNormalRouteKey(TENANT_ID, noise.matcher, noise.receiverUrl()),
                BSUtil.toByteString(noise.incarnation()));
        }
        NormalMatching valid = normalMatching(TENANT_ID, "metrics/+/cpu", 1,
            "receiver", "deliverer", routeCount);
        kvData.put(toNormalRouteKey(TENANT_ID, valid.matcher, valid.receiverUrl()),
            BSUtil.toByteString(valid.incarnation()));
    }

    private void setupDenseSameFilter(NavigableMap<ByteString, ByteString> kvData) {
        for (int i = 0; i < routeCount; i++) {
            NormalMatching matching = normalMatching(TENANT_ID, "metrics/+/cpu", 1,
                "receiver" + padded(i), "deliverer" + i, i);
            kvData.put(toNormalRouteKey(TENANT_ID, matching.matcher, matching.receiverUrl()),
                BSUtil.toByteString(matching.incarnation()));
        }
    }

    private String padded(int value) {
        return String.format("%08d", value);
    }

    private static final class TreeMapKVReader implements IKVRangeRefreshableReader {
        private final NavigableMap<ByteString, ByteString> data;

        private TreeMapKVReader(NavigableMap<ByteString, ByteString> data) {
            this.data = data;
        }

        @Override
        public long version() {
            return 0;
        }

        @Override
        public State state() {
            return State.newBuilder().setType(State.StateType.Normal).build();
        }

        @Override
        public long lastAppliedIndex() {
            return 0;
        }

        @Override
        public Boundary boundary() {
            if (data.isEmpty()) {
                return FULL_BOUNDARY;
            }
            return toBoundary(data.firstKey(), upperBound(data.lastKey()));
        }

        @Override
        public ClusterConfig clusterConfig() {
            return ClusterConfig.newBuilder().build();
        }

        @Override
        public long size(Boundary boundary) {
            if (data.isEmpty()) {
                return 0;
            }
            ByteString start = BoundaryUtil.startKey(boundary);
            ByteString end = BoundaryUtil.endKey(boundary);
            NavigableMap<ByteString, ByteString> sub = data;
            if (start != null) {
                sub = sub.tailMap(start, true);
            }
            if (end != null) {
                sub = sub.headMap(end, false);
            }
            return sub.size();
        }

        @Override
        public boolean exist(ByteString key) {
            return data.containsKey(key);
        }

        @Override
        public Optional<ByteString> get(ByteString key) {
            return Optional.ofNullable(data.get(key));
        }

        @Override
        public ByteString getDirect(ByteString key) {
            return data.get(key);
        }

        @Override
        public IKVIterator iterator() {
            return new TreeMapKVIterator(data);
        }

        @Override
        public IKVIterator iterator(Boundary boundary) {
            ByteString start = BoundaryUtil.startKey(boundary);
            ByteString end = BoundaryUtil.endKey(boundary);
            NavigableMap<ByteString, ByteString> sub = data;
            if (start != null) {
                sub = sub.tailMap(start, true);
            }
            if (end != null) {
                sub = sub.headMap(end, false);
            }
            return new TreeMapKVIterator(sub);
        }

        @Override
        public void close() {
        }

        @Override
        public void refresh() {
        }
    }

    private static final class TreeMapKVIterator implements IKVIterator {
        private final NavigableMap<ByteString, ByteString> data;
        private Map.Entry<ByteString, ByteString> current;

        private TreeMapKVIterator(NavigableMap<ByteString, ByteString> data) {
            this.data = data;
        }

        @Override
        public ByteString key() {
            if (current == null) {
                throw new IllegalStateException("Iterator is not valid");
            }
            return current.getKey();
        }

        @Override
        public ByteString value() {
            if (current == null) {
                throw new IllegalStateException("Iterator is not valid");
            }
            return current.getValue();
        }

        @Override
        public boolean isValid() {
            return current != null;
        }

        @Override
        public void next() {
            if (current == null) {
                throw new IllegalStateException("Iterator is not valid");
            }
            current = data.higherEntry(current.getKey());
        }

        @Override
        public void prev() {
            if (current == null) {
                throw new IllegalStateException("Iterator is not valid");
            }
            current = data.lowerEntry(current.getKey());
        }

        @Override
        public void seekToFirst() {
            current = data.firstEntry();
        }

        @Override
        public void seekToLast() {
            current = data.lastEntry();
        }

        @Override
        public void seek(ByteString key) {
            current = key == null ? data.firstEntry() : data.ceilingEntry(key);
        }

        @Override
        public void seekForPrev(ByteString key) {
            current = key == null ? data.lastEntry() : data.floorEntry(key);
        }

        @Override
        public void close() {
        }
    }
}
