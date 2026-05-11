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

package org.apache.bifromq.dist.worker.schema.benchmark;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.bifromq.dist.worker.schema.KVSchemaUtil;
import org.apache.bifromq.type.RouteMatcher;
import org.apache.bifromq.util.TopicUtil;
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
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(value = 1, jvmArgsAppend = "--enable-native-access=ALL-UNNAMED")
@State(Scope.Thread)
public class KVSchemaUtilBenchmark {
    @Param({"plain", "wildcard", "shared"})
    String filterShape;

    private String tenantId;
    private RouteMatcher normalMatcher;
    private RouteMatcher groupMatcher;
    private String receiverUrl;
    private List<String> filterLevels;

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
            .include(KVSchemaUtilBenchmark.class.getSimpleName())
            .addProfiler(GCProfiler.class)
            .shouldDoGC(true)
            .build();
        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setup() {
        tenantId = "tenantA";
        normalMatcher = TopicUtil.from(topicFilter());
        groupMatcher = TopicUtil.from("$share/groupA/" + topicFilter());
        receiverUrl = KVSchemaUtil.toReceiverUrl(1, "receiverA", "delivererA");
        filterLevels = normalMatcher.getFilterLevelList();
    }

    @Benchmark
    public void tenantBeginKey(Blackhole bh) {
        bh.consume(KVSchemaUtil.tenantBeginKey(tenantId));
    }

    @Benchmark
    public void tenantRouteStartKey(Blackhole bh) {
        bh.consume(KVSchemaUtil.tenantRouteStartKey(tenantId, filterLevels));
    }

    @Benchmark
    public void normalRouteKey(Blackhole bh) {
        bh.consume(KVSchemaUtil.toNormalRouteKey(tenantId, normalMatcher, receiverUrl));
    }

    @Benchmark
    public void groupRouteKey(Blackhole bh) {
        bh.consume(KVSchemaUtil.toGroupRouteKey(tenantId, groupMatcher));
    }

    private String topicFilter() {
        return switch (filterShape) {
            case "plain" -> "device/region/up/status";
            case "wildcard" -> "device/+/up/#";
            case "shared" -> "tenant/$sys/up/+";
            default -> throw new IllegalStateException("Unexpected filter shape: " + filterShape);
        };
    }
}
