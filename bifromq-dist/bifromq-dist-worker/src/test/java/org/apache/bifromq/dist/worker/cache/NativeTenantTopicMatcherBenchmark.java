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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.bifromq.dist.trie.ITopicFilterIterator;
import org.apache.bifromq.dist.trie.ThreadLocalTopicFilterIterator;
import org.apache.bifromq.dist.trie.TopicTrieNode;
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
public class NativeTenantTopicMatcherBenchmark {
    @Param({"100", "1000"})
    int topicCount;

    @Param({"plain", "sys", "shared"})
    String topicShape;

    @Param({"1", "32"})
    int filterMultiplier;

    private Set<String> topics;
    private TopicTrieNode<String> root;
    private List<List<String>> seekFilters;

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
            .include(NativeTenantTopicMatcherBenchmark.class.getSimpleName())
            .addProfiler(GCProfiler.class)
            .shouldDoGC(true)
            .build();
        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setup() {
        topics = new HashSet<>(topicCount);
        TopicTrieNode.Builder<String> builder = TopicTrieNode.builder(false);
        for (int i = 0; i < topicCount; i++) {
            String topic = topic(i);
            topics.add(topic);
            builder.addTopic(TopicUtil.parse(topic, false), topic);
        }
        root = builder.build();
        seekFilters = seekFilters();
    }

    @Benchmark
    public void javaIteratorStyleSeek(Blackhole bh) {
        try (ITopicFilterIterator<String> iterator = ThreadLocalTopicFilterIterator.get(root)) {
            for (List<String> seekFilter : seekFilters) {
                iterator.seek(seekFilter);
                if (iterator.isValid()) {
                    bh.consume(iterator.key());
                    Set<String> matchedTopics = new HashSet<>();
                    for (Set<String> topicSet : iterator.value().values()) {
                        matchedTopics.addAll(topicSet);
                    }
                    bh.consume(matchedTopics);
                }
            }
        }
    }

    @Benchmark
    public void nativeTenantMatcherStyleSeek(Blackhole bh) {
        try (NativeTenantTopicMatcher matcher = new NativeTenantTopicMatcher(topics)) {
            for (List<String> seekFilter : seekFilters) {
                NativeTenantTopicMatcher.MatchResult result = matcher.match(seekFilter);
                if (result != null) {
                    bh.consume(result.key());
                    bh.consume(result.topics());
                }
            }
        }
    }

    @Benchmark
    public void nativeTenantMatcherBatch16(Blackhole bh) {
        nativeTenantMatcherBatch(bh, 16);
    }

    @Benchmark
    public void nativeTenantMatcherBatch64(Blackhole bh) {
        nativeTenantMatcherBatch(bh, 64);
    }

    @Benchmark
    public void nativeTenantMatcherBatch128(Blackhole bh) {
        nativeTenantMatcherBatch(bh, 128);
    }

    private void nativeTenantMatcherBatch(Blackhole bh, int batchSize) {
        try (NativeTenantTopicMatcher matcher = new NativeTenantTopicMatcher(topics)) {
            for (int from = 0; from < seekFilters.size(); from += batchSize) {
                int to = Math.min(from + batchSize, seekFilters.size());
                NativeTenantTopicMatcher.BatchMatchResult result = matcher.matchBatch(seekFilters.subList(from, to));
                for (int i = 0; i < result.resultCounts().length; i++) {
                    if (result.resultCounts()[i] > 0) {
                        bh.consume(result.keys().get(i));
                        for (int j = 0; j < result.resultCounts()[i]; j++) {
                            bh.consume(result.topic(matcher, i, j));
                        }
                    }
                }
            }
        }
    }

    private String topic(int i) {
        return switch (topicShape) {
            case "plain" -> "device" + i + "/region" + i % 16 + "/up/status";
            case "sys" -> i % 4 == 0
                ? "$sys/broker" + i % 8 + "/metric" + i
                : "device" + i + "/region" + i % 16 + "/up/status";
            case "shared" -> "tenant" + i % 8 + "/group" + i % 16 + "/device" + i + "/up/status";
            default -> throw new IllegalStateException("Unexpected topic shape: " + topicShape);
        };
    }

    private List<List<String>> seekFilters() {
        List<String> filters = switch (topicShape) {
            case "plain" -> List.of(
                "device0/region0/up/status",
                "device10/+/up/#",
                "+/region1/up/status",
                "device20/#");
            case "sys" -> List.of(
                "#",
                "$sys/broker0/#",
                "+/region1/up/status",
                "device20/#");
            case "shared" -> List.of(
                "tenant0/group0/device0/up/status",
                "tenant1/+/+/up/#",
                "+/group1/device17/#",
                sharedFilterLevels("$share/groupA/tenant2/+/device26/up/#"));
            default -> throw new IllegalStateException("Unexpected topic shape: " + topicShape);
        };
        List<List<String>> parsedFilters = new ArrayList<>(filters.size() * filterMultiplier);
        for (int i = 0; i < filterMultiplier; i++) {
            for (String filter : filters) {
                parsedFilters.add(TopicUtil.parse(filter, true));
            }
        }
        return parsedFilters;
    }

    private String sharedFilterLevels(String sharedFilter) {
        RouteMatcher matcher = TopicUtil.from(sharedFilter);
        return String.join("/", matcher.getFilterLevelList());
    }
}
