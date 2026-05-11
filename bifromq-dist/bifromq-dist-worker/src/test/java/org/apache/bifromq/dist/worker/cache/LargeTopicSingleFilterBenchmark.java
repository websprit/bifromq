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
import org.apache.bifromq.native_binding.topic.NativeTopicTrie;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@Fork(value = 1, jvmArgsAppend = "--enable-native-access=ALL-UNNAMED")
@State(Scope.Thread)
public class LargeTopicSingleFilterBenchmark {
    @Param({"100000"})
    int topicCount;

    @Param({"exact", "narrowPlus", "regionPlus", "deviceHash", "all"})
    String filterShape;

    private List<String> topics;
    private List<String> filterLevels;
    private TopicTrieNode<String> javaRoot;
    private NativeTopicTrie nativeTrie;

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
            .include(LargeTopicSingleFilterBenchmark.class.getSimpleName())
            .addProfiler(GCProfiler.class)
            .shouldDoGC(true)
            .build();
        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setup() {
        topics = new ArrayList<>(topicCount);
        TopicTrieNode.Builder<String> builder = TopicTrieNode.builder(false);
        nativeTrie = new NativeTopicTrie(false);
        long topicIndex = 0;
        for (int i = 0; i < topicCount; i++) {
            String topic = topic(i);
            topics.add(topic);
            List<String> topicLevels = TopicUtil.parse(topic, false);
            builder.addTopic(topicLevels, topic);
            nativeTrie.add(topicLevels, topicIndex++);
        }
        javaRoot = builder.build();
        filterLevels = TopicUtil.parse(filter(), true);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        nativeTrie.close();
    }

    @Benchmark
    public void javaBuildOnly(Blackhole bh) {
        TopicTrieNode.Builder<String> builder = TopicTrieNode.builder(false);
        for (String topic : topics) {
            builder.addTopic(TopicUtil.parse(topic, false), topic);
        }
        bh.consume(builder.build());
    }

    @Benchmark
    public void nativeBuildOnly(Blackhole bh) {
        try (NativeTopicTrie trie = new NativeTopicTrie(false)) {
            long topicIndex = 0;
            for (String topic : topics) {
                trie.add(TopicUtil.parse(topic, false), topicIndex++);
            }
            bh.consume(trie);
        }
    }

    @Benchmark
    public void javaMatchOnly(Blackhole bh) {
        try (ITopicFilterIterator<String> iterator = ThreadLocalTopicFilterIterator.get(javaRoot)) {
            iterator.seek(filterLevels);
            if (iterator.isValid() && iterator.key().equals(filterLevels)) {
                int matchedTopicCount = 0;
                for (Set<String> topicSet : iterator.value().values()) {
                    matchedTopicCount += topicSet.size();
                }
                bh.consume(matchedTopicCount);
            }
        }
    }

    @Benchmark
    public void javaMatchCopyTopics(Blackhole bh) {
        try (ITopicFilterIterator<String> iterator = ThreadLocalTopicFilterIterator.get(javaRoot)) {
            iterator.seek(filterLevels);
            if (iterator.isValid() && iterator.key().equals(filterLevels)) {
                Set<String> matchedTopics = new HashSet<>();
                for (Set<String> topicSet : iterator.value().values()) {
                    matchedTopics.addAll(topicSet);
                }
                bh.consume(matchedTopics);
            }
        }
    }

    @Benchmark
    public void nativeMatchOnly(Blackhole bh) {
        Set<Long> matchedTopicIndexes = nativeTrie.match(filterLevels);
        bh.consume(matchedTopicIndexes.size());
    }

    @Benchmark
    public void nativeMatchCopyTopics(Blackhole bh) {
        Set<Long> matchedTopicIndexes = nativeTrie.match(filterLevels);
        Set<String> matchedTopics = new HashSet<>(matchedTopicIndexes.size());
        for (long topicIndex : matchedTopicIndexes) {
            matchedTopics.add(topics.get(Math.toIntExact(topicIndex)));
        }
        bh.consume(matchedTopics);
    }

    private String topic(int i) {
        return "device" + i + "/region" + i % 16 + "/up/status";
    }

    private String filter() {
        return switch (filterShape) {
            case "exact" -> "device123/region11/up/status";
            case "narrowPlus" -> "device123/+/up/status";
            case "regionPlus" -> "+/region1/up/status";
            case "deviceHash" -> "device123/#";
            case "all" -> "#";
            default -> throw new IllegalStateException("Unexpected filter shape: " + filterShape);
        };
    }
}
