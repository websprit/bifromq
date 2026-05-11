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
import org.apache.bifromq.native_binding.topic.NativeTopicTrie;
import org.apache.bifromq.util.TopicUtil;

final class NativeTenantTopicMatcher implements AutoCloseable {
    private final List<String> topics;
    private final NativeTopicTrie trie;

    NativeTenantTopicMatcher(Set<String> topics) {
        this.topics = new ArrayList<>(topics.size());
        trie = new NativeTopicTrie(false);
        long topicIndex = 0;
        for (String topic : topics) {
            this.topics.add(topic);
            trie.add(TopicUtil.parse(topic, false), topicIndex++);
        }
    }

    MatchResult match(List<String> filterLevels) {
        Set<Long> topicIndexes = trie.match(filterLevels);
        if (topicIndexes.isEmpty()) {
            return null;
        }
        Set<String> matchedTopics = new HashSet<>(topicIndexes.size());
        for (long topicIndex : topicIndexes) {
            matchedTopics.add(topic(topicIndex));
        }
        return new MatchResult(filterLevels, matchedTopics);
    }

    BatchMatchResult matchBatch(List<List<String>> filterLevelsList) {
        NativeTopicTrie.BatchMatchResult result = trie.matchBatch(filterLevelsList);
        return new BatchMatchResult(filterLevelsList, result.resultOffsets(), result.resultCounts(), result.topicIds());
    }

    String topic(long topicIndex) {
        return topics.get(Math.toIntExact(topicIndex));
    }

    @Override
    public void close() {
        trie.close();
    }

    record MatchResult(List<String> key, Set<String> topics) {
    }

    record BatchMatchResult(List<List<String>> keys, int[] resultOffsets, int[] resultCounts, long[] topicIds) {
        TopicIds topicIds(int filterIndex) {
            return new TopicIds(topicIds, resultOffsets[filterIndex], resultCounts[filterIndex]);
        }

        String topic(NativeTenantTopicMatcher matcher, int filterIndex, int resultIndex) {
            return matcher.topic(topicIds[resultOffsets[filterIndex] + resultIndex]);
        }
    }

    record TopicIds(long[] topicIds, int offset, int count) {
        String topic(NativeTenantTopicMatcher matcher, int resultIndex) {
            return matcher.topic(topicIds[offset + resultIndex]);
        }
    }
}
