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

import static org.apache.bifromq.util.TopicConst.MULTI_WILDCARD;
import static org.apache.bifromq.util.TopicConst.SINGLE_WILDCARD;
import static org.apache.bifromq.util.TopicConst.SYS_PREFIX;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.bifromq.dist.worker.schema.cache.GroupMatching;
import org.apache.bifromq.dist.worker.schema.cache.Matching;
import org.apache.bifromq.native_binding.topic.NativeTopicTrie;
import org.apache.bifromq.util.TopicUtil;

final class NativeSubscriptionRouteIndex implements AutoCloseable {
    private final NativeTopicTrie trie = new NativeTopicTrie(false);
    private final Map<List<String>, Long> filterIds = new HashMap<>();
    private final Map<Long, List<String>> filters = new HashMap<>();
    private final Map<List<String>, Set<Matching>> routes = new HashMap<>();
    private long nextFilterId;

    void add(Matching matching) {
        List<String> filterLevels = List.copyOf(matching.matcher.getFilterLevelList());
        Set<Matching> matchings = routes.computeIfAbsent(filterLevels, k -> new HashSet<>());
        if (matchings.add(matching) && matchings.size() == 1) {
            long filterId = nextFilterId++;
            filterIds.put(filterLevels, filterId);
            filters.put(filterId, filterLevels);
            trie.add(filterLevels, filterId);
        }
    }

    void remove(Matching matching) {
        List<String> filterLevels = List.copyOf(matching.matcher.getFilterLevelList());
        Set<Matching> matchings = routes.get(filterLevels);
        if (matchings == null) {
            return;
        }
        matchings.remove(matching);
        removeFilterIfEmpty(filterLevels, matchings);
    }

    void removeGroup(GroupMatching matching) {
        List<String> filterLevels = List.copyOf(matching.matcher.getFilterLevelList());
        Set<Matching> matchings = routes.get(filterLevels);
        if (matchings == null) {
            return;
        }
        matchings.removeIf(existing -> existing.type() == Matching.Type.Group
            && existing.mqttTopicFilter().equals(matching.mqttTopicFilter()));
        removeFilterIfEmpty(filterLevels, matchings);
    }

    Set<Matching> match(String topic) {
        Set<Matching> matched = new HashSet<>();
        for (List<String> candidate : matchingFilterCandidates(TopicUtil.parse(topic, false))) {
            for (long filterId : trie.get(candidate)) {
                Set<Matching> matchings = routes.get(filters.get(filterId));
                if (matchings != null) {
                    matched.addAll(matchings);
                }
            }
        }
        return matched;
    }

    private void removeFilterIfEmpty(List<String> filterLevels, Set<Matching> matchings) {
        if (matchings.isEmpty()) {
            routes.remove(filterLevels);
            Long filterId = filterIds.remove(filterLevels);
            if (filterId != null) {
                filters.remove(filterId);
                trie.remove(filterLevels, filterId);
            }
        }
    }

    private Set<List<String>> matchingFilterCandidates(List<String> topicLevels) {
        Set<List<String>> candidates = new LinkedHashSet<>();
        collectExactCandidates(topicLevels, 0, new ArrayList<>(topicLevels.size()), candidates);
        for (int prefixLength = 0; prefixLength <= topicLevels.size(); prefixLength++) {
            collectHashCandidates(topicLevels, prefixLength, 0, new ArrayList<>(prefixLength + 1), candidates);
        }
        return candidates;
    }

    private void collectExactCandidates(List<String> topicLevels,
                                        int levelIndex,
                                        List<String> candidate,
                                        Set<List<String>> candidates) {
        if (levelIndex == topicLevels.size()) {
            candidates.add(List.copyOf(candidate));
            return;
        }
        String topicLevel = topicLevels.get(levelIndex);
        candidate.add(topicLevel);
        collectExactCandidates(topicLevels, levelIndex + 1, candidate, candidates);
        candidate.remove(candidate.size() - 1);
        if (levelIndex != 0 || !topicLevel.startsWith(SYS_PREFIX)) {
            candidate.add(SINGLE_WILDCARD);
            collectExactCandidates(topicLevels, levelIndex + 1, candidate, candidates);
            candidate.remove(candidate.size() - 1);
        }
    }

    private void collectHashCandidates(List<String> topicLevels,
                                       int prefixLength,
                                       int levelIndex,
                                       List<String> candidate,
                                       Set<List<String>> candidates) {
        if (levelIndex == prefixLength) {
            if (prefixLength > 0 || topicLevels.isEmpty() || !topicLevels.get(0).startsWith(SYS_PREFIX)) {
                candidate.add(MULTI_WILDCARD);
                candidates.add(List.copyOf(candidate));
                candidate.remove(candidate.size() - 1);
            }
            return;
        }
        String topicLevel = topicLevels.get(levelIndex);
        candidate.add(topicLevel);
        collectHashCandidates(topicLevels, prefixLength, levelIndex + 1, candidate, candidates);
        candidate.remove(candidate.size() - 1);
        if (levelIndex != 0 || !topicLevel.startsWith(SYS_PREFIX)) {
            candidate.add(SINGLE_WILDCARD);
            collectHashCandidates(topicLevels, prefixLength, levelIndex + 1, candidate, candidates);
            candidate.remove(candidate.size() - 1);
        }
    }

    @Override
    public void close() {
        trie.close();
    }
}
