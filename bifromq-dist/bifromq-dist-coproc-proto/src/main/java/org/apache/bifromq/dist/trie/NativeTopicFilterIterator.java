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

package org.apache.bifromq.dist.trie;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

final class NativeTopicFilterIterator<V> implements ITopicFilterIterator<V> {
    private final Map<Long, TopicValue<V>> topicValues = new HashMap<>();
    private org.apache.bifromq.native_binding.topic.NativeTopicTrie nativeTrie;
    private org.apache.bifromq.native_binding.topic.NativeTopicFilterIterator nativeIterator;
    private long nextValueId;

    @Override
    public void init(TopicTrieNode<V> root) {
        close();
        nativeTrie = new org.apache.bifromq.native_binding.topic.NativeTopicTrie(root.isGlobal());
        addTopics(root, new ArrayList<>());
        nativeIterator = new org.apache.bifromq.native_binding.topic.NativeTopicFilterIterator(nativeTrie);
        seek(List.of());
    }

    @Override
    public void close() {
        if (nativeIterator != null) {
            nativeIterator.close();
            nativeIterator = null;
        }
        if (nativeTrie != null) {
            nativeTrie.close();
            nativeTrie = null;
        }
        topicValues.clear();
        nextValueId = 0;
    }

    @Override
    public void seek(List<String> filterLevels) {
        nativeIterator.seek(filterLevels);
    }

    @Override
    public void seekPrev(List<String> filterLevels) {
        nativeIterator.seekPrev(filterLevels);
    }

    @Override
    public boolean isValid() {
        return nativeIterator.isValid();
    }

    @Override
    public void prev() {
        nativeIterator.prev();
    }

    @Override
    public void next() {
        nativeIterator.next();
    }

    @Override
    public List<String> key() {
        if (!isValid()) {
            throw new NoSuchElementException();
        }
        return nativeIterator.key();
    }

    @Override
    public Map<List<String>, Set<V>> value() {
        if (!isValid()) {
            throw new NoSuchElementException();
        }
        Map<List<String>, Set<V>> result = new HashMap<>();
        for (long valueId : nativeIterator.values()) {
            TopicValue<V> topicValue = topicValues.get(valueId);
            result.computeIfAbsent(topicValue.topicLevels, k -> new HashSet<>()).add(topicValue.value);
        }
        return result;
    }

    private void addTopics(TopicTrieNode<V> node, List<String> prefix) {
        if (node.isUserTopic()) {
            List<String> topicLevels = List.copyOf(prefix);
            for (V value : node.values()) {
                long valueId = nextValueId++;
                topicValues.put(valueId, new TopicValue<>(topicLevels, value));
                nativeTrie.add(topicLevels, valueId);
            }
        }
        for (TopicTrieNode<V> child : node.children().values()) {
            prefix.add(child.levelName());
            addTopics(child, prefix);
            prefix.remove(prefix.size() - 1);
        }
    }

    private record TopicValue<V>(List<String> topicLevels, V value) {
    }
}
