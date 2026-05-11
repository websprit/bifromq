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

import static org.apache.bifromq.basekv.utils.BoundaryUtil.compare;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.intersect;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.isNULLRange;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.buildMatchRoute;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.tenantBeginKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.tenantRouteStartKey;

import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Timer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.basekv.store.api.IKVRangeRefreshableReader;
import org.apache.bifromq.dist.trie.ITopicFilterIterator;
import org.apache.bifromq.dist.trie.ThreadLocalTopicFilterIterator;
import org.apache.bifromq.dist.trie.TopicTrieNode;
import org.apache.bifromq.dist.worker.schema.cache.GroupMatching;
import org.apache.bifromq.dist.worker.schema.cache.Matching;
import org.apache.bifromq.dist.worker.schema.cache.NormalMatching;
import org.apache.bifromq.native_binding.NativeLoader;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.sysprops.props.NativeTenantRouteMatcherBatchEnabled;
import org.apache.bifromq.sysprops.props.NativeTenantRouteMatcherEnabled;
import org.apache.bifromq.type.RouteMatcher;
import org.apache.bifromq.util.TopicUtil;

@Slf4j
class TenantRouteMatcher implements ITenantRouteMatcher {
    private static volatile boolean nativeTenantRouteMatcherAvailable =
        NativeLoader.isAvailable() && NativeTenantRouteMatcherEnabled.INSTANCE.get();
    private static volatile boolean nativeTenantRouteMatcherBatchAvailable =
        NativeLoader.isAvailable() && NativeTenantRouteMatcherBatchEnabled.INSTANCE.get();

    private final String tenantId;
    private final Timer timer;
    private final Supplier<IKVRangeRefreshableReader> kvReaderSupplier;
    private final IEventCollector eventCollector;
    private final Executor indexBuildExecutor;
    private final List<PendingRouteUpdate> pendingRouteUpdates = new ArrayList<>();
    private NativeSubscriptionRouteIndex nativeRouteIndex;
    private CompletableFuture<Void> nativeRouteIndexBuildTask;
    private boolean nativeRouteIndexInitialized;
    private boolean closed;

    public TenantRouteMatcher(String tenantId,
                              Supplier<IKVRangeRefreshableReader> kvReaderSupplier,
                              IEventCollector eventCollector,
                              Timer timer) {
        this(tenantId, kvReaderSupplier, eventCollector, timer, Runnable::run);
    }

    public TenantRouteMatcher(String tenantId,
                              Supplier<IKVRangeRefreshableReader> kvReaderSupplier,
                              IEventCollector eventCollector,
                              Timer timer,
                              Executor indexBuildExecutor) {
        this.tenantId = tenantId;
        this.timer = timer;
        this.kvReaderSupplier = kvReaderSupplier;
        this.eventCollector = eventCollector;
        this.indexBuildExecutor = indexBuildExecutor;
    }

    @Override
    public Map<String, IMatchedRoutes> matchAll(Set<String> topics,
                                                int maxPersistentFanoutCount,
                                                int maxGroupFanoutCount) {
        final Timer.Sample sample = Timer.start();
        Map<String, IMatchedRoutes> matchedRoutes =
            newMatchedRoutes(topics, maxPersistentFanoutCount, maxGroupFanoutCount);

        try (IKVRangeRefreshableReader rangeReader = kvReaderSupplier.get()) {
            ByteString tenantStartKey = tenantBeginKey(tenantId);
            Boundary tenantBoundary =
                intersect(toBoundary(tenantStartKey, upperBound(tenantStartKey)), rangeReader.boundary());
            if (isNULLRange(tenantBoundary)) {
                return matchedRoutes;
            }
            if (nativeTenantRouteMatcherAvailable) {
                try {
                    if (nativeRouteIndexReady()) {
                        matchAllWithNativeRouteIndex(matchedRoutes, topics,
                            maxPersistentFanoutCount, maxGroupFanoutCount);
                        sample.stop(timer);
                        return matchedRoutes;
                    }
                    triggerNativeRouteIndexBuild(tenantBoundary);
                    try (IExpansionMatcher expansionMatcher = new JavaExpansionMatcher(topics);
                         IKVIterator itr = rangeReader.iterator(tenantBoundary)) {
                        matchAllWithExpansionMatcher(matchedRoutes, expansionMatcher, itr, tenantBoundary,
                            maxPersistentFanoutCount, maxGroupFanoutCount);
                        sample.stop(timer);
                        return matchedRoutes;
                    }
                } catch (Throwable e) {
                    nativeTenantRouteMatcherAvailable = false;
                    closeNativeRouteIndex();
                    log.warn("Native tenant route index unavailable, using fallback: {}", e.getMessage());
                }
            }
            if (nativeTenantRouteMatcherBatchAvailable) {
                try {
                    Map<String, IMatchedRoutes> nativeMatchedRoutes =
                        newMatchedRoutes(topics, maxPersistentFanoutCount, maxGroupFanoutCount);
                    try (IExpansionMatcher expansionMatcher = new NativeExpansionMatcher(topics);
                         IKVIterator itr = rangeReader.iterator(tenantBoundary)) {
                        matchAllWithExpansionMatcher(nativeMatchedRoutes, expansionMatcher, itr, tenantBoundary,
                            maxPersistentFanoutCount, maxGroupFanoutCount);
                    }
                    sample.stop(timer);
                    return nativeMatchedRoutes;
                } catch (Throwable e) {
                    nativeTenantRouteMatcherBatchAvailable = false;
                    log.warn("Native tenant route matcher seek unavailable, using fallback: {}", e.getMessage());
                }
            }
            try (IExpansionMatcher expansionMatcher = newExpansionMatcher(topics);
                 IKVIterator itr = rangeReader.iterator(tenantBoundary)) {
                matchAllWithExpansionMatcher(matchedRoutes, expansionMatcher, itr, tenantBoundary,
                    maxPersistentFanoutCount, maxGroupFanoutCount);
                sample.stop(timer);
                return matchedRoutes;
            }
        }
    }

    @Override
    public synchronized void addRoutes(NavigableMap<RouteMatcher, Set<Matching>> routes) {
        if (!nativeRouteIndexInitialized) {
            if (nativeRouteIndexBuildTask != null && !nativeRouteIndexBuildTask.isDone()) {
                pendingRouteUpdates.add(PendingRouteUpdate.add(routes));
            }
            return;
        }
        applyAddRoutes(routes, nativeRouteIndex);
    }

    @Override
    public synchronized void removeRoutes(NavigableMap<RouteMatcher, Set<Matching>> routes) {
        if (!nativeRouteIndexInitialized) {
            if (nativeRouteIndexBuildTask != null && !nativeRouteIndexBuildTask.isDone()) {
                pendingRouteUpdates.add(PendingRouteUpdate.remove(routes));
            }
            return;
        }
        applyRemoveRoutes(routes, nativeRouteIndex);
    }

    @Override
    public synchronized void close() {
        closed = true;
        closeNativeRouteIndex();
    }

    private synchronized boolean nativeRouteIndexReady() {
        return nativeRouteIndexInitialized;
    }

    private synchronized void triggerNativeRouteIndexBuild(Boundary tenantBoundary) {
        if (nativeRouteIndexInitialized || closed
            || nativeRouteIndexBuildTask != null && !nativeRouteIndexBuildTask.isDone()) {
            return;
        }
        Boundary buildBoundary = tenantBoundary.toBuilder().build();
        nativeRouteIndexBuildTask = CompletableFuture.runAsync(() -> buildNativeRouteIndex(buildBoundary),
            indexBuildExecutor).whenComplete((v, e) -> {
            if (e != null) {
                nativeTenantRouteMatcherAvailable = false;
                log.warn("Native tenant route index build failed, using fallback: {}", e.getMessage());
            }
        });
    }

    private void buildNativeRouteIndex(Boundary tenantBoundary) {
        NativeSubscriptionRouteIndex routeIndex = new NativeSubscriptionRouteIndex();
        try (IKVRangeRefreshableReader rangeReader = kvReaderSupplier.get();
             IKVIterator itr = rangeReader.iterator(tenantBoundary)) {
            itr.seek(tenantBoundary.getStartKey());
            while (itr.isValid() && compare(itr.key(), tenantBoundary.getEndKey()) < 0) {
                routeIndex.add(buildMatchRoute(itr.key(), itr.value()));
                itr.next();
            }
        } catch (Throwable e) {
            routeIndex.close();
            throw e;
        }
        installNativeRouteIndex(routeIndex);
    }

    private synchronized void installNativeRouteIndex(NativeSubscriptionRouteIndex routeIndex) {
        if (closed || nativeRouteIndexInitialized) {
            routeIndex.close();
            return;
        }
        for (PendingRouteUpdate update : pendingRouteUpdates) {
            update.apply(routeIndex);
        }
        pendingRouteUpdates.clear();
        nativeRouteIndex = routeIndex;
        nativeRouteIndexInitialized = true;
    }

    private synchronized void matchAllWithNativeRouteIndex(Map<String, IMatchedRoutes> matchedRoutes,
                                                           Set<String> topics,
                                                           int maxPersistentFanoutCount,
                                                           int maxGroupFanoutCount) {
        for (String topic : topics) {
            for (Matching matching : nativeRouteIndex.match(topic)) {
                addMatching(matchedRoutes, topic, matching, maxPersistentFanoutCount, maxGroupFanoutCount);
            }
        }
    }

    private synchronized void closeNativeRouteIndex() {
        if (nativeRouteIndex != null) {
            nativeRouteIndex.close();
            nativeRouteIndex = null;
        }
        pendingRouteUpdates.clear();
        nativeRouteIndexInitialized = false;
    }

    private static void applyAddRoutes(NavigableMap<RouteMatcher, Set<Matching>> routes,
                                       NativeSubscriptionRouteIndex routeIndex) {
        routes.values().forEach(matchings -> matchings.forEach(routeIndex::add));
    }

    private static void applyRemoveRoutes(NavigableMap<RouteMatcher, Set<Matching>> routes,
                                          NativeSubscriptionRouteIndex routeIndex) {
        for (Set<Matching> matchings : routes.values()) {
            for (Matching matching : matchings) {
                if (matching.type() == Matching.Type.Group) {
                    GroupMatching groupMatching = (GroupMatching) matching;
                    routeIndex.removeGroup(groupMatching);
                    if (!groupMatching.receivers().isEmpty()) {
                        routeIndex.add(groupMatching);
                    }
                } else {
                    routeIndex.remove(matching);
                }
            }
        }
    }

    private Map<String, IMatchedRoutes> newMatchedRoutes(Set<String> topics,
                                                          int maxPersistentFanoutCount,
                                                          int maxGroupFanoutCount) {
        Map<String, IMatchedRoutes> matchedRoutes = new HashMap<>();
        topics.forEach(topic -> matchedRoutes.put(topic,
            new MatchedRoutes(tenantId, topic, eventCollector, maxPersistentFanoutCount, maxGroupFanoutCount)));
        return matchedRoutes;
    }

    private void matchAllWithExpansionMatcher(Map<String, IMatchedRoutes> matchedRoutes,
                                               IExpansionMatcher expansionMatcher,
                                               IKVIterator itr,
                                               Boundary tenantBoundary,
                                               int maxPersistentFanoutCount,
                                               int maxGroupFanoutCount) {
        Map<List<String>, Set<String>> matchedTopicFilters = new HashMap<>();
        itr.seek(tenantBoundary.getStartKey());
        int probe = 0;
        while (itr.isValid() && compare(itr.key(), tenantBoundary.getEndKey()) < 0) {
            Matching matching = buildMatchRoute(itr.key(), itr.value());
            List<String> seekTopicFilter = matching.matcher.getFilterLevelList();
            if (matchedTopicFilters.containsKey(seekTopicFilter)) {
                Set<String> matchedTopics = matchedTopicFilters.get(seekTopicFilter);
                itr.next();
                for (String topic : matchedTopics) {
                    addMatching(matchedRoutes, topic, matching,
                        maxPersistentFanoutCount, maxGroupFanoutCount);
                }
                continue;
            }
            expansionMatcher.seek(seekTopicFilter);
            if (expansionMatcher.isValid()) {
                List<String> topicFilterToMatch = expansionMatcher.key();
                if (topicFilterToMatch.equals(seekTopicFilter)) {
                    Set<String> backingTopics = expansionMatcher.topics();
                    for (String topic : backingTopics) {
                        addMatching(matchedRoutes, topic, matching,
                            maxPersistentFanoutCount, maxGroupFanoutCount);
                    }
                    matchedTopicFilters.put(seekTopicFilter, backingTopics);
                    itr.next();
                    probe = 0;
                } else {
                    if (probe++ < 20) {
                        itr.next();
                    } else {
                        ByteString nextMatch = tenantRouteStartKey(tenantId, topicFilterToMatch);
                        itr.seek(nextMatch);
                    }
                }
            } else if (expansionMatcher.noMoreMatches()) {
                break;
            } else {
                matchedTopicFilters.put(seekTopicFilter, Set.of());
                itr.next();
            }
        }
    }

    private IExpansionMatcher newExpansionMatcher(Set<String> topics) {
        if (nativeTenantRouteMatcherAvailable) {
            try {
                return new NativeExpansionMatcher(topics);
            } catch (Throwable e) {
                nativeTenantRouteMatcherAvailable = false;
                log.warn("Native tenant route matcher unavailable, using Java fallback: {}", e.getMessage());
            }
        }
        return new JavaExpansionMatcher(topics);
    }

    private void addMatching(Map<String, IMatchedRoutes> matchedRoutes,
                             String topic,
                             Matching matching,
                             int maxPersistentFanoutCount,
                             int maxGroupFanoutCount) {
        MatchedRoutes matchResult = (MatchedRoutes) matchedRoutes.computeIfAbsent(topic,
            k -> new MatchedRoutes(tenantId, k, eventCollector, maxPersistentFanoutCount, maxGroupFanoutCount));
        switch (matching.type()) {
            case Normal -> matchResult.addNormalMatching((NormalMatching) matching);
            case Group -> matchResult.putGroupMatching((GroupMatching) matching);
            default -> {
            }
        }
    }

    private record PendingRouteUpdate(boolean add, NavigableMap<RouteMatcher, Set<Matching>> routes) {
        static PendingRouteUpdate add(NavigableMap<RouteMatcher, Set<Matching>> routes) {
            return new PendingRouteUpdate(true, routes);
        }

        static PendingRouteUpdate remove(NavigableMap<RouteMatcher, Set<Matching>> routes) {
            return new PendingRouteUpdate(false, routes);
        }

        void apply(NativeSubscriptionRouteIndex routeIndex) {
            if (add) {
                applyAddRoutes(routes, routeIndex);
            } else {
                applyRemoveRoutes(routes, routeIndex);
            }
        }
    }

    private interface IExpansionMatcher extends AutoCloseable {
        void seek(List<String> filterLevels);

        boolean isValid();

        List<String> key();

        Set<String> topics();

        boolean noMoreMatches();

        @Override
        void close();
    }

    private static final class JavaExpansionMatcher implements IExpansionMatcher {
        private final ITopicFilterIterator<String> iterator;

        JavaExpansionMatcher(Set<String> topics) {
            TopicTrieNode.Builder<String> topicTrieBuilder = TopicTrieNode.builder(false);
            topics.forEach(topic -> topicTrieBuilder.addTopic(TopicUtil.parse(topic, false), topic));
            iterator = ThreadLocalTopicFilterIterator.get(topicTrieBuilder.build());
        }

        @Override
        public void seek(List<String> filterLevels) {
            iterator.seek(filterLevels);
        }

        @Override
        public boolean isValid() {
            return iterator.isValid();
        }

        @Override
        public List<String> key() {
            return iterator.key();
        }

        @Override
        public Set<String> topics() {
            Set<String> topics = new HashSet<>();
            for (Set<String> topicSet : iterator.value().values()) {
                topics.addAll(topicSet);
            }
            return topics;
        }

        @Override
        public boolean noMoreMatches() {
            return !iterator.isValid();
        }

        @Override
        public void close() {
            iterator.close();
        }
    }

    private static final class NativeExpansionMatcher implements IExpansionMatcher {
        private final Set<String> topics;
        private final NativeTenantTopicMatcher matcher;
        private JavaExpansionMatcher fallback;
        private NativeTenantTopicMatcher.MatchResult current;

        NativeExpansionMatcher(Set<String> topics) {
            this.topics = topics;
            matcher = new NativeTenantTopicMatcher(topics);
        }

        @Override
        public void seek(List<String> filterLevels) {
            current = null;
            if (fallback != null) {
                fallback.seek(filterLevels);
                if (fallback.isValid() && fallback.key().equals(filterLevels)) {
                    current = matcher.match(filterLevels);
                }
                return;
            }
            current = matcher.match(filterLevels);
            if (current == null) {
                fallback().seek(filterLevels);
            }
        }

        @Override
        public boolean isValid() {
            return current != null || fallback != null && fallback.isValid();
        }

        @Override
        public List<String> key() {
            return current != null ? current.key() : fallback.key();
        }

        @Override
        public Set<String> topics() {
            return current != null ? current.topics() : fallback.topics();
        }

        @Override
        public boolean noMoreMatches() {
            return current == null && fallback != null && fallback.noMoreMatches();
        }

        @Override
        public void close() {
            if (fallback != null) {
                fallback.close();
            }
            matcher.close();
        }

        private JavaExpansionMatcher fallback() {
            if (fallback == null) {
                fallback = new JavaExpansionMatcher(topics);
            }
            return fallback;
        }
    }
}
