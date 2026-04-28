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

package org.apache.bifromq.basekv.store.range;

import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_CLUSTER_CONFIG_BYTES;
import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_LAST_APPLIED_INDEX_BYTES;
import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_RANGE_BOUND_BYTES;
import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_STATE_BYTES;
import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_VER_BYTES;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.protobuf.ByteString;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.localengine.IKVSpaceRefreshableReader;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.basekv.store.api.IKVRangeRefreshableReader;
import org.apache.bifromq.basekv.store.util.KVUtil;

/**
 * A IKVRangeRefreshableReader implementation that caches get/exist results for hot keys.
 * Cache entries expire after a short TTL and are invalidated on write.
 */
@Slf4j
class CachingKVRangeReader implements IKVRangeRefreshableReader {
    private final IKVSpaceRefreshableReader kvSpaceReader;
    private final Cache<ByteString, Optional<ByteString>> readCache;

    CachingKVRangeReader(IKVSpaceRefreshableReader spaceReader,
                          Cache<ByteString, Optional<ByteString>> readCache) {
        this.kvSpaceReader = spaceReader;
        this.readCache = readCache;
    }

    @Override
    public long version() {
        return kvSpaceReader.metadata(METADATA_VER_BYTES).map(KVUtil::toLongNativeOrder).orElse(-1L);
    }

    @Override
    public State state() {
        return kvSpaceReader.metadata(METADATA_STATE_BYTES)
            .map(stateBytes -> {
                try {
                    return State.parseFrom(stateBytes);
                } catch (Throwable e) {
                    log.warn("Failed to parse KVRange state from metadata", e);
                    return State.newBuilder().setType(State.StateType.NoUse).build();
                }
            })
            .orElse(State.newBuilder().setType(State.StateType.NoUse).build());
    }

    @Override
    public long lastAppliedIndex() {
        return kvSpaceReader.metadata(METADATA_LAST_APPLIED_INDEX_BYTES).map(KVUtil::toLong).orElse(-1L);
    }

    @Override
    public Boundary boundary() {
        return kvSpaceReader.metadata(METADATA_RANGE_BOUND_BYTES)
            .map(boundaryBytes -> {
                try {
                    return Boundary.parseFrom(boundaryBytes);
                } catch (Throwable e) {
                    log.warn("Failed to parse KVRange boundary from metadata", e);
                    return Boundary.getDefaultInstance();
                }
            })
            .orElse(Boundary.getDefaultInstance());
    }

    @Override
    public ClusterConfig clusterConfig() {
        return kvSpaceReader.metadata(METADATA_CLUSTER_CONFIG_BYTES)
            .map(clusterConfigBytes -> {
                try {
                    return ClusterConfig.parseFrom(clusterConfigBytes);
                } catch (Throwable e) {
                    log.warn("Failed to parse KVRange cluster config from metadata", e);
                    return ClusterConfig.getDefaultInstance();
                }
            })
            .orElse(ClusterConfig.getDefaultInstance());
    }

    @Override
    public long size(Boundary boundary) {
        return kvSpaceReader.size(boundary);
    }

    @Override
    public boolean exist(ByteString key) {
        Optional<ByteString> cached = readCache.getIfPresent(key);
        if (cached != null) {
            return cached.isPresent();
        }
        return kvSpaceReader.exist(key);
    }

    @Override
    public Optional<ByteString> get(ByteString key) {
        Optional<ByteString> cached = readCache.getIfPresent(key);
        if (cached != null) {
            return cached;
        }
        Optional<ByteString> result = kvSpaceReader.get(key);
        readCache.put(key, result);
        return result;
    }

    @Override
    public ByteString getDirect(ByteString key) {
        return kvSpaceReader.getDirect(key);
    }

    @Override
    public IKVIterator iterator() {
        return new KVIterator(kvSpaceReader.newIterator());
    }

    @Override
    public IKVIterator iterator(Boundary boundary) {
        return new KVIterator(kvSpaceReader.newIterator(boundary));
    }

    @Override
    public void refresh() {
        readCache.invalidateAll();
        kvSpaceReader.refresh();
        readCache.invalidateAll();
    }

    @Override
    public void close() {
        kvSpaceReader.close();
    }
}
