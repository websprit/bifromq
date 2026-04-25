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
import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_RANGE_BOUND_BYTES;
import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_STATE_BYTES;
import static org.apache.bifromq.basekv.store.range.KVRangeKeys.METADATA_VER_BYTES;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.NULL_BOUNDARY;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceReader;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.store.api.IKVRangeReader;
import org.apache.bifromq.basekv.store.api.IKVRangeRefreshableReader;
import org.apache.bifromq.basekv.store.util.KVUtil;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

class KVRange implements IKVRange {
    private final KVRangeId id;
    private final ICPableKVSpace kvSpace;
    private final Logger logger;
    private final BehaviorSubject<Long> versionSubject;
    private final BehaviorSubject<State> stateSubject;
    private final BehaviorSubject<ClusterConfig> clusterConfigSubject;
    private final BehaviorSubject<Boundary> boundarySubject;
    private final BehaviorSubject<Long> lastAppliedIndexSubject;
    private final Disposable disposable;

    // volatile metadata caches to avoid RxJava subscription overhead on hot paths
    private volatile long cachedVer = -1L;
    private volatile State cachedState = State.newBuilder().setType(State.StateType.NoUse).build();
    private volatile ClusterConfig cachedClusterConfig = ClusterConfig.getDefaultInstance();
    private volatile Boundary cachedBoundary = NULL_BOUNDARY;
    private volatile long cachedLastAppliedIndex = -1L;

    // Hot-key read cache: Caffeine with 1s TTL, max 1024 entries
    private final Cache<ByteString, Optional<ByteString>> readCache;

    // Key invalidation callback consumed by writers
    private final Consumer<ByteString> cacheInvalidator;
    private final Runnable cacheFullInvalidator;

    KVRange(KVRangeId id, ICPableKVSpace kvSpace, String... tags) {
        this.id = id;
        this.kvSpace = kvSpace;
        this.logger = MDCLogger.getLogger(KVRange.class, tags);
        versionSubject = BehaviorSubject.createDefault(-1L);
        stateSubject = BehaviorSubject.createDefault(
            State.newBuilder().setType(State.StateType.NoUse).build());
        clusterConfigSubject = BehaviorSubject.createDefault(ClusterConfig.getDefaultInstance());
        boundarySubject = BehaviorSubject.createDefault(NULL_BOUNDARY);
        lastAppliedIndexSubject = BehaviorSubject.createDefault(-1L);
        disposable = kvSpace.metadata().subscribe(this::onMetadataChanged);
        readCache = Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofSeconds(1))
            .maximumSize(1024)
            .build();
        cacheInvalidator = readCache::invalidate;
        cacheFullInvalidator = readCache::invalidateAll;
    }

    public KVRange(KVRangeId id, ICPableKVSpace kvSpace, KVRangeSnapshot snapshot, String... tags) {
        this(id, kvSpace, tags);
        startRestore(snapshot, IKVRangeRestoreSession.IKVRestoreProgressListener.NOOP).done();
    }

    @Override
    public KVRangeId id() {
        return id;
    }

    @Override
    public Observable<Long> ver() {
        return versionSubject.distinctUntilChanged();
    }

    @Override
    public long currentVer() {
        return cachedVer;
    }

    @Override
    public Observable<State> state() {
        return stateSubject.distinctUntilChanged();
    }

    @Override
    public State currentState() {
        return cachedState;
    }

    @Override
    public Observable<ClusterConfig> clusterConfig() {
        return clusterConfigSubject.distinctUntilChanged();
    }

    @Override
    public ClusterConfig currentClusterConfig() {
        return cachedClusterConfig;
    }

    @Override
    public Observable<Boundary> boundary() {
        return boundarySubject.distinctUntilChanged();
    }

    @Override
    public Boundary currentBoundary() {
        return cachedBoundary;
    }

    @Override
    public Observable<Long> lastAppliedIndex() {
        return lastAppliedIndexSubject.distinctUntilChanged();
    }

    @Override
    public long currentLastAppliedIndex() {
        return cachedLastAppliedIndex;
    }

    @Override
    public KVRangeSnapshot checkpoint() {
        String checkpointId = kvSpace.checkpoint();
        try (IKVRangeReader checkpointReader = new KVRangeReader(
            kvSpace.openCheckpoint(checkpointId).get().newReader())) {
            KVRangeSnapshot.Builder builder = KVRangeSnapshot.newBuilder()
                .setVer(checkpointReader.version())
                .setId(id)
                .setCheckpointId(checkpointId)
                .setLastAppliedIndex(checkpointReader.lastAppliedIndex())
                .setState(checkpointReader.state())
                .setBoundary(checkpointReader.boundary())
                .setClusterConfig(checkpointReader.clusterConfig());
            return builder.build();
        }
    }

    @Override
    public boolean hasCheckpoint(KVRangeSnapshot checkpoint) {
        assert checkpoint.getId().equals(id);
        return checkpoint.hasCheckpointId() && kvSpace.openCheckpoint(checkpoint.getCheckpointId()).isPresent();
    }

    @Override
    public IKVRangeReader open(KVRangeSnapshot checkpoint) {
        return new KVRangeReader(kvSpace.openCheckpoint(checkpoint.getCheckpointId()).get().newReader());
    }

    @Override
    public IKVRangeRefreshableReader newReader() {
        return new CachingKVRangeReader(kvSpace.reader(), readCache);
    }

    @Override
    public IKVRangeWriter<?> toWriter() {
        return new KVRangeWriter(id, kvSpace, cacheFullInvalidator);
    }

    @Override
    public IKVRangeWriter<?> toWriter(IKVLoadRecorder recorder) {
        return new LoadRecordableKVRangeWriter(id, kvSpace, recorder, cacheFullInvalidator);
    }

    @Override
    public IKVRangeRestoreSession startRestore(KVRangeSnapshot snapshot,
                                               IKVRangeRestoreSession.IKVRestoreProgressListener progressListener) {
        return new KVRangeRestoreSession(kvSpace.startRestore(progressListener::onProgress))
            .ver(snapshot.getVer())
            .lastAppliedIndex(snapshot.getLastAppliedIndex())
            .state(snapshot.getState())
            .boundary(snapshot.getBoundary())
            .clusterConfig(snapshot.getClusterConfig());
    }

    @Override
    public long size() {
        return kvSpace.size();
    }

    @Override
    public void close() {
        disposable.dispose();
        versionSubject.onComplete();
        stateSubject.onComplete();
        clusterConfigSubject.onComplete();
        boundarySubject.onComplete();
        lastAppliedIndexSubject.onComplete();
        kvSpace.close();
    }

    @Override
    public void destroy() {
        kvSpace.destroy();
    }

    private void onMetadataChanged(Map<ByteString, ByteString> metadata) {
        updateVersion(metadata.get(METADATA_VER_BYTES));
        updateState(metadata.get(METADATA_STATE_BYTES));
        updateClusterConfig(metadata.get(METADATA_CLUSTER_CONFIG_BYTES));
        updateBoundary(metadata.get(METADATA_RANGE_BOUND_BYTES));
        updateLastAppliedIndex(metadata.get(KVRangeKeys.METADATA_LAST_APPLIED_INDEX_BYTES));
    }

    private void updateVersion(ByteString versionBytes) {
        if (versionBytes != null) {
            long ver = KVUtil.toLongNativeOrder(versionBytes);
            cachedVer = ver;
            versionSubject.onNext(ver);
        }
    }

    private void updateState(ByteString stateBytes) {
        if (stateBytes != null) {
            try {
                State state = State.parseFrom(stateBytes);
                cachedState = state;
                stateSubject.onNext(state);
            } catch (Throwable e) {
                logger.warn("Failed to parse state from bytes", e);
            }
        }
    }

    private void updateClusterConfig(ByteString clusterConfigBytes) {
        if (clusterConfigBytes != null) {
            try {
                ClusterConfig clusterConfig = ClusterConfig.parseFrom(clusterConfigBytes);
                cachedClusterConfig = clusterConfig;
                clusterConfigSubject.onNext(clusterConfig);
            } catch (Throwable e) {
                logger.warn("Failed to parse cluster config from bytes", e);
            }
        }
    }

    private void updateBoundary(ByteString boundaryBytes) {
        if (boundaryBytes != null) {
            try {
                Boundary boundary = Boundary.parseFrom(boundaryBytes);
                cachedBoundary = boundary;
                boundarySubject.onNext(boundary);
            } catch (Throwable e) {
                logger.warn("Failed to parse boundary from bytes", e);
            }
        }
    }

    private void updateLastAppliedIndex(ByteString lastAppliedIndexBytes) {
        if (lastAppliedIndexBytes != null) {
            long idx = KVUtil.toLong(lastAppliedIndexBytes);
            cachedLastAppliedIndex = idx;
            lastAppliedIndexSubject.onNext(idx);
        }
    }
}
