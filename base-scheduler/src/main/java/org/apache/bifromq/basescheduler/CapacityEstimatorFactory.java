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

package org.apache.bifromq.basescheduler;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basehookloader.BaseHookLoader;
import org.apache.bifromq.basescheduler.spi.ICapacityEstimator;
import org.apache.bifromq.basescheduler.spi.ICapacityEstimatorFactory;

@Slf4j
class CapacityEstimatorFactory implements ICapacityEstimatorFactory {
    public static final ICapacityEstimatorFactory INSTANCE = new CapacityEstimatorFactory();

    private final ICapacityEstimatorFactory delegate;

    private CapacityEstimatorFactory() {
        Map<String, ICapacityEstimatorFactory> factoryMap = BaseHookLoader.load(ICapacityEstimatorFactory.class);
        if (factoryMap.isEmpty()) {
            delegate = FallbackFactory.INSTANCE;
        } else {
            delegate = factoryMap.values().iterator().next();
            if (factoryMap.size() > 1) {
                log.warn("Multiple CapacityEstimatorFactory implementations found, the first loaded will be used:{}",
                    delegate.getClass().getName());
            }
        }
    }

    @Override
    public <BatcherKey> ICapacityEstimator<BatcherKey> get(String name, BatcherKey batcherKey) {
        try {
            ICapacityEstimator<BatcherKey> estimator = delegate.get(name, batcherKey);
            if (estimator == null) {
                return FallbackFactory.INSTANCE.get(name, batcherKey);
            }
            return estimator;
        } catch (Throwable e) {
            log.error("Failed to create CapacityEstimator: scheduler={}", name, e);
            return FallbackFactory.INSTANCE.get(name, batcherKey);
        }
    }

    @Override
    public void close() {
        delegate.close();
    }

    private static class AdaptiveCapacityEstimator<BatcherKey> implements ICapacityEstimator<BatcherKey> {
        private static final int MAX_INFLIGHT = 65536;
        private static final int MIN_INFLIGHT = 256;
        private static final int INITIAL_INFLIGHT = 4096;
        private static final int MAX_BATCH_SIZE = 10000;
        private static final long LOW_LATENCY_NS = 5_000_000;   // 5ms
        private static final long HIGH_LATENCY_NS = 100_000_000; // 100ms
        private static final double EMA_ALPHA = 0.3;

        private volatile double emaLatency = 0;
        private volatile int maxInflight = INITIAL_INFLIGHT;

        @Override
        public void record(long weightedSize, long latencyNs) {
            if (emaLatency == 0) {
                emaLatency = latencyNs;
            } else {
                emaLatency = EMA_ALPHA * latencyNs + (1 - EMA_ALPHA) * emaLatency;
            }
            if (emaLatency < LOW_LATENCY_NS) {
                maxInflight = Math.min(MAX_INFLIGHT, maxInflight << 1);
            } else if (emaLatency > HIGH_LATENCY_NS) {
                maxInflight = Math.max(MIN_INFLIGHT, maxInflight >> 1);
            }
        }

        @Override
        public boolean hasCapacity(long inflightWeight, BatcherKey key) {
            return inflightWeight < maxInflight;
        }

        @Override
        public long maxCapacity(BatcherKey key) {
            return MAX_BATCH_SIZE;
        }

        @Override
        public void onBackPressure() {
            maxInflight = Math.max(MIN_INFLIGHT, maxInflight >> 1);
        }
    }

    private static class FallbackFactory implements ICapacityEstimatorFactory {
        private static final ICapacityEstimatorFactory INSTANCE = new FallbackFactory();

        @Override
        public <BatcherKey> ICapacityEstimator<BatcherKey> get(String name, BatcherKey batcherKey) {
            return new AdaptiveCapacityEstimator<>();
        }
    }
}
