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

import java.util.NavigableMap;
import java.util.Map;
import java.util.Set;
import org.apache.bifromq.dist.worker.schema.cache.Matching;
import org.apache.bifromq.type.RouteMatcher;

/**
 * The interface for tenant route matcher.
 */
public interface ITenantRouteMatcher {
    /**
     * Match the topic within the given boundary.
     *
     * @param topics the topics to match
     * @param maxPersistentFanoutCount the maximum number of persistent fanout matches to return
     * @param maxGroupFanoutCount the maximum number of group fanout matches to return
     * @return the result
     */
    Map<String, IMatchedRoutes> matchAll(Set<String> topics, int maxPersistentFanoutCount, int maxGroupFanoutCount);

    default void addRoutes(NavigableMap<RouteMatcher, Set<Matching>> routes) {
    }

    default void removeRoutes(NavigableMap<RouteMatcher, Set<Matching>> routes) {
    }

    default void close() {
    }
}
