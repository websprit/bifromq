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

package org.apache.bifromq.sysprops.props;

import org.apache.bifromq.sysprops.BifroMQSysProp;
import org.apache.bifromq.sysprops.parser.BooleanParser;

/**
 * Controls whether native (Rust FFM) engine is used for topic matching and KV
 * encoding/decoding.
 * <p>
 * When set to {@code true} (default), the native Rust implementation is used if
 * the native library
 * is available. When set to {@code false}, pure Java implementation is used.
 * <p>
 * Can be controlled at deployment time via Helm chart:
 * {@code nativeEngine.enabled}
 * or via JVM system property: {@code -Dnative_engine_enabled=false}
 */
public class NativeEngineEnabled extends BifroMQSysProp<Boolean, BooleanParser> {
    public static final NativeEngineEnabled INSTANCE = new NativeEngineEnabled();

    private NativeEngineEnabled() {
        super("native_engine_enabled", true, BooleanParser.INSTANCE);
    }
}
