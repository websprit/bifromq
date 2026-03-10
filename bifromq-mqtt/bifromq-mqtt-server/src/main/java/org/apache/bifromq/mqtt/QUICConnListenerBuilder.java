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

package org.apache.bifromq.mqtt;

import com.google.common.base.Preconditions;
import io.netty.incubator.codec.quic.QuicSslContext;
import java.util.concurrent.TimeUnit;
import lombok.NonNull;

/**
 * Builder for QUIC connection listener configuration.
 */
public final class QUICConnListenerBuilder {
    private final MQTTBrokerBuilder serverBuilder;
    private String host;
    private int port = 14567;
    private QuicSslContext sslContext;
    private long maxIdleTimeoutMs = 30_000;
    private long initialMaxData = 10_000_000;
    private long initialMaxStreamDataBidiLocal = 1_000_000;
    private long initialMaxStreamDataBidiRemote = 1_000_000;
    private long initialMaxStreamsBidi = 100;

    QUICConnListenerBuilder(MQTTBrokerBuilder builder) {
        this.serverBuilder = builder;
    }

    public QUICConnListenerBuilder host(String host) {
        Preconditions.checkArgument(host != null, "host can't be null");
        this.host = host;
        return this;
    }

    public QUICConnListenerBuilder port(int port) {
        Preconditions.checkArgument(port > 0, "port must be positive");
        this.port = port;
        return this;
    }

    public QUICConnListenerBuilder sslContext(@NonNull QuicSslContext sslContext) {
        this.sslContext = sslContext;
        return this;
    }

    public QUICConnListenerBuilder maxIdleTimeout(long timeout, TimeUnit unit) {
        this.maxIdleTimeoutMs = unit.toMillis(timeout);
        return this;
    }

    public QUICConnListenerBuilder initialMaxData(long initialMaxData) {
        this.initialMaxData = initialMaxData;
        return this;
    }

    public QUICConnListenerBuilder initialMaxStreamDataBidiLocal(long value) {
        this.initialMaxStreamDataBidiLocal = value;
        return this;
    }

    public QUICConnListenerBuilder initialMaxStreamDataBidiRemote(long value) {
        this.initialMaxStreamDataBidiRemote = value;
        return this;
    }

    public QUICConnListenerBuilder initialMaxStreamsBidi(long value) {
        this.initialMaxStreamsBidi = value;
        return this;
    }

    public MQTTBrokerBuilder buildListener() {
        return serverBuilder;
    }

    // Getters for MQTTBroker to consume
    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public QuicSslContext sslContext() {
        return sslContext;
    }

    public long maxIdleTimeoutMs() {
        return maxIdleTimeoutMs;
    }

    public long initialMaxData() {
        return initialMaxData;
    }

    public long initialMaxStreamDataBidiLocal() {
        return initialMaxStreamDataBidiLocal;
    }

    public long initialMaxStreamDataBidiRemote() {
        return initialMaxStreamDataBidiRemote;
    }

    public long initialMaxStreamsBidi() {
        return initialMaxStreamsBidi;
    }
}
