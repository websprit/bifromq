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

package org.apache.bifromq.starter.config.model.mqtt;

import com.fasterxml.jackson.annotation.JsonMerge;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.protobuf.Struct;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.starter.config.model.mqtt.listener.TCPListenerConfig;
import org.apache.bifromq.starter.config.model.mqtt.listener.TLSListenerConfig;
import org.apache.bifromq.starter.config.model.mqtt.listener.WSListenerConfig;
import org.apache.bifromq.starter.config.model.mqtt.listener.WSSListenerConfig;
import org.apache.bifromq.starter.config.model.mqtt.listener.QUICListenerConfig;
import org.apache.bifromq.starter.config.model.serde.StructMapDeserializer;
import org.apache.bifromq.starter.config.model.serde.StructMapSerializer;

@Getter
@Setter
public class MQTTServerConfig {
    public static final String DEFAULT_LISTENER_ID = "default";

    private boolean enable = true;
    private int connTimeoutSec = 20;
    private int maxConnPerSec = 2000;
    private int maxDisconnPerSec = 1000;
    private int maxMsgByteSize = 256 * 1024;
    private int maxConnBandwidth = 512 * 1024;
    private int bossELGThreads = 1;
    private int workerELGThreads = Math.max(2, EnvProvider.INSTANCE.availableProcessors() / 2);

    @JsonSerialize(using = StructMapSerializer.class)
    @JsonDeserialize(using = StructMapDeserializer.class)
    @JsonSetter(nulls = Nulls.SKIP)
    private Map<String, Struct> userPropsCustomizerFactoryConfig = new HashMap<>();
    @JsonSetter(nulls = Nulls.SKIP)
    @JsonMerge
    private TCPListenerConfig tcpListener = new TCPListenerConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    @JsonMerge
    private TLSListenerConfig tlsListener = new TLSListenerConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    @JsonMerge
    private WSListenerConfig wsListener = new WSListenerConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    @JsonMerge
    private WSSListenerConfig wssListener = new WSSListenerConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    @JsonMerge
    private QUICListenerConfig quicListener = new QUICListenerConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    @JsonMerge
    private Map<String, TCPListenerConfig> tcpListeners = new LinkedHashMap<>();
    @JsonSetter(nulls = Nulls.SKIP)
    @JsonMerge
    private Map<String, TLSListenerConfig> tlsListeners = new LinkedHashMap<>();
    @JsonSetter(nulls = Nulls.SKIP)
    @JsonMerge
    private Map<String, WSListenerConfig> wsListeners = new LinkedHashMap<>();
    @JsonSetter(nulls = Nulls.SKIP)
    @JsonMerge
    private Map<String, WSSListenerConfig> wssListeners = new LinkedHashMap<>();
    @JsonSetter(nulls = Nulls.SKIP)
    @JsonMerge
    private Map<String, QUICListenerConfig> quicListeners = new LinkedHashMap<>();

    public Map<String, TCPListenerConfig> effectiveTcpListeners() {
        return tcpListeners.isEmpty() ? legacyTcpListener() : tcpListeners;
    }

    public Map<String, TLSListenerConfig> effectiveTlsListeners() {
        return tlsListeners.isEmpty() ? legacyTlsListener() : tlsListeners;
    }

    public Map<String, WSListenerConfig> effectiveWsListeners() {
        return wsListeners.isEmpty() ? legacyWsListener() : wsListeners;
    }

    public Map<String, WSSListenerConfig> effectiveWssListeners() {
        return wssListeners.isEmpty() ? legacyWssListener() : wssListeners;
    }

    public Map<String, QUICListenerConfig> effectiveQuicListeners() {
        return quicListeners.isEmpty() ? legacyQuicListener() : quicListeners;
    }

    private Map<String, TCPListenerConfig> legacyTcpListener() {
        Map<String, TCPListenerConfig> listeners = new LinkedHashMap<>();
        listeners.put(DEFAULT_LISTENER_ID, tcpListener);
        return listeners;
    }

    private Map<String, TLSListenerConfig> legacyTlsListener() {
        Map<String, TLSListenerConfig> listeners = new LinkedHashMap<>();
        listeners.put(DEFAULT_LISTENER_ID, tlsListener);
        return listeners;
    }

    private Map<String, WSListenerConfig> legacyWsListener() {
        Map<String, WSListenerConfig> listeners = new LinkedHashMap<>();
        listeners.put(DEFAULT_LISTENER_ID, wsListener);
        return listeners;
    }

    private Map<String, WSSListenerConfig> legacyWssListener() {
        Map<String, WSSListenerConfig> listeners = new LinkedHashMap<>();
        listeners.put(DEFAULT_LISTENER_ID, wssListener);
        return listeners;
    }

    private Map<String, QUICListenerConfig> legacyQuicListener() {
        Map<String, QUICListenerConfig> listeners = new LinkedHashMap<>();
        listeners.put(DEFAULT_LISTENER_ID, quicListener);
        return listeners;
    }
}
