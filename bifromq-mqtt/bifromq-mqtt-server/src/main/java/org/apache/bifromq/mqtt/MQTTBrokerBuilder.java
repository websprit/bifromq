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

import com.google.protobuf.Struct;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.baserpc.server.RPCServerBuilder;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.mqtt.service.ILocalDistService;
import org.apache.bifromq.mqtt.service.ILocalSessionRegistry;
import org.apache.bifromq.mqtt.service.ILocalTopicRouter;
import org.apache.bifromq.mqtt.service.LocalDistService;
import org.apache.bifromq.mqtt.service.LocalSessionRegistry;
import org.apache.bifromq.mqtt.service.LocalTopicRouter;
import org.apache.bifromq.plugin.authprovider.IAuthProvider;
import org.apache.bifromq.plugin.clientbalancer.IClientBalancer;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.resourcethrottler.IResourceThrottler;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.retain.client.IRetainClient;
import org.apache.bifromq.sessiondict.client.ISessionDictClient;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@Accessors(fluent = true)
@Setter
public class MQTTBrokerBuilder implements IMQTTBrokerBuilder {
    static final String DEFAULT_LISTENER_ID = "default";
    int connectTimeoutSeconds = 20;
    int connectRateLimit = 1000;
    int disconnectRate = 1000;
    long writeLimit = 512 * 1024;
    long readLimit = 512 * 1024;
    int maxBytesInMessage = 256 * 1024;
    int mqttBossELGThreads;
    int mqttWorkerELGThreads;
    RPCServerBuilder rpcServerBuilder;
    IAuthProvider authProvider;
    IClientBalancer clientBalancer;
    IResourceThrottler resourceThrottler;
    IEventCollector eventCollector;
    ISettingProvider settingProvider;
    IDistClient distClient;
    IInboxClient inboxClient;
    IRetainClient retainClient;
    ISessionDictClient sessionDictClient;
    Map<String, Struct> userPropsCustomizerFactoryConfig = new HashMap<>();

    @Setter(AccessLevel.NONE)
    ILocalSessionRegistry sessionRegistry;
    @Setter(AccessLevel.NONE)
    ILocalDistService distService;
    @Setter(AccessLevel.NONE)
    Map<String, ConnListenerBuilder.TCPConnListenerBuilder> tcpListenerBuilders = new LinkedHashMap<>();
    @Setter(AccessLevel.NONE)
    Map<String, ConnListenerBuilder.TLSConnListenerBuilder> tlsListenerBuilders = new LinkedHashMap<>();
    @Setter(AccessLevel.NONE)
    Map<String, ConnListenerBuilder.WSConnListenerBuilder> wsListenerBuilders = new LinkedHashMap<>();
    @Setter(AccessLevel.NONE)
    Map<String, ConnListenerBuilder.WSSConnListenerBuilder> wssListenerBuilders = new LinkedHashMap<>();
    @Setter(AccessLevel.NONE)
    Map<String, QUICConnListenerBuilder> quicListenerBuilders = new LinkedHashMap<>();

    @Override
    public ConnListenerBuilder.TCPConnListenerBuilder buildTcpConnListener() {
        return buildTcpConnListener(DEFAULT_LISTENER_ID);
    }

    @Override
    public ConnListenerBuilder.TCPConnListenerBuilder buildTcpConnListener(String listenerId) {
        return tcpListenerBuilders.computeIfAbsent(requireListenerId(listenerId),
            id -> new ConnListenerBuilder.TCPConnListenerBuilder(this, id));
    }

    @Override
    public ConnListenerBuilder.TLSConnListenerBuilder buildTLSConnListener() {
        return buildTLSConnListener(DEFAULT_LISTENER_ID);
    }

    @Override
    public ConnListenerBuilder.TLSConnListenerBuilder buildTLSConnListener(String listenerId) {
        return tlsListenerBuilders.computeIfAbsent(requireListenerId(listenerId),
            id -> new ConnListenerBuilder.TLSConnListenerBuilder(this, id));
    }

    @Override
    public ConnListenerBuilder.WSConnListenerBuilder buildWSConnListener() {
        return buildWSConnListener(DEFAULT_LISTENER_ID);
    }

    @Override
    public ConnListenerBuilder.WSConnListenerBuilder buildWSConnListener(String listenerId) {
        return wsListenerBuilders.computeIfAbsent(requireListenerId(listenerId),
            id -> new ConnListenerBuilder.WSConnListenerBuilder(this, id));
    }

    @Override
    public ConnListenerBuilder.WSSConnListenerBuilder buildWSSConnListener() {
        return buildWSSConnListener(DEFAULT_LISTENER_ID);
    }

    @Override
    public ConnListenerBuilder.WSSConnListenerBuilder buildWSSConnListener(String listenerId) {
        return wssListenerBuilders.computeIfAbsent(requireListenerId(listenerId),
            id -> new ConnListenerBuilder.WSSConnListenerBuilder(this, id));
    }

    @Override
    public QUICConnListenerBuilder buildQUICConnListener() {
        return buildQUICConnListener(DEFAULT_LISTENER_ID);
    }

    @Override
    public QUICConnListenerBuilder buildQUICConnListener(String listenerId) {
        return quicListenerBuilders.computeIfAbsent(requireListenerId(listenerId),
            id -> new QUICConnListenerBuilder(this, id));
    }

    public MQTTBrokerBuilder distClient(IDistClient distClient) {
        this.distClient = distClient;
        sessionRegistry = new LocalSessionRegistry();
        ILocalTopicRouter router = new LocalTopicRouter(brokerId(), distClient);
        distService = new LocalDistService(brokerId(), sessionRegistry, router, distClient, resourceThrottler);
        return this;
    }

    @Override
    public String brokerId() {
        return rpcServerBuilder.id();
    }

    public IMQTTBroker build() {
        return new MQTTBroker(this);
    }

    private static String requireListenerId(String listenerId) {
        if (listenerId == null || listenerId.isEmpty()) {
            throw new IllegalArgumentException("listenerId cannot be null or empty");
        }
        return listenerId;
    }
}
