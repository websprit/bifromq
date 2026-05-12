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

package org.apache.bifromq.mqtt.integration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.mqtt.MQTTBrokerBuilder;
import org.apache.bifromq.mqtt.integration.v3.client.MqttTestClient;
import org.apache.bifromq.plugin.authprovider.IAuthProvider;
import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.authprovider.type.Granted;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthResult;
import org.apache.bifromq.plugin.authprovider.type.Ok;
import org.apache.bifromq.plugin.authprovider.type.Success;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class MQTTNamedTCPListenerTest extends MQTTTest {
    private static final String EDGE_URI = "tcp://127.0.0.1:18830";

    @Mock
    private IAuthProvider edgeAuthProvider;

    @Override
    protected void customizeMQTTBrokerBuilder(MQTTBrokerBuilder brokerBuilder) {
        brokerBuilder.buildTcpConnListener("edge")
            .host("127.0.0.1")
            .port(18830)
            .buildListener();
    }

    @Test(groups = "integration")
    public void mqtt3ConnectionsCarryListenerIdentityAndUseScopedAuthProvider() {
        stubMQTT3AuthProviders();
        MqttTestClient defaultClient = new MqttTestClient(BROKER_URI, "default-v3");
        MqttTestClient edgeClient = new MqttTestClient(EDGE_URI, "edge-v3");
        try {
            MqttConnectOptions connectOptions = new MqttConnectOptions();
            connectOptions.setCleanSession(true);

            defaultClient.connect(connectOptions);
            edgeClient.connect(connectOptions);

            verify(authProvider).auth(argThat((MQTT3AuthData authData) ->
                authData.getListenerId().equals("default")
                    && authData.getTransportType().equals("TCP")));
            verify(edgeAuthProvider).auth(argThat((MQTT3AuthData authData) ->
                authData.getListenerId().equals("edge")
                    && authData.getTransportType().equals("TCP")));
        } finally {
            if (defaultClient.isConnected()) {
                defaultClient.disconnect();
            }
            if (edgeClient.isConnected()) {
                edgeClient.disconnect();
            }
            defaultClient.close();
            edgeClient.close();
        }
    }

    @Test(groups = "integration")
    public void mqtt5ConnectionsCarryListenerIdentityAndUseScopedAuthProvider() {
        stubMQTT5AuthProviders();
        org.apache.bifromq.mqtt.integration.v5.client.MqttTestClient defaultClient =
            new org.apache.bifromq.mqtt.integration.v5.client.MqttTestClient(BROKER_URI, "default-v5");
        org.apache.bifromq.mqtt.integration.v5.client.MqttTestClient edgeClient =
            new org.apache.bifromq.mqtt.integration.v5.client.MqttTestClient(EDGE_URI, "edge-v5");
        try {
            MqttConnectionOptions connectOptions = new MqttConnectionOptions();
            connectOptions.setCleanStart(true);
            connectOptions.setSessionExpiryInterval(0L);

            defaultClient.connect(connectOptions);
            edgeClient.connect(connectOptions);

            verify(authProvider).auth(argThat((MQTT5AuthData authData) ->
                authData.getListenerId().equals("default")
                    && authData.getTransportType().equals("TCP")));
            verify(edgeAuthProvider).auth(argThat((MQTT5AuthData authData) ->
                authData.getListenerId().equals("edge")
                    && authData.getTransportType().equals("TCP")));
        } finally {
            if (defaultClient.isConnected()) {
                defaultClient.disconnect();
            }
            if (edgeClient.isConnected()) {
                edgeClient.disconnect();
            }
            defaultClient.close();
            edgeClient.close();
        }
    }

    private void stubMQTT3AuthProviders() {
        when(authProvider.forListener("edge", "TCP")).thenReturn(edgeAuthProvider);
        when(authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder()
                    .setTenantId(tenantId)
                    .setUserId("defaultUser")
                    .build())
                .build()));
        when(edgeAuthProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder()
                    .setTenantId(tenantId)
                    .setUserId("edgeUser")
                    .build())
                .build()));
        stubPermissionChecks();
    }

    private void stubMQTT5AuthProviders() {
        when(authProvider.forListener("edge", "TCP")).thenReturn(edgeAuthProvider);
        when(authProvider.auth(any(MQTT5AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT5AuthResult.newBuilder()
                .setSuccess(Success.newBuilder()
                    .setTenantId(tenantId)
                    .setUserId("defaultUser")
                    .build())
                .build()));
        when(edgeAuthProvider.auth(any(MQTT5AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT5AuthResult.newBuilder()
                .setSuccess(Success.newBuilder()
                    .setTenantId(tenantId)
                    .setUserId("edgeUser")
                    .build())
                .build()));
        stubPermissionChecks();
    }

    private void stubPermissionChecks() {
        CheckResult granted = CheckResult.newBuilder()
            .setGranted(Granted.newBuilder().build())
            .build();
        when(authProvider.checkPermission(any(), any())).thenReturn(CompletableFuture.completedFuture(granted));
        when(edgeAuthProvider.checkPermission(any(), any())).thenReturn(CompletableFuture.completedFuture(granted));
    }
}
