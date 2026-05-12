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

package org.apache.bifromq.starter.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.bifromq.starter.config.model.mqtt.MQTTServerConfig;
import org.apache.bifromq.starter.utils.ConfigFileUtil;
import org.testng.annotations.Test;

public class MQTTServerListenerConfigTest {
    @Test
    public void legacySingletonListenerMapsToDefault() throws Exception {
        StandaloneConfig config = buildConfig("clusterConfig:\n" +
            "  env: Test\n" +
            "mqttServiceConfig:\n" +
            "  server:\n" +
            "    tcpListener:\n" +
            "      port: 18830\n" +
            "    wsListener:\n" +
            "      enable: false\n" +
            "    quicListener:\n" +
            "      enable: false\n");

        MQTTServerConfig serverConfig = config.getMqttServiceConfig().getServer();
        assertTrue(serverConfig.getTcpListeners().isEmpty());
        assertEquals(serverConfig.effectiveTcpListeners().size(), 1);
        assertTrue(serverConfig.effectiveTcpListeners().containsKey(MQTTServerConfig.DEFAULT_LISTENER_ID));
        assertEquals(serverConfig.effectiveTcpListeners().get(MQTTServerConfig.DEFAULT_LISTENER_ID).getPort(), 18830);
    }

    @Test
    public void namedTcpListenersOverrideLegacySingleton() throws Exception {
        StandaloneConfig config = buildConfig("clusterConfig:\n" +
            "  env: Test\n" +
            "mqttServiceConfig:\n" +
            "  server:\n" +
            "    tcpListener:\n" +
            "      port: 18830\n" +
            "    tcpListeners:\n" +
            "      edge:\n" +
            "        port: 18831\n" +
            "        authProviderFQN: com.example.EdgeAuthProvider\n" +
            "      internal:\n" +
            "        host: 127.0.0.1\n" +
            "        port: 18832\n" +
            "    wsListener:\n" +
            "      enable: false\n" +
            "    quicListener:\n" +
            "      enable: false\n");

        MQTTServerConfig serverConfig = config.getMqttServiceConfig().getServer();
        assertEquals(serverConfig.effectiveTcpListeners().size(), 2);
        assertFalse(serverConfig.effectiveTcpListeners().containsKey(MQTTServerConfig.DEFAULT_LISTENER_ID));
        assertEquals(serverConfig.effectiveTcpListeners().get("edge").getHost(), "0.0.0.0");
        assertEquals(serverConfig.effectiveTcpListeners().get("edge").getAuthProviderFQN(),
            "com.example.EdgeAuthProvider");
        assertEquals(serverConfig.effectiveTcpListeners().get("internal").getHost(), "127.0.0.1");
    }

    @Test
    public void duplicateTcpEndpointRejected() {
        assertThrows(IllegalArgumentException.class, () -> buildConfig("clusterConfig:\n" +
            "  env: Test\n" +
            "mqttServiceConfig:\n" +
            "  server:\n" +
            "    tcpListeners:\n" +
            "      edge:\n" +
            "        port: 18831\n" +
            "      internal:\n" +
            "        port: 18831\n" +
            "    wsListener:\n" +
            "      enable: false\n" +
            "    quicListener:\n" +
            "      enable: false\n"));
    }

    @Test
    public void duplicateListenerIdInSameTransportRejectedByYamlLoader() {
        assertThrows(RuntimeException.class, () -> buildConfig("clusterConfig:\n" +
            "  env: Test\n" +
            "mqttServiceConfig:\n" +
            "  server:\n" +
            "    tcpListeners:\n" +
            "      edge:\n" +
            "        port: 18831\n" +
            "      edge:\n" +
            "        port: 18832\n"));
    }

    @Test
    public void tcpAndQuicCanSharePortWithDifferentSocketProtocol() throws Exception {
        StandaloneConfig config = buildConfig("clusterConfig:\n" +
            "  env: Test\n" +
            "mqttServiceConfig:\n" +
            "  server:\n" +
            "    tcpListeners:\n" +
            "      tcp:\n" +
            "        port: 18831\n" +
            "    wsListener:\n" +
            "      enable: false\n" +
            "    quicListeners:\n" +
            "      quic:\n" +
            "        enable: true\n" +
            "        port: 18831\n");

        assertEquals(config.getMqttServiceConfig().getServer().effectiveQuicListeners().get("quic").getPort(), 18831);
    }

    private StandaloneConfig buildConfig(String yaml) throws Exception {
        File file = File.createTempFile("bifromq-mqtt-listener-config", ".yaml");
        Files.writeString(file.toPath(), yaml, StandardCharsets.UTF_8);
        StandaloneConfig config = ConfigFileUtil.build(file, StandaloneConfig.class);
        StandaloneConfigConsolidator.consolidate(config);
        return config;
    }
}
