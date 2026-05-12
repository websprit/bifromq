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

package org.apache.bifromq.mqtt.utils;

import static org.testng.Assert.assertEquals;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.apache.bifromq.mqtt.handler.ChannelAttrs;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthData;
import org.testng.annotations.Test;

public class AuthUtilTest {
    @Test
    public void mqtt3AuthDataCarriesListenerIdentity() {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.attr(ChannelAttrs.LISTENER_ID).set("edge");
        channel.attr(ChannelAttrs.TRANSPORT_TYPE).set("TCP");
        MqttConnectMessage connectMessage = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .build();

        MQTT3AuthData authData = AuthUtil.buildMQTT3AuthData(channel, connectMessage);

        assertEquals(authData.getListenerId(), "edge");
        assertEquals(authData.getTransportType(), "TCP");
    }

    @Test
    public void mqtt5AuthDataCarriesListenerIdentity() {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.attr(ChannelAttrs.LISTENER_ID).set("public");
        channel.attr(ChannelAttrs.TRANSPORT_TYPE).set("WS");
        MqttConnectMessage connectMessage = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_5)
            .build();

        MQTT5AuthData authData = AuthUtil.buildMQTT5AuthData(channel, connectMessage);

        assertEquals(authData.getListenerId(), "public");
        assertEquals(authData.getTransportType(), "WS");
    }
}
