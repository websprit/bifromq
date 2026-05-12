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

package org.apache.bifromq.mqtt.session;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.mqtt.MockableTest;
import org.apache.bifromq.mqtt.handler.ChannelAttrs;
import org.apache.bifromq.plugin.authprovider.IAuthProvider;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class MQTTSessionContextTest extends MockableTest {
    @Mock
    private IAuthProvider globalAuthProvider;
    @Mock
    private IAuthProvider listenerAuthProvider;
    @Mock
    private ChannelHandlerContext channelHandlerContext;

    @Test
    public void authProviderUsesListenerScopedDelegate() {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.attr(ChannelAttrs.LISTENER_ID).set("edge");
        channel.attr(ChannelAttrs.TRANSPORT_TYPE).set("TCP");
        when(channelHandlerContext.channel()).thenReturn(channel);
        when(globalAuthProvider.forListener("edge", "TCP")).thenReturn(listenerAuthProvider);
        MQTT3AuthData authData = MQTT3AuthData.newBuilder().build();
        when(listenerAuthProvider.auth(authData)).thenReturn(CompletableFuture.completedFuture(
            MQTT3AuthResult.newBuilder().build()));
        MQTTSessionContext sessionContext = MQTTSessionContext.builder()
            .authProvider(globalAuthProvider)
            .build();

        sessionContext.authProvider(channelHandlerContext).auth(authData);

        verify(globalAuthProvider).forListener("edge", "TCP");
        verify(listenerAuthProvider).auth(authData);
    }
}
