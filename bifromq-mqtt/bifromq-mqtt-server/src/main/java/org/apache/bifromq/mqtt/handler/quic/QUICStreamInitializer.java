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

package org.apache.bifromq.mqtt.handler.quic;

import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.mqtt.handler.ChannelAttrs;
import org.apache.bifromq.mqtt.handler.ConditionalRejectHandler;
import org.apache.bifromq.mqtt.handler.MQTTMessageDebounceHandler;
import org.apache.bifromq.mqtt.handler.MQTTPreludeHandler;
import org.apache.bifromq.mqtt.handler.condition.DirectMemPressureCondition;
import org.apache.bifromq.mqtt.session.MQTTSessionContext;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;

/**
 * Initializer for QUIC stream channels.
 * <p>
 * This initializer sets up the pipeline for each new bidirectional QUIC stream
 * with the same handler names and structure as the TCP pipeline, ensuring that
 * dynamic pipeline modifications in {@code MQTTConnectHandler} (e.g., replace
 * by name)
 * work correctly. This addresses review issue #10 (Pipeline replace name
 * assumptions).
 * <p>
 * Key differences from TCP pipeline initialization:
 * <ul>
 * <li>The {@code MQTT_SESSION_CTX} attribute is propagated from the parent
 * QuicChannel (fixes #1)</li>
 * <li>Remote address attribute is propagated from the parent QuicChannel (fixes
 * #3)</li>
 * <li>No {@code SslHandler} is added (QUIC TLS is at connection level) (fixes
 * #2)</li>
 * <li>No {@code ProxyProtocolHandler} (not applicable to QUIC/UDP) (fixes #12
 * low)</li>
 * <li>No {@code ChannelTrafficShapingHandler} per stream; should be at
 * connection level (fixes #13)</li>
 * </ul>
 */
@Slf4j
public class QUICStreamInitializer extends ChannelInitializer<QuicStreamChannel> {

    /**
     * Attribute key to store the QUICStreamRouter on each stream channel,
     * so MQTTConnectHandler can retrieve it after session creation.
     */
    public static final AttributeKey<QUICStreamRouter> STREAM_ROUTER_KEY = AttributeKey
            .valueOf("QUIC_STREAM_ROUTER_ON_STREAM");

    private final int connectTimeoutSeconds;
    private final int maxBytesInMessage;
    private final IEventCollector eventCollector;

    public QUICStreamInitializer(int connectTimeoutSeconds,
            int maxBytesInMessage,
            IEventCollector eventCollector) {
        this.connectTimeoutSeconds = connectTimeoutSeconds;
        this.maxBytesInMessage = maxBytesInMessage;
        this.eventCollector = eventCollector;
    }

    @Override
    protected void initChannel(QuicStreamChannel ch) {
        // Propagate session context from parent QuicChannel (fixes #1)
        MQTTSessionContext sessionCtx = ch.parent().attr(ChannelAttrs.MQTT_SESSION_CTX).get();
        if (sessionCtx == null) {
            log.error("MQTT_SESSION_CTX not found on parent QuicChannel, closing stream");
            ch.close();
            return;
        }
        ch.attr(ChannelAttrs.MQTT_SESSION_CTX).set(sessionCtx);

        // Propagate remote address from parent QuicChannel (fixes #3)
        java.net.InetSocketAddress peerAddr = ch.parent().attr(QUICConnectionHandler.QUIC_PEER_ADDR).get();
        if (peerAddr != null) {
            ch.attr(ChannelAttrs.PEER_ADDR).set(peerAddr);
        }

        // Propagate QUICStreamRouter from parent QuicChannel to stream attribute
        QUICStreamRouter router = ch.parent().attr(QUICConnectionHandler.QUIC_STREAM_ROUTER).get();
        if (router != null) {
            ch.attr(STREAM_ROUTER_KEY).set(router);
            // Register this stream with the router based on stream ID
            long streamId = ch.streamId();
            if (streamId == 0) {
                // Stream 0 is the control stream
                router.setControlStream(ch);
                log.debug("Control stream registered: streamId={}", streamId);
            } else {
                // Data streams: map QUIC stream ID to bucket index
                // QUIC bidirectional stream IDs: 0, 4, 8, 12, ... (client-initiated)
                // We use streamId/4 - 1 to get bucket index (skip stream 0)
                int bucketIndex = (int) ((streamId / 4) - 1) % router.dataStreamCount();
                if (bucketIndex >= 0 && bucketIndex < router.dataStreamCount()) {
                    router.setDataStream(bucketIndex, ch);
                    log.debug("Data stream registered: streamId={}, bucket={}", streamId, bucketIndex);
                }
            }
        }

        // Build pipeline with same handler names as TCP pipeline (fixes #10)
        // The order and names MUST match what MQTTConnectHandler expects to find
        ch.pipeline().addLast(MqttEncoder.class.getName(), MqttEncoder.INSTANCE);
        ch.pipeline().addLast(MqttDecoder.class.getName(), new MqttDecoder(maxBytesInMessage));
        ch.pipeline().addLast(MQTTMessageDebounceHandler.NAME, new MQTTMessageDebounceHandler());
        ch.pipeline().addLast(ConditionalRejectHandler.NAME,
                new ConditionalRejectHandler(DirectMemPressureCondition.INSTANCE, eventCollector));
        ch.pipeline().addLast(MQTTPreludeHandler.NAME,
                new MQTTPreludeHandler(connectTimeoutSeconds));

        log.debug("QUIC stream pipeline initialized: streamId={}, remote={}",
                ch.streamId(), peerAddr);
    }
}
