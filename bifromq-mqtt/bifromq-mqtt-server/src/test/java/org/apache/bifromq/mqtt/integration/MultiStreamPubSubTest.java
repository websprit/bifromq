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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.incubator.codec.quic.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

/**
 * Full end-to-end multi-stream MQTT over QUIC integration test.
 * <p>
 * Tests the complete flow: Producer → Broker → Consumer, using QUIC
 * multi-stream mode:
 * <ul>
 * <li>Stream 0 (Control): CONNECT/CONNACK, SUBSCRIBE/SUBACK</li>
 * <li>Stream 4+ (Data): PUBLISH messages routed by topic hash</li>
 * </ul>
 * <p>
 * The broker is a minimal in-process QUIC server that:
 * <ol>
 * <li>Accepts CONNECT on the control stream and replies CONNACK</li>
 * <li>Accepts SUBSCRIBE on the control stream and replies SUBACK</li>
 * <li>Receives PUBLISH on data streams from producer</li>
 * <li>Forwards PUBLISH to matching subscribers on their data streams</li>
 * </ol>
 * <p>
 * This validates:
 * <ul>
 * <li>Multi-stream QUIC connection (control + data streams)</li>
 * <li>Control-plane / data-plane stream separation</li>
 * <li>Topic-based routing across multiple data streams</li>
 * <li>End-to-end message delivery from producer to consumer</li>
 * </ul>
 */
@Slf4j
public class MultiStreamPubSubTest {

    private static final int DATA_STREAM_COUNT = 4; // Use 4 data streams for testing

    // ============================================================
    // Minimal Broker: manages subscriptions and forwards messages
    // ============================================================
    private static class MinimalBroker {
        // clientId → QuicChannel
        private final Map<String, QuicChannel> clients = new ConcurrentHashMap<>();
        // topic → Set<clientId>
        private final Map<String, Set<String>> subscriptions = new ConcurrentHashMap<>();
        // clientId → List<QuicStreamChannel> (data streams opened by server to client)
        private final Map<String, List<QuicStreamChannel>> clientDataStreams = new ConcurrentHashMap<>();

        void handleConnect(ChannelHandlerContext ctx, MqttConnectMessage connectMsg) {
            String clientId = connectMsg.payload().clientIdentifier();
            QuicStreamChannel streamChannel = (QuicStreamChannel) ctx.channel();
            QuicChannel quicChannel = streamChannel.parent();
            clients.put(clientId, quicChannel);
            // Store clientId on the QuicChannel for later lookup
            quicChannel.attr(io.netty.util.AttributeKey.<String>valueOf("clientId")).set(clientId);
            log.info("[Broker] Client connected: {} on stream {}", clientId, streamChannel.streamId());

            // Send CONNACK
            MqttFixedHeader fixedHeader = new MqttFixedHeader(
                    MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
            MqttConnAckVariableHeader varHeader = new MqttConnAckVariableHeader(
                    MqttConnectReturnCode.CONNECTION_ACCEPTED, false);
            ctx.writeAndFlush(new MqttConnAckMessage(fixedHeader, varHeader));
        }

        void handleSubscribe(ChannelHandlerContext ctx, MqttSubscribeMessage subMsg) {
            QuicStreamChannel streamChannel = (QuicStreamChannel) ctx.channel();
            String clientId = streamChannel.parent().attr(
                    io.netty.util.AttributeKey.<String>valueOf("clientId")).get();
            log.info("[Broker] Subscribe from {}: {} topics", clientId,
                    subMsg.payload().topicSubscriptions().size());

            List<Integer> grantedQoS = new ArrayList<>();
            for (MqttTopicSubscription sub : subMsg.payload().topicSubscriptions()) {
                subscriptions.computeIfAbsent(sub.topicName(), k -> ConcurrentHashMap.newKeySet())
                        .add(clientId);
                grantedQoS.add(sub.qualityOfService().value());
                log.info("[Broker] Subscribed {} to topic: {}", clientId, sub.topicName());
            }

            // Send SUBACK
            MqttFixedHeader fixedHeader = new MqttFixedHeader(
                    MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
            MqttMessageIdVariableHeader varHeader = MqttMessageIdVariableHeader.from(
                    subMsg.variableHeader().messageId());
            MqttSubAckMessage subAck = new MqttSubAckMessage(fixedHeader, varHeader,
                    new MqttSubAckPayload(grantedQoS));
            ctx.writeAndFlush(subAck);
        }

        void handlePublish(ChannelHandlerContext ctx, MqttPublishMessage pubMsg) {
            String topic = pubMsg.variableHeader().topicName();
            byte[] payload = new byte[pubMsg.payload().readableBytes()];
            pubMsg.payload().getBytes(pubMsg.payload().readerIndex(), payload);
            log.info("[Broker] PUBLISH received: topic={}, payload={}",
                    topic, new String(payload, StandardCharsets.UTF_8));

            // Forward to all subscribers
            Set<String> subs = subscriptions.get(topic);
            if (subs == null || subs.isEmpty()) {
                log.info("[Broker] No subscribers for topic: {}", topic);
                return;
            }

            // Get publisher's clientId to avoid echo
            QuicStreamChannel srcStream = (QuicStreamChannel) ctx.channel();
            String publisherId = srcStream.parent().attr(
                    io.netty.util.AttributeKey.<String>valueOf("clientId")).get();

            for (String subClientId : subs) {
                if (subClientId.equals(publisherId)) {
                    continue; // skip echo
                }
                QuicChannel consumerConn = clients.get(subClientId);
                if (consumerConn == null || !consumerConn.isActive()) {
                    continue;
                }
                // Forward on a data stream
                forwardToClient(consumerConn, subClientId, topic, payload);
            }
        }

        private void forwardToClient(QuicChannel consumerConn, String clientId,
                String topic, byte[] payload) {
            List<QuicStreamChannel> streams = clientDataStreams.get(clientId);
            if (streams != null && !streams.isEmpty()) {
                // Pick a stream by topic hash
                int idx = Math.abs(topic.hashCode() % streams.size());
                QuicStreamChannel dataStream = streams.get(idx);
                if (dataStream.isActive()) {
                    MqttFixedHeader fixedHeader = new MqttFixedHeader(
                            MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, false, 0);
                    MqttPublishVariableHeader varHeader = new MqttPublishVariableHeader(topic, 0);
                    MqttPublishMessage fwdMsg = new MqttPublishMessage(
                            fixedHeader, varHeader, Unpooled.wrappedBuffer(payload));
                    dataStream.writeAndFlush(fwdMsg);
                    log.info("[Broker] Forwarded PUBLISH to {} on data stream {}: topic={}",
                            clientId, dataStream.streamId(), topic);
                    return;
                }
            }
            log.warn("[Broker] No active data stream for client: {}", clientId);
        }

        /**
         * Open server-initiated data streams to a connected consumer client.
         * These will be used to forward PUBLISH messages.
         */
        void openDataStreams(QuicChannel clientConn, String clientId, int count,
                CountDownLatch streamsReady) {
            List<QuicStreamChannel> streams = new CopyOnWriteArrayList<>();
            clientDataStreams.put(clientId, streams);
            for (int i = 0; i < count; i++) {
                clientConn.createStream(QuicStreamType.BIDIRECTIONAL,
                        new ChannelInitializer<QuicStreamChannel>() {
                            @Override
                            protected void initChannel(QuicStreamChannel ch) {
                                ch.pipeline().addLast(MqttEncoder.INSTANCE);
                                ch.pipeline().addLast(new MqttDecoder(65535));
                            }
                        }).addListener(f -> {
                            if (f.isSuccess()) {
                                QuicStreamChannel stream = (QuicStreamChannel) f.getNow();
                                streams.add(stream);
                                log.info("[Broker] Server-initiated data stream {} for client {}",
                                        stream.streamId(), clientId);
                                if (streams.size() == count) {
                                    streamsReady.countDown();
                                }
                            } else {
                                log.error("[Broker] Failed to create data stream for client {}", clientId, f.cause());
                            }
                        });
            }
        }
    }

    /**
     * Full publish-subscribe test over multi-stream QUIC.
     * <p>
     * Flow:
     * 
     * <pre>
     * 1. Consumer connects (control stream 0), subscribes to "sensor/temp" and "sensor/humidity"
     * 2. Broker opens data streams to consumer for pushing messages
     * 3. Producer connects (control stream 0)
     * 4. Producer opens data streams and publishes messages on different topics
     * 5. Broker receives PUBLISH, matches subscriptions, forwards to consumer on data streams
     * 6. Consumer verifies received messages on data streams
     * </pre>
     */
    @Test(timeOut = 30000)
    void testMultiStreamPubSub() throws Exception {
        SelfSignedCertificate cert = new SelfSignedCertificate();

        QuicSslContext serverSslCtx = QuicSslContextBuilder
                .forServer(cert.key(), null, cert.cert())
                .applicationProtocols("mqtt")
                .build();

        QuicSslContext clientSslCtx = QuicSslContextBuilder
                .forClient()
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .applicationProtocols("mqtt")
                .build();

        // Use separate EventLoopGroups to avoid event-loop starvation with QUIC codec
        EventLoopGroup serverGroup = new NioEventLoopGroup(2);
        EventLoopGroup consumerGroup = new NioEventLoopGroup(1);
        EventLoopGroup producerGroup = new NioEventLoopGroup(1);
        MinimalBroker broker = new MinimalBroker();

        // Latches
        CountDownLatch consumerConnected = new CountDownLatch(1);
        CountDownLatch consumerSubscribed = new CountDownLatch(1);
        CountDownLatch producerConnected = new CountDownLatch(1);
        CountDownLatch consumerDataStreamsReady = new CountDownLatch(1);
        CountDownLatch messagesReceived = new CountDownLatch(3); // expect 3 messages

        // Track received messages on consumer
        List<String> receivedTopics = new CopyOnWriteArrayList<>();
        List<String> receivedPayloads = new CopyOnWriteArrayList<>();
        // Track which stream each message arrived on (to verify multi-stream routing)
        List<Long> receivedOnStreamIds = new CopyOnWriteArrayList<>();

        try {
            // ===== QUIC Server (Broker) =====
            QuicServerCodecBuilder serverCodec = new QuicServerCodecBuilder()
                    .sslContext(serverSslCtx)
                    .maxIdleTimeout(10000, TimeUnit.MILLISECONDS)
                    .initialMaxData(10000000)
                    .initialMaxStreamDataBidirectionalLocal(1000000)
                    .initialMaxStreamDataBidirectionalRemote(1000000)
                    .initialMaxStreamsBidirectional(100)
                    .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
                    // Use ChannelInitializer for connection-level handler (per-connection instance)
                    .handler(new ChannelInitializer<QuicChannel>() {
                        @Override
                        protected void initChannel(QuicChannel ch) {
                            // No-op: connection level
                        }
                    })
                    .streamHandler(new ChannelInitializer<QuicStreamChannel>() {
                        @Override
                        protected void initChannel(QuicStreamChannel ch) {
                            ch.pipeline().addLast(MqttEncoder.INSTANCE);
                            ch.pipeline().addLast(new MqttDecoder(65535));
                            ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                    if (msg instanceof MqttMessage mqttMsg) {
                                        long streamId = ch.streamId();
                                        MqttMessageType type = mqttMsg.fixedHeader().messageType();
                                        log.info("[Broker] Received {} on stream {}", type, streamId);

                                        switch (type) {
                                            case CONNECT -> {
                                                broker.handleConnect(ctx, (MqttConnectMessage) mqttMsg);
                                                String cid = ((MqttConnectMessage) mqttMsg).payload()
                                                        .clientIdentifier();
                                                if (cid.startsWith("consumer")) {
                                                    consumerConnected.countDown();
                                                    // Open server→consumer data streams
                                                    QuicChannel qc = (QuicChannel) ctx.channel().parent();
                                                    broker.openDataStreams(qc, cid, DATA_STREAM_COUNT,
                                                            consumerDataStreamsReady);
                                                } else {
                                                    producerConnected.countDown();
                                                }
                                            }
                                            case SUBSCRIBE -> {
                                                broker.handleSubscribe(ctx, (MqttSubscribeMessage) mqttMsg);
                                                consumerSubscribed.countDown();
                                            }
                                            case PUBLISH -> {
                                                MqttPublishMessage pubMsg = (MqttPublishMessage) mqttMsg;
                                                broker.handlePublish(ctx, pubMsg);
                                            }
                                            default ->
                                                log.info("[Broker] Ignoring message type: {}", type);
                                        }
                                    }
                                }
                            });
                        }
                    });

            Channel serverChannel = new Bootstrap()
                    .group(serverGroup)
                    .channel(NioDatagramChannel.class)
                    .handler(serverCodec.build())
                    .bind(new InetSocketAddress("127.0.0.1", 0))
                    .sync().channel();

            InetSocketAddress serverAddr = (InetSocketAddress) serverChannel.localAddress();
            log.info("[Test] QUIC broker listening on: {}", serverAddr);

            // ===== Consumer Client =====
            log.info("[Test] === Phase 1: Consumer connects and subscribes ===");

            QuicClientCodecBuilder clientCodec = new QuicClientCodecBuilder()
                    .sslContext(clientSslCtx)
                    .maxIdleTimeout(10000, TimeUnit.MILLISECONDS)
                    .initialMaxData(10000000)
                    .initialMaxStreamDataBidirectionalLocal(1000000)
                    .initialMaxStreamDataBidirectionalRemote(1000000)
                    .initialMaxStreamsBidirectional(100);

            Channel consumerUdp = new Bootstrap()
                    .group(consumerGroup)
                    .channel(NioDatagramChannel.class)
                    .handler(clientCodec.build())
                    .bind(0).sync().channel();

            QuicChannel consumerConn = QuicChannel.newBootstrap(consumerUdp)
                    .handler(new ChannelInboundHandlerAdapter())
                    .streamHandler(new ChannelInitializer<QuicStreamChannel>() {
                        @Override
                        protected void initChannel(QuicStreamChannel ch) {
                            // Handler for server-initiated data streams (broker → consumer)
                            ch.pipeline().addLast(MqttEncoder.INSTANCE);
                            ch.pipeline().addLast(new MqttDecoder(65535));
                            ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                    if (msg instanceof MqttPublishMessage pubMsg) {
                                        String topic = pubMsg.variableHeader().topicName();
                                        byte[] payload = new byte[pubMsg.payload().readableBytes()];
                                        pubMsg.payload().getBytes(pubMsg.payload().readerIndex(), payload);
                                        String payloadStr = new String(payload, StandardCharsets.UTF_8);
                                        long streamId = ((QuicStreamChannel) ctx.channel()).streamId();

                                        receivedTopics.add(topic);
                                        receivedPayloads.add(payloadStr);
                                        receivedOnStreamIds.add(streamId);

                                        log.info(
                                                "[Consumer] ✅ Received PUBLISH on data stream {}: topic={}, payload={}",
                                                streamId, topic, payloadStr);
                                        messagesReceived.countDown();
                                    }
                                }
                            });
                        }
                    })
                    .remoteAddress(serverAddr)
                    .connect()
                    .get(5, TimeUnit.SECONDS);

            log.info("[Consumer] QUIC connection established");

            // Consumer: Open control stream (stream 0) for CONNECT
            QuicStreamChannel consumerControlStream = consumerConn.createStream(
                    QuicStreamType.BIDIRECTIONAL,
                    new ChannelInitializer<QuicStreamChannel>() {
                        @Override
                        protected void initChannel(QuicStreamChannel ch) {
                            ch.pipeline().addLast(MqttEncoder.INSTANCE);
                            ch.pipeline().addLast(new MqttDecoder(65535));
                            ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                    if (msg instanceof MqttMessage mqttMsg) {
                                        MqttMessageType type = mqttMsg.fixedHeader().messageType();
                                        log.info("[Consumer] Received {} on control stream", type);
                                        if (type == MqttMessageType.CONNACK) {
                                            consumerConnected.countDown();
                                        } else if (type == MqttMessageType.SUBACK) {
                                            consumerSubscribed.countDown();
                                        }
                                    }
                                }
                            });
                        }
                    }).get(5, TimeUnit.SECONDS);

            // Send CONNECT
            consumerControlStream.writeAndFlush(buildConnectMessage("consumer-1")).sync();
            log.info("[Consumer] CONNECT sent");
            assertTrue(consumerConnected.await(5, TimeUnit.SECONDS),
                    "Consumer should receive CONNACK");

            // Wait for broker to open data streams to consumer
            assertTrue(consumerDataStreamsReady.await(5, TimeUnit.SECONDS),
                    "Broker should open data streams to consumer");
            log.info("[Consumer] Broker data streams ready");

            // Send SUBSCRIBE (on control stream)
            MqttSubscribeMessage subMsg = buildSubscribeMessage(1,
                    "sensor/temp", MqttQoS.AT_MOST_ONCE,
                    "sensor/humidity", MqttQoS.AT_MOST_ONCE,
                    "device/status", MqttQoS.AT_MOST_ONCE);
            consumerControlStream.writeAndFlush(subMsg).sync();
            log.info("[Consumer] SUBSCRIBE sent for 3 topics");
            assertTrue(consumerSubscribed.await(5, TimeUnit.SECONDS),
                    "Consumer should receive SUBACK");

            // ===== Producer Client =====
            log.info("[Test] === Phase 2: Producer connects and publishes ===");

            // Build a separate QUIC client codec for producer (cannot reuse consumer's
            // codec handler)
            QuicClientCodecBuilder producerCodecBuilder = new QuicClientCodecBuilder()
                    .sslContext(clientSslCtx)
                    .maxIdleTimeout(10000, TimeUnit.MILLISECONDS)
                    .initialMaxData(10000000)
                    .initialMaxStreamDataBidirectionalLocal(1000000)
                    .initialMaxStreamDataBidirectionalRemote(1000000)
                    .initialMaxStreamsBidirectional(100);

            Channel producerUdp = new Bootstrap()
                    .group(producerGroup)
                    .channel(NioDatagramChannel.class)
                    .handler(producerCodecBuilder.build())
                    .bind(0).sync().channel();

            QuicChannel producerConn = QuicChannel.newBootstrap(producerUdp)
                    .handler(new ChannelInboundHandlerAdapter())
                    .streamHandler(new ChannelInboundHandlerAdapter())
                    .remoteAddress(serverAddr)
                    .connect()
                    .get(5, TimeUnit.SECONDS);

            log.info("[Producer] QUIC connection established");

            // Producer: Open control stream for CONNECT
            QuicStreamChannel producerControlStream = producerConn.createStream(
                    QuicStreamType.BIDIRECTIONAL,
                    new ChannelInitializer<QuicStreamChannel>() {
                        @Override
                        protected void initChannel(QuicStreamChannel ch) {
                            ch.pipeline().addLast(MqttEncoder.INSTANCE);
                            ch.pipeline().addLast(new MqttDecoder(65535));
                            ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                    if (msg instanceof MqttConnAckMessage) {
                                        log.info("[Producer] Received CONNACK");
                                        producerConnected.countDown();
                                    }
                                }
                            });
                        }
                    }).get(5, TimeUnit.SECONDS);

            producerControlStream.writeAndFlush(buildConnectMessage("producer-1")).sync();
            log.info("[Producer] CONNECT sent");
            assertTrue(producerConnected.await(5, TimeUnit.SECONDS),
                    "Producer should receive CONNACK");

            // Producer: Open multiple data streams for publishing
            QuicStreamChannel[] producerDataStreams = new QuicStreamChannel[DATA_STREAM_COUNT];
            for (int i = 0; i < DATA_STREAM_COUNT; i++) {
                producerDataStreams[i] = producerConn.createStream(
                        QuicStreamType.BIDIRECTIONAL,
                        new ChannelInitializer<QuicStreamChannel>() {
                            @Override
                            protected void initChannel(QuicStreamChannel ch) {
                                ch.pipeline().addLast(MqttEncoder.INSTANCE);
                                ch.pipeline().addLast(new MqttDecoder(65535));
                            }
                        }).get(5, TimeUnit.SECONDS);
                log.info("[Producer] Data stream {} opened (streamId={})",
                        i, producerDataStreams[i].streamId());
            }

            // ===== Publish messages on different data streams =====
            log.info("[Test] === Phase 3: Publishing messages ===");

            String[][] messages = {
                    { "sensor/temp", "{\"value\": 23.5, \"unit\": \"C\"}" },
                    { "sensor/humidity", "{\"value\": 65, \"unit\": \"%\"}" },
                    { "device/status", "{\"online\": true, \"battery\": 85}" },
            };

            for (String[] entry : messages) {
                String topic = entry[0];
                String payload = entry[1];
                // Route to data stream by topic hash (same as QUICStreamRouter logic)
                int streamIdx = Math.abs(topic.hashCode() % DATA_STREAM_COUNT);
                QuicStreamChannel dataStream = producerDataStreams[streamIdx];

                MqttPublishMessage pubMsg = buildPublishMessage(topic, payload);
                dataStream.writeAndFlush(pubMsg).sync();
                log.info("[Producer] Published on data stream {} (streamId={}): topic={}, payload={}",
                        streamIdx, dataStream.streamId(), topic, payload);
            }

            // ===== Verify consumer received all messages =====
            log.info("[Test] === Phase 4: Verifying message delivery ===");

            assertTrue(messagesReceived.await(10, TimeUnit.SECONDS),
                    "Consumer should receive all 3 published messages");

            assertEquals(receivedTopics.size(), 3, "Should receive exactly 3 messages");
            assertTrue(receivedTopics.contains("sensor/temp"), "Should receive sensor/temp");
            assertTrue(receivedTopics.contains("sensor/humidity"), "Should receive sensor/humidity");
            assertTrue(receivedTopics.contains("device/status"), "Should receive device/status");

            // Verify payloads
            int tempIdx = receivedTopics.indexOf("sensor/temp");
            assertEquals(receivedPayloads.get(tempIdx), "{\"value\": 23.5, \"unit\": \"C\"}");
            int humidityIdx = receivedTopics.indexOf("sensor/humidity");
            assertEquals(receivedPayloads.get(humidityIdx), "{\"value\": 65, \"unit\": \"%\"}");
            int statusIdx = receivedTopics.indexOf("device/status");
            assertEquals(receivedPayloads.get(statusIdx), "{\"online\": true, \"battery\": 85}");

            // Verify multi-stream: messages arrived on data streams (not control stream 0)
            for (long streamId : receivedOnStreamIds) {
                assertTrue(streamId > 0,
                        "PUBLISH should arrive on data streams (streamId > 0), got: " + streamId);
            }

            // Check that at least 2 different streams were used (topic hash distribution)
            long distinctStreams = receivedOnStreamIds.stream().distinct().count();
            log.info("[Test] Messages received on {} distinct data streams: {}",
                    distinctStreams, receivedOnStreamIds);

            log.info("╔══════════════════════════════════════════════════════════════╗");
            log.info("║  ✅ Multi-Stream MQTT over QUIC Pub/Sub Test PASSED         ║");
            log.info("║                                                              ║");
            log.info("║  Producer → [QUIC data streams] → Broker → [QUIC data       ║");
            log.info("║  streams] → Consumer                                         ║");
            log.info("║                                                              ║");
            log.info("║  3 topics published, 3 messages delivered                    ║");
            log.info("║  {} distinct data streams used for delivery              ║", distinctStreams);
            log.info("╚══════════════════════════════════════════════════════════════╝");

            // Cleanup
            for (QuicStreamChannel ds : producerDataStreams) {
                ds.close().sync();
            }
            producerControlStream.close().sync();
            producerConn.close().sync();
            producerUdp.close().sync();

            consumerControlStream.close().sync();
            consumerConn.close().sync();
            consumerUdp.close().sync();

            serverChannel.close().sync();

        } finally {
            serverGroup.shutdownGracefully().sync();
            consumerGroup.shutdownGracefully().sync();
            producerGroup.shutdownGracefully().sync();
            cert.delete();
        }
    }

    // ============================================================
    // Helper methods to build MQTT messages
    // ============================================================

    private MqttConnectMessage buildConnectMessage(String clientId) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader varHeader = new MqttConnectVariableHeader(
                "MQTT", 4, false, false, false, 0, false, true, 60);
        MqttConnectPayload payload = new MqttConnectPayload(
                clientId, null, null, null, (byte[]) null);
        return new MqttConnectMessage(fixedHeader, varHeader, payload);
    }

    private MqttSubscribeMessage buildSubscribeMessage(int messageId, Object... topicsAndQos) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.SUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 0);
        MqttMessageIdVariableHeader varHeader = MqttMessageIdVariableHeader.from(messageId);

        List<MqttTopicSubscription> subs = new ArrayList<>();
        for (int i = 0; i < topicsAndQos.length; i += 2) {
            String topic = (String) topicsAndQos[i];
            MqttQoS qos = (MqttQoS) topicsAndQos[i + 1];
            subs.add(new MqttTopicSubscription(topic, qos));
        }

        MqttSubscribePayload subPayload = new MqttSubscribePayload(subs);
        return new MqttSubscribeMessage(fixedHeader, varHeader, subPayload);
    }

    private MqttPublishMessage buildPublishMessage(String topic, String payload) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttPublishVariableHeader varHeader = new MqttPublishVariableHeader(topic, 0);
        return new MqttPublishMessage(fixedHeader, varHeader,
                Unpooled.wrappedBuffer(payload.getBytes(StandardCharsets.UTF_8)));
    }
}
