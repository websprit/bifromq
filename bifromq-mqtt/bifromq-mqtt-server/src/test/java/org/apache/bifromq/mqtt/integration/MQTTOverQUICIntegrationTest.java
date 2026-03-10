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

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.incubator.codec.quic.QuicClientCodecBuilder;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.incubator.codec.quic.QuicStreamType;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

/**
 * End-to-end integration test for MQTT over QUIC.
 * <p>
 * This test creates a real QUIC client using netty-incubator-codec-quic,
 * connects to a minimal QUIC server, and verifies MQTT protocol framing
 * over a QUIC bidirectional stream.
 * <p>
 * Note: This test does NOT start the full MQTTBroker (which requires all
 * backend services). Instead, it verifies the QUIC transport layer works
 * correctly by testing:
 * <ul>
 * <li>QUIC TLS 1.3 handshake with self-signed certificates</li>
 * <li>Bidirectional stream creation</li>
 * <li>MQTT codec (encoder/decoder) over QUIC stream</li>
 * <li>MQTT CONNECT message framing and transmission</li>
 * </ul>
 */
@Slf4j
public class MQTTOverQUICIntegrationTest {

    /**
     * Test that a QUIC client can establish a TLS 1.3 connection and
     * send/receive MQTT-framed messages over a bidirectional stream.
     * <p>
     * This test sets up a minimal QUIC server that echoes back a CONNACK
     * when it receives a CONNECT message, verifying the full transport path.
     */
    @Test(timeOut = 30000)
    void testQUICTransportWithMQTTCodec() throws Exception {
        SelfSignedCertificate cert = new SelfSignedCertificate();

        // Build server-side QUIC SSL context
        QuicSslContext serverSslCtx = QuicSslContextBuilder
                .forServer(cert.key(), null, cert.cert())
                .applicationProtocols("mqtt")
                .build();

        // Build client-side QUIC SSL context
        QuicSslContext clientSslCtx = QuicSslContextBuilder
                .forClient()
                .trustManager(io.netty.handler.ssl.util.InsecureTrustManagerFactory.INSTANCE)
                .applicationProtocols("mqtt")
                .build();

        EventLoopGroup group = new NioEventLoopGroup(1);
        final CountDownLatch serverReceivedConnect = new CountDownLatch(1);
        final CountDownLatch clientReceivedConnAck = new CountDownLatch(1);
        final AtomicReference<MqttMessage> receivedOnServer = new AtomicReference<>();
        final AtomicReference<MqttMessage> receivedOnClient = new AtomicReference<>();

        try {
            // ---- Server setup ----
            io.netty.incubator.codec.quic.QuicServerCodecBuilder serverCodec = new io.netty.incubator.codec.quic.QuicServerCodecBuilder()
                    .sslContext(serverSslCtx)
                    .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
                    .initialMaxData(10000000)
                    .initialMaxStreamDataBidirectionalLocal(1000000)
                    .initialMaxStreamDataBidirectionalRemote(1000000)
                    .initialMaxStreamsBidirectional(100)
                    .tokenHandler(io.netty.incubator.codec.quic.InsecureQuicTokenHandler.INSTANCE)
                    .handler(new ChannelInboundHandlerAdapter())
                    .streamHandler(new ChannelInitializer<QuicStreamChannel>() {
                        @Override
                        protected void initChannel(QuicStreamChannel ch) {
                            ch.pipeline().addLast(MqttEncoder.INSTANCE);
                            ch.pipeline().addLast(new MqttDecoder(65535));
                            ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                    if (msg instanceof MqttMessage mqttMsg) {
                                        receivedOnServer.set(mqttMsg);
                                        log.info("Server received MQTT message: {}",
                                                mqttMsg.fixedHeader().messageType());
                                        serverReceivedConnect.countDown();

                                        // Send CONNACK reply
                                        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                                                MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
                                        io.netty.handler.codec.mqtt.MqttConnAckVariableHeader varHeader = new io.netty.handler.codec.mqtt.MqttConnAckVariableHeader(
                                                MqttConnectReturnCode.CONNECTION_ACCEPTED, false);
                                        MqttConnAckMessage connAck = new MqttConnAckMessage(fixedHeader, varHeader);
                                        ctx.writeAndFlush(connAck);
                                    }
                                }
                            });
                        }
                    });

            Channel serverChannel = new Bootstrap()
                    .group(group)
                    .channel(NioDatagramChannel.class)
                    .handler(serverCodec.build())
                    .bind(new InetSocketAddress("127.0.0.1", 0))
                    .sync().channel();

            InetSocketAddress serverAddr = (InetSocketAddress) serverChannel.localAddress();
            log.info("Test QUIC server listening on: {}", serverAddr);

            // ---- Client setup ----
            QuicClientCodecBuilder clientCodec = new QuicClientCodecBuilder()
                    .sslContext(clientSslCtx)
                    .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
                    .initialMaxData(10000000)
                    .initialMaxStreamDataBidirectionalLocal(1000000)
                    .initialMaxStreamDataBidirectionalRemote(1000000)
                    .initialMaxStreamsBidirectional(100);

            Channel clientUdpChannel = new Bootstrap()
                    .group(group)
                    .channel(NioDatagramChannel.class)
                    .handler(clientCodec.build())
                    .bind(0).sync().channel();

            QuicChannel quicChannel = QuicChannel.newBootstrap(clientUdpChannel)
                    .handler(new ChannelInboundHandlerAdapter())
                    .streamHandler(new ChannelInboundHandlerAdapter())
                    .remoteAddress(serverAddr)
                    .connect()
                    .get(5, TimeUnit.SECONDS);

            log.info("QUIC client connected to: {}", serverAddr);

            // Open a bidirectional stream and send MQTT CONNECT
            QuicStreamChannel streamChannel = quicChannel.createStream(
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
                                        receivedOnClient.set(mqttMsg);
                                        log.info("Client received MQTT message: {}",
                                                mqttMsg.fixedHeader().messageType());
                                        clientReceivedConnAck.countDown();
                                    }
                                }
                            });
                        }
                    }).get(5, TimeUnit.SECONDS);

            // Build and send MQTT CONNECT message
            MqttFixedHeader connectFixedHeader = new MqttFixedHeader(
                    MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
            MqttConnectVariableHeader connectVarHeader = new MqttConnectVariableHeader(
                    "MQTT", // protocol name
                    4, // protocol level (MQTT 3.1.1)
                    false, // hasUserName
                    false, // hasPassword
                    false, // isWillRetain
                    0, // willQos
                    false, // isWillFlag
                    true, // isCleanSession
                    60 // keepAliveTimeSeconds
            );
            MqttConnectPayload connectPayload = new MqttConnectPayload(
                    "quic-test-client-" + System.nanoTime(),
                    null, null, null, (byte[]) null);
            MqttConnectMessage connectMsg = new MqttConnectMessage(
                    connectFixedHeader, connectVarHeader, connectPayload);

            streamChannel.writeAndFlush(connectMsg).sync();
            log.info("MQTT CONNECT sent over QUIC stream");

            // ---- Assertions ----

            // 1. Server should receive MQTT CONNECT
            assertTrue(serverReceivedConnect.await(5, TimeUnit.SECONDS),
                    "Server should receive MQTT CONNECT within 5 seconds");
            MqttMessage serverMsg = receivedOnServer.get();
            assertNotNull(serverMsg, "Server received message should not be null");
            assertTrue(serverMsg.fixedHeader().messageType() == MqttMessageType.CONNECT,
                    "Server should receive CONNECT message type");

            // 2. Client should receive MQTT CONNACK
            assertTrue(clientReceivedConnAck.await(5, TimeUnit.SECONDS),
                    "Client should receive MQTT CONNACK within 5 seconds");
            MqttMessage clientMsg = receivedOnClient.get();
            assertNotNull(clientMsg, "Client received message should not be null");
            assertTrue(clientMsg.fixedHeader().messageType() == MqttMessageType.CONNACK,
                    "Client should receive CONNACK message type");

            MqttConnAckMessage connAck = (MqttConnAckMessage) clientMsg;
            assertTrue(connAck.variableHeader().connectReturnCode() == MqttConnectReturnCode.CONNECTION_ACCEPTED,
                    "CONNACK should indicate CONNECTION_ACCEPTED");

            log.info("✅ MQTT over QUIC end-to-end test PASSED");

            // Cleanup
            streamChannel.close().sync();
            quicChannel.close().sync();
            clientUdpChannel.close().sync();
            serverChannel.close().sync();

        } finally {
            group.shutdownGracefully().sync();
            cert.delete();
        }
    }
}
