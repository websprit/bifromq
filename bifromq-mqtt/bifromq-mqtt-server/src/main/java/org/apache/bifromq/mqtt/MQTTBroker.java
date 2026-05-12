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

import static org.apache.bifromq.mqtt.handler.condition.ORCondition.or;

import com.google.common.util.concurrent.RateLimiter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.netty4.NettyEventExecutorMetrics;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import io.netty.incubator.codec.quic.QuicServerCodecBuilder;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.baseenv.NettyEnv;
import org.apache.bifromq.mqtt.handler.ChannelAttrs;
import org.apache.bifromq.mqtt.handler.ClientAddrHandler;
import org.apache.bifromq.mqtt.handler.ConditionalRejectHandler;
import org.apache.bifromq.mqtt.handler.ConnectionRateLimitHandler;
import org.apache.bifromq.mqtt.handler.MQTTMessageDebounceHandler;
import org.apache.bifromq.mqtt.handler.MQTTPreludeHandler;
import org.apache.bifromq.mqtt.handler.ProxyProtocolDetector;
import org.apache.bifromq.mqtt.handler.ProxyProtocolHandler;
import org.apache.bifromq.mqtt.handler.condition.DirectMemPressureCondition;
import org.apache.bifromq.mqtt.handler.condition.HeapMemPressureCondition;
import org.apache.bifromq.mqtt.handler.quic.QUICConnectionHandler;
import org.apache.bifromq.mqtt.handler.quic.QUICStreamInitializer;
import org.apache.bifromq.mqtt.handler.quic.HmacQuicTokenHandler;
import org.apache.bifromq.mqtt.handler.ws.MqttOverWSHandler;
import org.apache.bifromq.mqtt.handler.ws.WebSocketOnlyHandler;
import org.apache.bifromq.mqtt.service.ILocalSessionServer;
import org.apache.bifromq.mqtt.session.MQTTSessionContext;
import org.apache.bifromq.mqtt.spi.UserPropsCustomizerFactory;

@Slf4j
class MQTTBroker implements IMQTTBroker {
    private static final String MQTT_SUBPROTOCOL_CSV_LIST = "mqtt, mqttv3.1, mqttv3.1.1";
    private final MQTTBrokerBuilder builder;
    private final ILocalSessionServer sessionServer;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final RateLimiter connRateLimiter;
    private volatile MQTTSessionContext sessionContext;
    private final Map<String, ChannelFuture> tcpChannelFs = new LinkedHashMap<>();
    private final Map<String, ChannelFuture> tlsChannelFs = new LinkedHashMap<>();
    private final Map<String, ChannelFuture> wsChannelFs = new LinkedHashMap<>();
    private final Map<String, ChannelFuture> wssChannelFs = new LinkedHashMap<>();
    private final Map<String, ChannelFuture> quicChannelFs = new LinkedHashMap<>();
    private final UserPropsCustomizerFactory userPropsCustomizerFactory;

    public MQTTBroker(MQTTBrokerBuilder builder) {
        this.builder = builder;
        bossGroup = NettyEnv.createEventLoopGroup(builder.mqttBossELGThreads, "mqtt-boss-elg");
        new NettyEventExecutorMetrics(bossGroup).bindTo(Metrics.globalRegistry);
        workerGroup = NettyEnv.createEventLoopGroup(builder.mqttWorkerELGThreads, "mqtt-worker-elg");
        new NettyEventExecutorMetrics(workerGroup).bindTo(Metrics.globalRegistry);
        connRateLimiter = RateLimiter.create(builder.connectRateLimit);
        userPropsCustomizerFactory = new UserPropsCustomizerFactory(builder.userPropsCustomizerFactoryConfig);
        sessionServer = ILocalSessionServer.builder()
                .rpcServerBuilder(builder.rpcServerBuilder)
                .sessionRegistry(builder.sessionRegistry)
                .distService(builder.distService)
                .build();
    }

    @Override
    public final void start() {
        try {
            sessionContext = MQTTSessionContext.builder()
                    .serverId(builder.brokerId())
                    .localSessionRegistry(builder.sessionRegistry)
                    .localDistService(builder.distService)
                    .authProvider(builder.authProvider)
                    .resourceThrottler(builder.resourceThrottler)
                    .eventCollector(builder.eventCollector)
                    .settingProvider(builder.settingProvider)
                    .distClient(builder.distClient)
                    .inboxClient(builder.inboxClient)
                    .retainClient(builder.retainClient)
                    .sessionDictClient(builder.sessionDictClient)
                    .clientBalancer(builder.clientBalancer)
                    .userPropsCustomizer(userPropsCustomizerFactory.create())
                    .build();
            log.info("Starting MQTT broker");
            log.debug("Starting server channel");
            for (ConnListenerBuilder.TCPConnListenerBuilder listenerBuilder : builder.tcpListenerBuilders.values()) {
                ChannelFuture channelF = bindTCPChannel(listenerBuilder);
                tcpChannelFs.put(listenerBuilder.listenerId(), channelF);
                Channel channel = channelF.sync().channel();
                log.debug("Accepting mqtt connection over tcp listener[{}] at {}",
                    listenerBuilder.listenerId(), channel.localAddress());
            }
            for (ConnListenerBuilder.TLSConnListenerBuilder listenerBuilder : builder.tlsListenerBuilders.values()) {
                ChannelFuture channelF = bindTLSChannel(listenerBuilder);
                tlsChannelFs.put(listenerBuilder.listenerId(), channelF);
                Channel channel = channelF.sync().channel();
                log.debug("Accepting mqtt connection over tls listener[{}] at {}",
                    listenerBuilder.listenerId(), channel.localAddress());
            }
            for (ConnListenerBuilder.WSConnListenerBuilder listenerBuilder : builder.wsListenerBuilders.values()) {
                ChannelFuture channelF = bindWSChannel(listenerBuilder);
                wsChannelFs.put(listenerBuilder.listenerId(), channelF);
                Channel channel = channelF.sync().channel();
                log.debug("Accepting mqtt connection over ws listener[{}] at {}",
                    listenerBuilder.listenerId(), channel.localAddress());
            }
            for (ConnListenerBuilder.WSSConnListenerBuilder listenerBuilder : builder.wssListenerBuilders.values()) {
                ChannelFuture channelF = bindWSSChannel(listenerBuilder);
                wssChannelFs.put(listenerBuilder.listenerId(), channelF);
                Channel channel = channelF.sync().channel();
                log.debug("Accepting mqtt connection over wss listener[{}] at {}",
                    listenerBuilder.listenerId(), channel.localAddress());
            }
            for (QUICConnListenerBuilder listenerBuilder : builder.quicListenerBuilders.values()) {
                ChannelFuture channelF = bindQUICChannel(listenerBuilder);
                quicChannelFs.put(listenerBuilder.listenerId(), channelF);
                Channel channel = channelF.sync().channel();
                log.debug("Accepting mqtt connection over quic listener[{}] at {}",
                    listenerBuilder.listenerId(), channel.localAddress());
            }
            log.info("MQTT broker started");
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public final void close() {
        log.info("Stopping MQTT broker");
        if (bossGroup.next().inEventLoop() || workerGroup.next().inEventLoop()) {
            log.warn("MQTTBroker.close() called from EventLoop thread, offloading to avoid deadlock");
            new Thread(this::close, "mqtt-broker-close").start();
            return;
        }
        tcpChannelFs.forEach((listenerId, channelF) -> {
            channelF.channel().close().syncUninterruptibly();
            log.debug("Stopped accepting mqtt connection over tcp listener[{}]", listenerId);
        });
        tlsChannelFs.forEach((listenerId, channelF) -> {
            channelF.channel().close().syncUninterruptibly();
            log.debug("Stopped accepting mqtt connection over tls listener[{}]", listenerId);
        });
        wsChannelFs.forEach((listenerId, channelF) -> {
            channelF.channel().close().syncUninterruptibly();
            log.debug("Stopped accepting mqtt connection over ws listener[{}]", listenerId);
        });
        wssChannelFs.forEach((listenerId, channelF) -> {
            channelF.channel().close().syncUninterruptibly();
            log.debug("Stopped accepting mqtt connection over wss listener[{}]", listenerId);
        });
        quicChannelFs.forEach((listenerId, channelF) -> {
            channelF.channel().close().syncUninterruptibly();
            log.debug("Stopped accepting mqtt connection over quic listener[{}]", listenerId);
        });
        sessionContext.localSessionRegistry.disconnectAll(builder.disconnectRate).join();
        log.debug("All mqtt connection closed");

        sessionContext.awaitBgTasksFinish().join();
        log.debug("All background tasks done");

        bossGroup.shutdownGracefully().syncUninterruptibly();
        log.debug("Boss group shutdown");
        workerGroup.shutdownGracefully().syncUninterruptibly();
        log.debug("Worker group shutdown");
        userPropsCustomizerFactory.close();
        log.info("MQTT broker stopped");
    }

    private ChannelFuture bindTCPChannel(ConnListenerBuilder.TCPConnListenerBuilder connBuilder) {
        return buildChannel(connBuilder, new MQTTChannelInitializer() {
            @Override
            protected void initChannel(SocketChannel ch) {
                super.initChannel(ch);
                ch.pipeline().addLast("connRateLimiter", new ConnectionRateLimitHandler(connRateLimiter,
                        builder.eventCollector, p -> {
                            p.addLast("trafficShaper",
                                    new ChannelTrafficShapingHandler(builder.writeLimit, builder.readLimit));
                            p.addLast(MqttEncoder.class.getName(), MqttEncoder.INSTANCE);
                            // insert PacketFilter here
                            p.addLast(MqttDecoder.class.getName(), new MqttDecoder(builder.maxBytesInMessage));
                            p.addLast(MQTTMessageDebounceHandler.NAME, new MQTTMessageDebounceHandler());
                            p.addLast(ConditionalRejectHandler.NAME,
                                    new ConditionalRejectHandler(
                                            or(DirectMemPressureCondition.INSTANCE, HeapMemPressureCondition.INSTANCE),
                                            sessionContext.eventCollector));
                            p.addLast(MQTTPreludeHandler.NAME,
                                    new MQTTPreludeHandler(builder.connectTimeoutSeconds));
                        }));
            }
        });
    }

    private ChannelFuture bindTLSChannel(ConnListenerBuilder.TLSConnListenerBuilder connBuilder) {
        return buildChannel(connBuilder, new MQTTChannelInitializer() {
            @Override
            protected void initChannel(SocketChannel ch) {
                super.initChannel(ch);
                ch.pipeline().addLast("connRateLimiter", new ConnectionRateLimitHandler(connRateLimiter,
                        builder.eventCollector, p -> {
                            p.addLast("ssl", connBuilder.sslContext.newHandler(ch.alloc()));
                            p.addLast("trafficShaper",
                                    new ChannelTrafficShapingHandler(builder.writeLimit, builder.readLimit));
                            p.addLast(MqttEncoder.class.getName(), MqttEncoder.INSTANCE);
                            // insert PacketFilter here
                            p.addLast(MqttDecoder.class.getName(), new MqttDecoder(builder.maxBytesInMessage));
                            p.addLast(MQTTMessageDebounceHandler.NAME, new MQTTMessageDebounceHandler());
                            p.addLast(ConditionalRejectHandler.NAME,
                                    new ConditionalRejectHandler(
                                            or(DirectMemPressureCondition.INSTANCE, HeapMemPressureCondition.INSTANCE),
                                            sessionContext.eventCollector));
                            p.addLast(MQTTPreludeHandler.NAME,
                                    new MQTTPreludeHandler(builder.connectTimeoutSeconds));
                        }));
            }
        });
    }

    private ChannelFuture bindWSChannel(ConnListenerBuilder.WSConnListenerBuilder connBuilder) {
        return buildChannel(connBuilder, new MQTTChannelInitializer() {
            @Override
            protected void initChannel(SocketChannel ch) {
                super.initChannel(ch);
                ch.pipeline().addLast("connRateLimiter", new ConnectionRateLimitHandler(connRateLimiter,
                        builder.eventCollector, p -> {
                            p.addLast("trafficShaper",
                                    new ChannelTrafficShapingHandler(builder.writeLimit, builder.readLimit));
                            p.addLast("httpEncoder", new HttpResponseEncoder());
                            p.addLast("httpDecoder", new HttpRequestDecoder());
                            p.addLast("remoteAddr", new ClientAddrHandler());
                            p.addLast("aggregator", new HttpObjectAggregator(65536));
                            p.addLast("webSocketOnly", new WebSocketOnlyHandler(connBuilder.path()));
                            p.addLast("webSocketHandler", new WebSocketServerProtocolHandler(connBuilder.path(),
                                    MQTT_SUBPROTOCOL_CSV_LIST));
                            p.addLast("webSocketHandshakeListener", new MqttOverWSHandler(
                                    builder.maxBytesInMessage, builder.connectTimeoutSeconds,
                                    sessionContext.eventCollector));
                        }));
            }
        });
    }

    private ChannelFuture bindWSSChannel(ConnListenerBuilder.WSSConnListenerBuilder connBuilder) {
        return buildChannel(connBuilder, new MQTTChannelInitializer() {
            @Override
            protected void initChannel(SocketChannel ch) {
                super.initChannel(ch);
                ch.pipeline().addLast("connRateLimiter", new ConnectionRateLimitHandler(connRateLimiter,
                        builder.eventCollector, p -> {
                            p.addLast("ssl", connBuilder.sslContext.newHandler(ch.alloc()));
                            p.addLast("trafficShaper",
                                    new ChannelTrafficShapingHandler(builder.writeLimit, builder.readLimit));
                            p.addLast("httpEncoder", new HttpResponseEncoder());
                            p.addLast("httpDecoder", new HttpRequestDecoder());
                            p.addLast(ClientAddrHandler.class.getName(), new ClientAddrHandler());
                            p.addLast("aggregator", new HttpObjectAggregator(65536));
                            p.addLast("webSocketOnly", new WebSocketOnlyHandler(connBuilder.path()));
                            p.addLast("webSocketHandler", new WebSocketServerProtocolHandler(connBuilder.path(),
                                    MQTT_SUBPROTOCOL_CSV_LIST));
                            p.addLast("webSocketHandshakeListener", new MqttOverWSHandler(
                                    builder.maxBytesInMessage, builder.connectTimeoutSeconds,
                                    sessionContext.eventCollector));
                        }));
            }
        });
    }

    @SuppressWarnings("unchecked")
    private <T extends ConnListenerBuilder<T>> ChannelFuture buildChannel(T listenerBuilder,
            final MQTTChannelInitializer chInitializer) {
        ServerBootstrap b = new ServerBootstrap().group(bossGroup, workerGroup)
                .channel(NettyEnv.determineServerSocketChannelClass(bossGroup))
                .childHandler(chInitializer)
                .childAttr(ChannelAttrs.MQTT_SESSION_CTX, sessionContext)
                .childAttr(ChannelAttrs.LISTENER_ID, listenerBuilder.listenerId())
                .childAttr(ChannelAttrs.TRANSPORT_TYPE, listenerBuilder.transportType());
        listenerBuilder.options.forEach((k, v) -> b.option((ChannelOption<? super Object>) k, v));
        listenerBuilder.childOptions.forEach((k, v) -> b.childOption((ChannelOption<? super Object>) k, v));
        // Bind and start to accept incoming connections.
        return b.bind(listenerBuilder.host, listenerBuilder.port);
    }

    private abstract static class MQTTChannelInitializer extends ChannelInitializer<SocketChannel> {
        @Override
        protected void initChannel(SocketChannel ch) {
            ChannelPipeline pipeline = ch.pipeline();
            // handler for proxy protocol v1 and v2
            pipeline
                    .addLast(ProxyProtocolDetector.class.getName(), new ProxyProtocolDetector())
                    .addLast(HAProxyMessageDecoder.class.getName(), new HAProxyMessageDecoder())
                    .addLast(ProxyProtocolHandler.class.getName(), new ProxyProtocolHandler());
        }
    }

    private ChannelFuture bindQUICChannel(QUICConnListenerBuilder connBuilder) {
        log.info("Binding QUIC listener: host={}, port={}, maxIdleTimeoutMs={}, initialMaxData={}, " +
                "initialMaxStreamDataBidiLocal={}, initialMaxStreamDataBidiRemote={}, initialMaxStreamsBidi={}",
            connBuilder.host(), connBuilder.port(), connBuilder.maxIdleTimeoutMs(), connBuilder.initialMaxData(),
            connBuilder.initialMaxStreamDataBidiLocal(), connBuilder.initialMaxStreamDataBidiRemote(),
            connBuilder.initialMaxStreamsBidi());
        QuicServerCodecBuilder quicServerCodecBuilder = new QuicServerCodecBuilder()
                .sslContext(connBuilder.sslContext())
                .maxIdleTimeout(connBuilder.maxIdleTimeoutMs(), TimeUnit.MILLISECONDS)
                .initialMaxData(connBuilder.initialMaxData())
                .initialMaxStreamDataBidirectionalLocal(connBuilder.initialMaxStreamDataBidiLocal())
                .initialMaxStreamDataBidirectionalRemote(connBuilder.initialMaxStreamDataBidiRemote())
                .initialMaxStreamsBidirectional(connBuilder.initialMaxStreamsBidi())
                .tokenHandler(new HmacQuicTokenHandler())
                .handler(new QUICConnectionHandler(sessionContext, connBuilder.listenerId(),
                    connBuilder.transportType()))
                .streamHandler(new QUICStreamInitializer(
                        builder.connectTimeoutSeconds,
                        builder.maxBytesInMessage,
                        sessionContext.eventCollector));

        Bootstrap b = new Bootstrap()
                .group(workerGroup)
                .channel(NettyEnv.determineDatagramChannelClass(workerGroup))
                .handler(new ChannelInitializer<>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ch.pipeline().addLast("quicDatagramDiag", new QUICDatagramDiagnosticHandler());
                        ch.pipeline().addLast("quicServerCodec", quicServerCodecBuilder.build());
                    }
                });

        InetSocketAddress bindAddr = connBuilder.host() != null
                ? new InetSocketAddress(connBuilder.host(), connBuilder.port())
                : new InetSocketAddress(connBuilder.port());
        ChannelFuture bindFuture = b.bind(bindAddr);
        bindFuture.addListener(future -> {
            if (future.isSuccess()) {
                log.info("QUIC listener bound successfully: localAddress={}", bindAddr);
            } else {
                log.error("QUIC listener bind failed: localAddress={}", bindAddr, future.cause());
            }
        });
        return bindFuture;
    }

    private static final class QUICDatagramDiagnosticHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof DatagramPacket packet) {
                log.trace("QUIC UDP datagram received: local={}, remote={}, bytes={}",
                    packet.recipient(), packet.sender(), packet.content().readableBytes());
            }
            super.channelRead(ctx, msg);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.warn("QUIC UDP datagram channel exception: local={}", ctx.channel().localAddress(), cause);
            ctx.fireExceptionCaught(cause);
        }
    }
}
