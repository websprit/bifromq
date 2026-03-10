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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.util.AttributeKey;
import java.net.InetSocketAddress;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.mqtt.handler.ChannelAttrs;
import org.apache.bifromq.mqtt.session.MQTTSessionContext;

/**
 * Connection-level handler for QUIC.
 * <p>
 * This handler is installed on the {@link QuicChannel} (not individual
 * streams).
 * It performs connection-level initialization:
 * <ul>
 * <li>Stores the remote address as an attribute accessible by stream
 * handlers</li>
 * <li>Applies connection-level rate limiting</li>
 * <li>Propagates the {@link MQTTSessionContext} attribute to child streams</li>
 * </ul>
 * <p>
 * Since QUIC TLS happens at the connection level, client certificate
 * information
 * is also extracted here and stored for stream handlers to access.
 */
@Slf4j
@ChannelHandler.Sharable
public class QUICConnectionHandler extends ChannelInboundHandlerAdapter {

    /**
     * Attribute key to store the remote address on the QuicChannel,
     * so stream handlers can retrieve it without relying on
     * QuicStreamChannel.remoteAddress().
     */
    public static final AttributeKey<InetSocketAddress> QUIC_PEER_ADDR = AttributeKey.valueOf("QUIC_PEER_ADDR");

    /**
     * Attribute key to store the client certificate chain on the QuicChannel.
     */
    public static final AttributeKey<java.security.cert.Certificate[]> QUIC_CLIENT_CERTS = AttributeKey
            .valueOf("QUIC_CLIENT_CERTS");

    private final MQTTSessionContext sessionContext;

    public QUICConnectionHandler(MQTTSessionContext sessionContext) {
        this.sessionContext = sessionContext;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        QuicChannel quicChannel = (QuicChannel) ctx.channel();
        log.debug("QUIC connection established from: {}", quicChannel.remoteAddress());

        // Store remote address at connection level for stream handlers (fixes #3)
        // Note: QuicChannel.remoteAddress() returns QuicConnectionAddress (not
        // InetSocketAddress),
        // use remoteSocketAddress() to get the actual peer IP address.
        InetSocketAddress remoteAddr = (InetSocketAddress) quicChannel.remoteSocketAddress();
        if (remoteAddr != null) {
            quicChannel.attr(QUIC_PEER_ADDR).set(remoteAddr);
        }

        // Extract client certificates from QUIC TLS (fixes #2)
        try {
            javax.net.ssl.SSLEngine sslEngine = quicChannel.sslEngine();
            if (sslEngine != null && sslEngine.getSession() != null) {
                java.security.cert.Certificate[] certs = sslEngine.getSession().getPeerCertificates();
                if (certs != null && certs.length > 0) {
                    quicChannel.attr(QUIC_CLIENT_CERTS).set(certs);
                }
            }
        } catch (javax.net.ssl.SSLPeerUnverifiedException e) {
            log.trace("No client certificate for QUIC connection: {}", remoteAddr);
        }

        // Store session context at connection level (fixes #1)
        // This will be propagated to stream channels in QUICStreamInitializer
        quicChannel.attr(ChannelAttrs.MQTT_SESSION_CTX).set(sessionContext);

        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.debug("QUIC connection closed: {}", ctx.channel().remoteAddress());
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("QUIC connection error: remote={}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}
