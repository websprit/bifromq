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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import java.net.InetSocketAddress;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for handling QUIC/TCP transport differences.
 * <p>
 * This class abstracts the transport layer so that handler code can work
 * uniformly with both TCP SocketChannels and QUIC StreamChannels.
 */
@Slf4j
public final class QUICUtils {

    private QUICUtils() {
        // utility class
    }

    /**
     * Check if the channel is a QUIC stream channel.
     */
    public static boolean isQuic(Channel ch) {
        return ch instanceof QuicStreamChannel;
    }

    /**
     * Get the connection-level channel.
     * For TCP: returns the channel itself.
     * For QUIC: returns the parent QuicChannel.
     */
    public static Channel connectionChannel(Channel ch) {
        if (ch instanceof QuicStreamChannel streamChannel) {
            return streamChannel.parent();
        }
        return ch;
    }

    /**
     * Get the remote address of the peer.
     * For TCP: uses channel.remoteAddress() directly.
     * For QUIC: retrieves from the parent QuicChannel since
     * QuicStreamChannel.remoteAddress() may return null.
     */
    public static InetSocketAddress remoteAddress(Channel ch) {
        if (ch instanceof QuicStreamChannel streamChannel) {
            QuicChannel parent = streamChannel.parent();
            return (InetSocketAddress) parent.remoteSocketAddress();
        }
        return (InetSocketAddress) ch.remoteAddress();
    }

    /**
     * Close the entire connection.
     * For TCP: closes the channel directly.
     * For QUIC: closes the parent QuicChannel to terminate the entire connection,
     * not just the current stream.
     */
    public static void closeConnection(ChannelHandlerContext ctx) {
        Channel ch = ctx.channel();
        if (ch instanceof QuicStreamChannel streamChannel) {
            QuicChannel quicChannel = streamChannel.parent();
            log.debug("Closing QUIC connection: {}", quicChannel.remoteAddress());
            quicChannel.close();
        } else {
            ctx.close();
        }
    }

    /**
     * Close the connection after writing and flushing a message.
     * For TCP: writes, flushes, then closes the channel.
     * For QUIC: writes, flushes to the stream, then closes the parent QuicChannel.
     */
    public static void writeFlushAndCloseConnection(ChannelHandlerContext ctx, Object msg) {
        Channel ch = ctx.channel();
        if (ch instanceof QuicStreamChannel streamChannel) {
            ctx.writeAndFlush(msg).addListener(future -> {
                QuicChannel quicChannel = streamChannel.parent();
                log.debug("Closing QUIC connection after flush: {}", quicChannel.remoteAddress());
                quicChannel.close();
            });
        } else {
            ctx.writeAndFlush(msg).addListener(io.netty.channel.ChannelFutureListener.CLOSE);
        }
    }

    /**
     * Get the connection-level channel ID (stable across streams).
     * For TCP: returns the channel's own ID.
     * For QUIC: returns the parent QuicChannel's ID so all streams
     * in the same connection share the same identifier.
     */
    public static String connectionId(Channel ch) {
        return connectionChannel(ch).id().asLongText();
    }

    /**
     * Set autoRead on the connection-level channel.
     * For TCP: sets autoRead directly on the channel.
     * For QUIC streams: sets autoRead on the parent QuicChannel,
     * which provides connection-level backpressure affecting all streams.
     */
    public static void setAutoReadAll(Channel ch, boolean autoRead) {
        Channel conn = connectionChannel(ch);
        conn.config().setAutoRead(autoRead);
    }
}
