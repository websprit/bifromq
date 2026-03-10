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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import lombok.extern.slf4j.Slf4j;

/**
 * Pipeline handler for QUIC data streams (Stream 1~N).
 * <p>
 * Filters inbound messages to only accept data-plane MQTT messages:
 * <ul>
 * <li>PUBLISH</li>
 * <li>PUBACK</li>
 * <li>PUBREC</li>
 * <li>PUBREL</li>
 * <li>PUBCOMP</li>
 * </ul>
 * <p>
 * Control-plane messages (CONNECT, SUBSCRIBE, etc.) received on a data stream
 * are logged as warnings and dropped.
 */
@Slf4j
public class DataStreamHandler extends ChannelInboundHandlerAdapter {

    public static final String NAME = "dataStreamFilter";

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof MqttMessage mqttMsg) {
            MqttMessageType type = mqttMsg.fixedHeader().messageType();
            if (isDataMessage(type)) {
                ctx.fireChannelRead(msg);
            } else {
                log.warn("Received control-plane message {} on data stream, dropping", type);
                // TODO: optionally send error response
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private boolean isDataMessage(MqttMessageType type) {
        return switch (type) {
            case PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP -> true;
            default -> false;
        };
    }
}
