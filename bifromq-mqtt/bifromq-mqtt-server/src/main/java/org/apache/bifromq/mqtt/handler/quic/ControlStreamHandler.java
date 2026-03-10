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
 * Pipeline handler for the QUIC control stream (Stream 0).
 * <p>
 * Filters inbound messages to only accept control-plane MQTT messages:
 * <ul>
 * <li>CONNECT / CONNACK</li>
 * <li>SUBSCRIBE / SUBACK</li>
 * <li>UNSUBSCRIBE / UNSUBACK</li>
 * <li>PINGREQ / PINGRESP</li>
 * <li>DISCONNECT</li>
 * <li>AUTH (MQTT 5.0)</li>
 * </ul>
 * <p>
 * Data-plane messages (PUBLISH, PUBACK, etc.) received on the control stream
 * are logged as warnings and dropped.
 */
@Slf4j
public class ControlStreamHandler extends ChannelInboundHandlerAdapter {

    public static final String NAME = "controlStreamFilter";

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof MqttMessage mqttMsg) {
            MqttMessageType type = mqttMsg.fixedHeader().messageType();
            if (isControlMessage(type)) {
                ctx.fireChannelRead(msg);
            } else {
                log.warn("Received data-plane message {} on control stream, dropping", type);
                // TODO: optionally send DISCONNECT with protocol error reason code
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private boolean isControlMessage(MqttMessageType type) {
        return switch (type) {
            case CONNECT, CONNACK,
                    SUBSCRIBE, SUBACK,
                    UNSUBSCRIBE, UNSUBACK,
                    PINGREQ, PINGRESP,
                    DISCONNECT, AUTH ->
                true;
            default -> false;
        };
    }
}
