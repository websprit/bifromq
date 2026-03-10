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

import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Routes MQTT messages to appropriate QUIC streams in multi-stream mode.
 * <p>
 * Stream Layout:
 * <ul>
 * <li>Stream 0 (Control): CONNECT, CONNACK, SUBSCRIBE, SUBACK, UNSUBSCRIBE,
 * UNSUBACK,
 * PINGREQ, PINGRESP, DISCONNECT</li>
 * <li>Stream 1~N (Data): PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP.
 * Topic → Stream mapping via consistent hashing.</li>
 * </ul>
 * <p>
 * Default data stream count is 16, balancing isolation vs resource overhead.
 */
@Slf4j
public class QUICStreamRouter {

    public static final int DEFAULT_DATA_STREAM_COUNT = 16;

    private final QuicChannel quicConnection;
    private final int dataStreamCount;
    private volatile QuicStreamChannel controlStream;
    private final QuicStreamChannel[] dataStreams;
    private final Map<Integer, Integer> packetIdToStreamIndex = new ConcurrentHashMap<>();

    public QUICStreamRouter(QuicChannel quicConnection) {
        this(quicConnection, DEFAULT_DATA_STREAM_COUNT);
    }

    public QUICStreamRouter(QuicChannel quicConnection, int dataStreamCount) {
        this.quicConnection = quicConnection;
        this.dataStreamCount = dataStreamCount;
        this.dataStreams = new QuicStreamChannel[dataStreamCount];
    }

    /**
     * Returns the control stream (Stream 0).
     * The control stream is used for CONNECT/CONNACK, SUBSCRIBE/SUBACK,
     * UNSUBSCRIBE/UNSUBACK, PINGREQ/PINGRESP, and DISCONNECT messages.
     */
    public QuicStreamChannel controlStream() {
        return controlStream;
    }

    /**
     * Sets the control stream. Called when Stream 0 is established.
     */
    public void setControlStream(QuicStreamChannel stream) {
        this.controlStream = stream;
    }

    /**
     * Resolves the data stream for a given topic using consistent hashing.
     *
     * @param topic the MQTT topic
     * @return the QuicStreamChannel to use for this topic
     */
    public QuicStreamChannel resolveStream(String topic) {
        int bucket = Math.abs(topic.hashCode() % dataStreamCount);
        return dataStreams[bucket];
    }

    /**
     * Resolves the data stream index for a given topic.
     *
     * @param topic the MQTT topic
     * @return the stream bucket index
     */
    public int resolveStreamIndex(String topic) {
        return Math.abs(topic.hashCode() % dataStreamCount);
    }

    /**
     * Registers a data stream at the specified index.
     */
    public void setDataStream(int index, QuicStreamChannel stream) {
        if (index < 0 || index >= dataStreamCount) {
            throw new IllegalArgumentException("Stream index out of range: " + index);
        }
        this.dataStreams[index] = stream;
    }

    /**
     * Gets the data stream at the specified index.
     */
    public QuicStreamChannel getDataStream(int index) {
        return dataStreams[index];
    }

    /**
     * Registers a packetId → stream index mapping.
     * Used for routing PUBACK/PUBREC/PUBREL/PUBCOMP responses
     * back to the correct data stream.
     */
    public void registerPacketId(int packetId, int streamIndex) {
        packetIdToStreamIndex.put(packetId, streamIndex);
    }

    /**
     * Resolves the data stream for a given packetId.
     * Used for routing QoS acknowledgment messages.
     *
     * @param packetId the MQTT packet ID
     * @return the QuicStreamChannel, or the control stream if not found
     */
    public QuicStreamChannel resolveByPacketId(int packetId) {
        Integer index = packetIdToStreamIndex.get(packetId);
        if (index != null && index >= 0 && index < dataStreamCount) {
            QuicStreamChannel stream = dataStreams[index];
            if (stream != null) {
                return stream;
            }
        }
        log.warn("No stream mapping found for packetId={}, falling back to control stream", packetId);
        return controlStream;
    }

    /**
     * Unregisters a packetId mapping after QoS handshake is complete.
     */
    public void unregisterPacketId(int packetId) {
        packetIdToStreamIndex.remove(packetId);
    }

    /**
     * Returns the number of data streams.
     */
    public int dataStreamCount() {
        return dataStreamCount;
    }

    /**
     * Checks if all data streams are initialized.
     */
    public boolean isReady() {
        if (controlStream == null) {
            return false;
        }
        for (QuicStreamChannel stream : dataStreams) {
            if (stream == null) {
                return false;
            }
        }
        return true;
    }
}
