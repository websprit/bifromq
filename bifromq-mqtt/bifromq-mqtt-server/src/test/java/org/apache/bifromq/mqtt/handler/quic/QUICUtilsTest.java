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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link QUICUtils} — TCP path only.
 * <p>
 * QUIC path tests require a real QuicStreamChannel which can only be obtained
 * from an actual QUIC connection. Those are covered by integration tests.
 */
public class QUICUtilsTest {

    @Test
    void testIsQuicWithTcpChannel() {
        EmbeddedChannel ch = new EmbeddedChannel();
        assertFalse(QUICUtils.isQuic(ch));
        ch.close();
    }

    @Test
    void testConnectionChannelWithTcpChannel() {
        EmbeddedChannel ch = new EmbeddedChannel();
        Channel result = QUICUtils.connectionChannel(ch);
        assertEquals(result, ch, "For TCP, connectionChannel should return the channel itself");
        ch.close();
    }

    @Test
    void testConnectionIdWithTcpChannel() {
        EmbeddedChannel ch = new EmbeddedChannel();
        String id = QUICUtils.connectionId(ch);
        assertNotNull(id);
        assertEquals(id, ch.id().asLongText());
        ch.close();
    }
}
