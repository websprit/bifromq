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

import static org.testng.Assert.assertNotNull;

import org.testng.annotations.Test;

/**
 * Unit tests for {@link QUICConnectionHandler}.
 * <p>
 * Note: Full handler tests require a real QuicChannel which can only be
 * obtained
 * from an actual QUIC transport. MQTTSessionContext is a final/builder class
 * that
 * cannot be mocked with Mockito on Java 25. Attribute propagation is verified
 * in end-to-end integration tests.
 */
public class QUICConnectionHandlerTest {

    @Test
    void testAttributeKeysDefined() {
        // Verify the attribute keys are defined correctly and not null
        assertNotNull(QUICConnectionHandler.QUIC_PEER_ADDR,
                "QUIC_PEER_ADDR attribute key should be defined");
        assertNotNull(QUICConnectionHandler.QUIC_CLIENT_CERTS,
                "QUIC_CLIENT_CERTS attribute key should be defined");
    }
}
