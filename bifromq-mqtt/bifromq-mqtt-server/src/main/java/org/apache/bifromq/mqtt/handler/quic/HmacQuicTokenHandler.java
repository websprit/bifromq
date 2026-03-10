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

import io.netty.buffer.ByteBuf;
import io.netty.incubator.codec.quic.QuicTokenHandler;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import lombok.extern.slf4j.Slf4j;

/**
 * Production-grade QUIC token handler using HMAC-SHA256 for token generation
 * and validation.
 * <p>
 * This replaces {@link io.netty.incubator.codec.quic.InsecureQuicTokenHandler}
 * to provide
 * proper address validation for QUIC Initial packets, mitigating amplification
 * attacks.
 * <p>
 * Token format (32 bytes):
 * 
 * <pre>
 *   [4 bytes: IP address] [2 bytes: port] [8 bytes: timestamp] [2 bytes: padding] [16 bytes: HMAC-SHA256 truncated]
 * </pre>
 * <p>
 * Token validity window: 60 seconds (configurable). Tokens older than this are
 * rejected,
 * forcing the client to retry with a new Initial packet.
 */
@Slf4j
public class HmacQuicTokenHandler implements QuicTokenHandler {

    private static final String HMAC_ALGO = "HmacSHA256";
    private static final int TOKEN_SIZE = 32;
    private static final int HMAC_TRUNCATED_SIZE = 16; // Truncated HMAC for compactness
    private static final int ADDR_SIZE = 4; // IPv4 address
    private static final int PORT_SIZE = 2;
    private static final int TIMESTAMP_SIZE = 8;
    private static final int PADDING_SIZE = 2;

    private final byte[] secretKey;
    private final long tokenValidityMs;

    /**
     * Creates a new HmacQuicTokenHandler with a randomly generated 256-bit secret
     * key.
     */
    public HmacQuicTokenHandler() {
        this(60_000L); // default: 60 seconds validity
    }

    /**
     * Creates a new HmacQuicTokenHandler with the specified token validity
     * duration.
     *
     * @param tokenValidityMs token validity window in milliseconds
     */
    public HmacQuicTokenHandler(long tokenValidityMs) {
        this.tokenValidityMs = tokenValidityMs;
        this.secretKey = new byte[32];
        new SecureRandom().nextBytes(this.secretKey);
        log.info("HmacQuicTokenHandler initialized with {}ms validity, key=random-256bit", tokenValidityMs);
    }

    /**
     * Creates a new HmacQuicTokenHandler with a specified secret key.
     *
     * @param secretKey       the HMAC secret key
     * @param tokenValidityMs token validity window in milliseconds
     */
    public HmacQuicTokenHandler(byte[] secretKey, long tokenValidityMs) {
        this.secretKey = secretKey.clone();
        this.tokenValidityMs = tokenValidityMs;
    }

    @Override
    public boolean writeToken(ByteBuf out, ByteBuf dcid, InetSocketAddress address) {
        try {
            byte[] addrBytes = address.getAddress().getAddress();
            int port = address.getPort();
            long timestamp = System.currentTimeMillis();

            // Build plaintext: addr + port + timestamp + padding
            ByteBuffer plain = ByteBuffer.allocate(ADDR_SIZE + PORT_SIZE + TIMESTAMP_SIZE + PADDING_SIZE);
            // For IPv6, we'd need a different approach. For now, handle IPv4 (4 bytes)
            // and zero-pad if needed.
            if (addrBytes.length == 4) {
                plain.put(addrBytes);
            } else {
                // IPv6: use last 4 bytes as a hash
                plain.put(addrBytes, addrBytes.length - 4, 4);
            }
            plain.putShort((short) port);
            plain.putLong(timestamp);
            plain.putShort((short) 0); // reserved padding
            plain.flip();

            // Compute HMAC
            Mac mac = Mac.getInstance(HMAC_ALGO);
            mac.init(new SecretKeySpec(secretKey, HMAC_ALGO));
            mac.update(plain);
            byte[] hmac = mac.doFinal();

            // Write token: plaintext + truncated HMAC
            plain.rewind();
            out.writeBytes(plain);
            out.writeBytes(hmac, 0, HMAC_TRUNCATED_SIZE);
            return true;
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            log.error("Failed to generate QUIC token", e);
            return false;
        }
    }

    @Override
    public int validateToken(ByteBuf token, InetSocketAddress address) {
        if (token.readableBytes() < TOKEN_SIZE) {
            return -1;
        }

        // Read plaintext portion
        byte[] addrBytes = new byte[ADDR_SIZE];
        token.readBytes(addrBytes);
        int port = token.readUnsignedShort();
        long timestamp = token.readLong();
        token.readShort(); // padding

        // Read HMAC
        byte[] receivedHmac = new byte[HMAC_TRUNCATED_SIZE];
        token.readBytes(receivedHmac);

        // 1. Check timestamp validity
        long now = System.currentTimeMillis();
        if (now - timestamp > tokenValidityMs) {
            log.debug("QUIC token expired: age={}ms", now - timestamp);
            return -1;
        }

        // 2. Verify address match
        byte[] clientAddr = address.getAddress().getAddress();
        byte[] clientAddrTruncated = new byte[4];
        if (clientAddr.length == 4) {
            System.arraycopy(clientAddr, 0, clientAddrTruncated, 0, 4);
        } else {
            System.arraycopy(clientAddr, clientAddr.length - 4, clientAddrTruncated, 0, 4);
        }
        if (!java.util.Arrays.equals(addrBytes, clientAddrTruncated)) {
            log.debug("QUIC token address mismatch");
            return -1;
        }
        if (port != address.getPort()) {
            log.debug("QUIC token port mismatch");
            return -1;
        }

        // 3. Verify HMAC
        try {
            ByteBuffer plain = ByteBuffer.allocate(ADDR_SIZE + PORT_SIZE + TIMESTAMP_SIZE + PADDING_SIZE);
            plain.put(addrBytes);
            plain.putShort((short) port);
            plain.putLong(timestamp);
            plain.putShort((short) 0);
            plain.flip();

            Mac mac = Mac.getInstance(HMAC_ALGO);
            mac.init(new SecretKeySpec(secretKey, HMAC_ALGO));
            mac.update(plain);
            byte[] expectedHmac = mac.doFinal();

            // Constant-time comparison (truncated)
            if (!constantTimeEquals(receivedHmac, expectedHmac, HMAC_TRUNCATED_SIZE)) {
                log.debug("QUIC token HMAC verification failed");
                return -1;
            }

            // Token is valid — return offset past the consumed token bytes
            return TOKEN_SIZE;
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            log.error("Failed to validate QUIC token", e);
            return -1;
        }
    }

    @Override
    public int maxTokenLength() {
        return TOKEN_SIZE;
    }

    /**
     * Constant-time byte array comparison to prevent timing attacks.
     */
    private static boolean constantTimeEquals(byte[] a, byte[] b, int length) {
        int result = 0;
        for (int i = 0; i < length; i++) {
            result |= a[i] ^ b[i];
        }
        return result == 0;
    }
}
