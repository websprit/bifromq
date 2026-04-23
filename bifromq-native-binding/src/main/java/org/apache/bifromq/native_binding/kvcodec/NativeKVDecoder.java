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

package org.apache.bifromq.native_binding.kvcodec;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import org.apache.bifromq.native_binding.NativeLoader;

/**
 * Java FFM bindings for Rust KV key decoding functions.
 * <p>
 * Returns zero-copy offsets into the original key buffer, enabling the caller
 * to slice ByteString instances without additional copying.
 */
public final class NativeKVDecoder {
    private static final Linker LINKER = Linker.nativeLinker();

    // DecodedRouteKey struct layout (must match Rust #[repr(C)]):
    // { tenant_id_offset: u32, tenant_id_len: u32, flag: u8, padding[3],
    //   payload_offset: u32, payload_len: u32 }
    private static final MemoryLayout DECODED_ROUTE_KEY_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.JAVA_INT.withName("tenant_id_offset"),
        ValueLayout.JAVA_INT.withName("tenant_id_len"),
        ValueLayout.JAVA_BYTE.withName("flag"),
        MemoryLayout.paddingLayout(3),
        ValueLayout.JAVA_INT.withName("payload_offset"),
        ValueLayout.JAVA_INT.withName("payload_len")
    );

    private static final MethodHandle DECODE_ROUTE_KEY;
    private static final MethodHandle DECODE_TENANT_ID;
    private static final MethodHandle DECODE_FLAG;

    static {
        var symbols = NativeLoader.symbols();

        DECODE_ROUTE_KEY = LINKER.downcallHandle(
            symbols.find("kv_decode_route_key").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,       // return: 1=ok, 0=invalid
                ValueLayout.ADDRESS,        // key_ptr
                ValueLayout.JAVA_INT,       // key_len
                ValueLayout.ADDRESS         // out: DecodedRouteKey*
            )
        );

        DECODE_TENANT_ID = LINKER.downcallHandle(
            symbols.find("kv_decode_tenant_id").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,       // return: tenant_id_len (0=invalid)
                ValueLayout.ADDRESS,        // key_ptr
                ValueLayout.JAVA_INT,       // key_len
                ValueLayout.ADDRESS         // out_offset ptr
            )
        );

        DECODE_FLAG = LINKER.downcallHandle(
            symbols.find("kv_decode_flag").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_BYTE,      // return: flag or 0xFF
                ValueLayout.ADDRESS,        // key_ptr
                ValueLayout.JAVA_INT        // key_len
            )
        );
    }

    /**
     * Decoded route key result.
     */
    public record DecodedRouteKey(
        int tenantIdOffset, int tenantIdLen,
        byte flag,
        int payloadOffset, int payloadLen
    ) {
    }

    /**
     * Decode a route key into its components.
     *
     * @return decoded result, or null if the key is invalid
     */
    public static DecodedRouteKey decodeRouteKey(byte[] key) {
        try (var arena = Arena.ofConfined()) {
            var keySeg = arena.allocateFrom(ValueLayout.JAVA_BYTE, key);
            var outSeg = arena.allocate(DECODED_ROUTE_KEY_LAYOUT);

            int result = (int) DECODE_ROUTE_KEY.invokeExact(keySeg, key.length, outSeg);
            if (result == 0) {
                return null;
            }

            return new DecodedRouteKey(
                outSeg.get(ValueLayout.JAVA_INT, 0),      // tenant_id_offset
                outSeg.get(ValueLayout.JAVA_INT, 4),      // tenant_id_len
                outSeg.get(ValueLayout.JAVA_BYTE, 8),     // flag
                outSeg.get(ValueLayout.JAVA_INT, 12),     // payload_offset
                outSeg.get(ValueLayout.JAVA_INT, 16)      // payload_len
            );
        } catch (Throwable t) {
            throw new RuntimeException("kv_decode_route_key failed", t);
        }
    }

    /**
     * Decode only the tenant ID from a key (fast path).
     *
     * @return [offset, length] or null if invalid
     */
    public static int[] decodeTenantId(byte[] key) {
        try (var arena = Arena.ofConfined()) {
            var keySeg = arena.allocateFrom(ValueLayout.JAVA_BYTE, key);
            var outOffset = arena.allocate(ValueLayout.JAVA_INT);

            int len = (int) DECODE_TENANT_ID.invokeExact(keySeg, key.length, outOffset);
            if (len == 0) {
                return null;
            }
            return new int[]{outOffset.get(ValueLayout.JAVA_INT, 0), len};
        } catch (Throwable t) {
            throw new RuntimeException("kv_decode_tenant_id failed", t);
        }
    }

    /**
     * Decode the flag byte from a route key.
     *
     * @return flag byte, or -1 if invalid
     */
    public static int decodeFlag(byte[] key) {
        try (var arena = Arena.ofConfined()) {
            var keySeg = arena.allocateFrom(ValueLayout.JAVA_BYTE, key);
            byte flag = (byte) DECODE_FLAG.invokeExact(keySeg, key.length);
            return flag == (byte) 0xFF ? -1 : flag & 0xFF;
        } catch (Throwable t) {
            throw new RuntimeException("kv_decode_flag failed", t);
        }
    }

    private NativeKVDecoder() {
    }
}
