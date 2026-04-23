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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.bifromq.native_binding.NativeArenaPool;
import org.apache.bifromq.native_binding.NativeLoader;

/**
 * Java FFM bindings for batch KV key encoding/decoding via Rust native library.
 * <p>
 * Uses a flat-buffer protocol for efficient batch data transfer across FFI:
 * <pre>
 * Input/Output: [u32:len_1][bytes_1][u32:len_2][bytes_2]...
 * </pre>
 * All u32 lengths are little-endian.
 */
public final class NativeKVBatchEncoder {
    private static final Linker LINKER = Linker.nativeLinker();

    private static final MethodHandle BATCH_ENCODE_NORMAL;
    private static final MethodHandle BATCH_ENCODE_GROUP;
    private static final MethodHandle BATCH_DECODE;

    static {
        var symbols = NativeLoader.symbols();

        BATCH_ENCODE_NORMAL = LINKER.downcallHandle(
            symbols.find("kv_batch_encode_normal_route_keys").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,       // return: bytes written or negative
                ValueLayout.ADDRESS,        // tenant_ptr
                ValueLayout.JAVA_INT,       // tenant_len
                ValueLayout.ADDRESS,        // filter_levels_data_ptr
                ValueLayout.JAVA_INT,       // filter_levels_data_len
                ValueLayout.ADDRESS,        // receivers_buf_ptr
                ValueLayout.JAVA_INT,       // receivers_buf_len
                ValueLayout.ADDRESS,        // out_buf
                ValueLayout.JAVA_INT        // buf_cap
            )
        );

        BATCH_ENCODE_GROUP = LINKER.downcallHandle(
            symbols.find("kv_batch_encode_group_route_keys").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,        // tenant_ptr
                ValueLayout.JAVA_INT,       // tenant_len
                ValueLayout.ADDRESS,        // filter_levels_data_ptr
                ValueLayout.JAVA_INT,       // filter_levels_data_len
                ValueLayout.ADDRESS,        // groups_buf_ptr
                ValueLayout.JAVA_INT,       // groups_buf_len
                ValueLayout.ADDRESS,        // out_buf
                ValueLayout.JAVA_INT        // buf_cap
            )
        );

        BATCH_DECODE = LINKER.downcallHandle(
            symbols.find("kv_batch_decode_route_keys").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,       // return: count
                ValueLayout.ADDRESS,        // keys_buf_ptr
                ValueLayout.JAVA_INT,       // keys_buf_len
                ValueLayout.ADDRESS,        // out_decoded
                ValueLayout.JAVA_INT        // out_cap
            )
        );
    }

    /**
     * Batch encode normal route keys sharing the same tenant and filter levels.
     *
     * @param tenantId         tenant ID bytes (UTF-8)
     * @param filterLevelsData pre-encoded filter levels (each followed by 0x00)
     * @param receivers        list of receiver URL bytes
     * @return list of encoded route key byte arrays
     */
    public static List<byte[]> batchEncodeNormalRouteKeys(
            byte[] tenantId, byte[] filterLevelsData, List<byte[]> receivers) {
        // Build flat input buffer: [u32:len][bytes]...
        int inputSize = receivers.stream().mapToInt(r -> 4 + r.length).sum();
        byte[] inputBuf = new byte[inputSize];
        ByteBuffer bb = ByteBuffer.wrap(inputBuf).order(ByteOrder.LITTLE_ENDIAN);
        for (byte[] recv : receivers) {
            bb.putInt(recv.length);
            bb.put(recv);
        }

        int cap = inputSize + receivers.size() * (tenantId.length + filterLevelsData.length + 16);
        try {
            var ctx = NativeArenaPool.get();
            var tenantSeg = ctx.allocateFrom(tenantId);
            var filterSeg = ctx.allocateFrom(filterLevelsData);
            var inputSeg = ctx.input(inputSize);
            inputSeg.copyFrom(MemorySegment.ofArray(inputBuf));
            var outBuf = ctx.output(cap);

            int written = (int) BATCH_ENCODE_NORMAL.invokeExact(
                tenantSeg, tenantId.length,
                filterSeg, filterLevelsData.length,
                inputSeg, inputSize,
                outBuf, cap
            );

            if (written < 0) {
                int needed = -written;
                outBuf = ctx.output(needed);
                written = (int) BATCH_ENCODE_NORMAL.invokeExact(
                    tenantSeg, tenantId.length,
                    filterSeg, filterLevelsData.length,
                    inputSeg, inputSize,
                    outBuf, needed
                );
            }

            return parseFlatBuffer(outBuf, written);
        } catch (Throwable t) {
            throw new RuntimeException("kv_batch_encode_normal_route_keys failed", t);
        }
    }

    /**
     * Batch encode group route keys sharing the same tenant and filter levels.
     *
     * @param tenantId         tenant ID bytes (UTF-8)
     * @param filterLevelsData pre-encoded filter levels (each followed by 0x00)
     * @param groups           list of group name bytes
     * @param ordered          list of ordered flags (parallel with groups)
     * @return list of encoded route key byte arrays
     */
    public static List<byte[]> batchEncodeGroupRouteKeys(
            byte[] tenantId, byte[] filterLevelsData,
            List<byte[]> groups, List<Boolean> ordered) {
        // Build flat input buffer: [u32:len][bytes][u8:ordered]...
        int inputSize = groups.stream().mapToInt(g -> 4 + g.length + 1).sum();
        byte[] inputBuf = new byte[inputSize];
        ByteBuffer bb = ByteBuffer.wrap(inputBuf).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < groups.size(); i++) {
            byte[] grp = groups.get(i);
            bb.putInt(grp.length);
            bb.put(grp);
            bb.put(ordered.get(i) ? (byte) 1 : (byte) 0);
        }

        int cap = inputSize + groups.size() * (tenantId.length + filterLevelsData.length + 16);
        try {
            var ctx = NativeArenaPool.get();
            var tenantSeg = ctx.allocateFrom(tenantId);
            var filterSeg = ctx.allocateFrom(filterLevelsData);
            var inputSeg = ctx.input(inputSize);
            inputSeg.copyFrom(MemorySegment.ofArray(inputBuf));
            var outBuf = ctx.output(cap);

            int written = (int) BATCH_ENCODE_GROUP.invokeExact(
                tenantSeg, tenantId.length,
                filterSeg, filterLevelsData.length,
                inputSeg, inputSize,
                outBuf, cap
            );

            if (written < 0) {
                int needed = -written;
                outBuf = ctx.output(needed);
                written = (int) BATCH_ENCODE_GROUP.invokeExact(
                    tenantSeg, tenantId.length,
                    filterSeg, filterLevelsData.length,
                    inputSeg, inputSize,
                    outBuf, needed
                );
            }

            return parseFlatBuffer(outBuf, written);
        } catch (Throwable t) {
            throw new RuntimeException("kv_batch_encode_group_route_keys failed", t);
        }
    }

    /**
     * Decoded route key result from batch decoding.
     */
    public record DecodedKey(int tenantIdOffset, int tenantIdLen, byte flag,
                             int payloadOffset, int payloadLen) {
    }

    /**
     * Batch decode route keys.
     *
     * @param keys list of route key byte arrays to decode
     * @return list of decoded route key components
     */
    public static List<DecodedKey> batchDecodeRouteKeys(List<byte[]> keys) {
        // Build flat input buffer
        int inputSize = keys.stream().mapToInt(k -> 4 + k.length).sum();
        byte[] inputBuf = new byte[inputSize];
        ByteBuffer bb = ByteBuffer.wrap(inputBuf).order(ByteOrder.LITTLE_ENDIAN);
        for (byte[] key : keys) {
            bb.putInt(key.length);
            bb.put(key);
        }

        // DecodedRouteKey struct is 20 bytes (5 x u32: tenant_offset, tenant_len, flag(u8+padding), payload_offset, payload_len)
        // Actually it's: u32 + u32 + u8 + u32 + u32 = 17 bytes, but with C alignment it's 20 bytes
        int structSize = 20;
        int outCap = keys.size();

        try {
            var ctx = NativeArenaPool.get();
            var inputSeg = ctx.input(inputSize);
            inputSeg.copyFrom(MemorySegment.ofArray(inputBuf));
            var outSeg = ctx.output((long) structSize * outCap);

            int count = (int) BATCH_DECODE.invokeExact(
                inputSeg, inputSize,
                outSeg, outCap
            );

            List<DecodedKey> result = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                long base = (long) i * structSize;
                int tenantOffset = outSeg.get(ValueLayout.JAVA_INT, base);
                int tenantLen = outSeg.get(ValueLayout.JAVA_INT, base + 4);
                byte flag = outSeg.get(ValueLayout.JAVA_BYTE, base + 8);
                int payloadOffset = outSeg.get(ValueLayout.JAVA_INT, base + 12);
                int payloadLen = outSeg.get(ValueLayout.JAVA_INT, base + 16);
                result.add(new DecodedKey(tenantOffset, tenantLen, flag, payloadOffset, payloadLen));
            }
            return result;
        } catch (Throwable t) {
            throw new RuntimeException("kv_batch_decode_route_keys failed", t);
        }
    }

    /**
     * Parse flat buffer output format: [u32:len][bytes]...
     */
    private static List<byte[]> parseFlatBuffer(MemorySegment seg, int totalLen) {
        List<byte[]> result = new ArrayList<>();
        int pos = 0;
        while (pos + 4 <= totalLen) {
            int len = seg.get(ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN), pos);
            pos += 4;
            byte[] entry = seg.asSlice(pos, len).toArray(ValueLayout.JAVA_BYTE);
            result.add(entry);
            pos += len;
        }
        return result;
    }

    private NativeKVBatchEncoder() {
    }
}
