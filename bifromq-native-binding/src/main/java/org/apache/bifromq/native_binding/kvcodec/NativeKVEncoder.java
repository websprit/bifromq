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
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import org.apache.bifromq.native_binding.NativeLoader;

/**
 * Java FFM bindings for Rust KV key encoding functions.
 * <p>
 * Binary layout matches {@code KVSchemaUtil.java} exactly.
 */
public final class NativeKVEncoder {
    private static final Linker LINKER = Linker.nativeLinker();

    private static final MethodHandle ENCODE_TENANT_BEGIN;
    private static final MethodHandle ENCODE_NORMAL_ROUTE;
    private static final MethodHandle ENCODE_GROUP_ROUTE;
    private static final MethodHandle ENCODE_ROUTE_START;

    static {
        var symbols = NativeLoader.symbols();

        ENCODE_TENANT_BEGIN = LINKER.downcallHandle(
            symbols.find("kv_encode_tenant_begin_key").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,       // return: bytes written or negative
                ValueLayout.ADDRESS,        // tenant_ptr
                ValueLayout.JAVA_INT,       // tenant_len
                ValueLayout.ADDRESS,        // out_buf
                ValueLayout.JAVA_INT        // buf_cap
            )
        );

        ENCODE_NORMAL_ROUTE = LINKER.downcallHandle(
            symbols.find("kv_encode_normal_route_key").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,        // tenant_ptr
                ValueLayout.JAVA_INT,       // tenant_len
                ValueLayout.ADDRESS,        // filter_levels_data_ptr
                ValueLayout.JAVA_INT,       // filter_levels_data_len
                ValueLayout.ADDRESS,        // receiver_ptr
                ValueLayout.JAVA_INT,       // receiver_len
                ValueLayout.ADDRESS,        // out_buf
                ValueLayout.JAVA_INT        // buf_cap
            )
        );

        // Group route key takes an extra is_ordered (u8) parameter
        ENCODE_GROUP_ROUTE = LINKER.downcallHandle(
            symbols.find("kv_encode_group_route_key").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,       // return
                ValueLayout.ADDRESS,        // tenant_ptr
                ValueLayout.JAVA_INT,       // tenant_len
                ValueLayout.ADDRESS,        // filter_levels_data_ptr
                ValueLayout.JAVA_INT,       // filter_levels_data_len
                ValueLayout.ADDRESS,        // group_ptr
                ValueLayout.JAVA_INT,       // group_len
                ValueLayout.JAVA_BYTE,      // is_ordered (u8: 1=ordered, 0=unordered)
                ValueLayout.ADDRESS,        // out_buf
                ValueLayout.JAVA_INT        // buf_cap
            )
        );

        ENCODE_ROUTE_START = LINKER.downcallHandle(
            symbols.find("kv_encode_route_start_key").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,       // return: bytes written or negative
                ValueLayout.ADDRESS,        // tenant_ptr
                ValueLayout.JAVA_INT,       // tenant_len
                ValueLayout.ADDRESS,        // filter_levels_data_ptr
                ValueLayout.JAVA_INT,       // filter_levels_data_len
                ValueLayout.ADDRESS,        // out_buf
                ValueLayout.JAVA_INT        // buf_cap
            )
        );
    }

    /**
     * Encode a tenant begin key.
     * <p>
     * Layout: {@code [SCHEMA_VER(0x00)] [short:tenantLen] [tenantId]}
     */
    public static byte[] encodeTenantBeginKey(byte[] tenantId) {
        int cap = tenantId.length + 16;
        try (var arena = Arena.ofConfined()) {
            var tenantSeg = arena.allocateFrom(ValueLayout.JAVA_BYTE, tenantId);
            var outBuf = arena.allocate(cap);
            int written = (int) ENCODE_TENANT_BEGIN.invokeExact(
                tenantSeg, tenantId.length, outBuf, cap
            );
            if (written < 0) {
                int needed = -written;
                outBuf = arena.allocate(needed);
                written = (int) ENCODE_TENANT_BEGIN.invokeExact(
                    tenantSeg, tenantId.length, outBuf, needed
                );
            }
            return outBuf.asSlice(0, written).toArray(ValueLayout.JAVA_BYTE);
        } catch (Throwable t) {
            throw new RuntimeException("kv_encode_tenant_begin_key failed", t);
        }
    }

    /**
     * Encode a normal route key.
     *
     * @param tenantId         tenant ID bytes (UTF-8)
     * @param filterLevelsData pre-encoded filter levels, each followed by 0x00
     *                         e.g. for ["a","b","c"]: "a\0b\0c\0"
     * @param receiverUrl      receiver URL bytes (UTF-8)
     */
    public static byte[] encodeNormalRouteKey(byte[] tenantId, byte[] filterLevelsData, byte[] receiverUrl) {
        int cap = tenantId.length + filterLevelsData.length + receiverUrl.length + 16;
        try (var arena = Arena.ofConfined()) {
            var tenantSeg = arena.allocateFrom(ValueLayout.JAVA_BYTE, tenantId);
            var filterSeg = arena.allocateFrom(ValueLayout.JAVA_BYTE, filterLevelsData);
            var receiverSeg = arena.allocateFrom(ValueLayout.JAVA_BYTE, receiverUrl);
            var outBuf = arena.allocate(cap);
            int written = (int) ENCODE_NORMAL_ROUTE.invokeExact(
                tenantSeg, tenantId.length,
                filterSeg, filterLevelsData.length,
                receiverSeg, receiverUrl.length,
                outBuf, cap
            );
            if (written < 0) {
                int needed = -written;
                outBuf = arena.allocate(needed);
                written = (int) ENCODE_NORMAL_ROUTE.invokeExact(
                    tenantSeg, tenantId.length,
                    filterSeg, filterLevelsData.length,
                    receiverSeg, receiverUrl.length,
                    outBuf, needed
                );
            }
            return outBuf.asSlice(0, written).toArray(ValueLayout.JAVA_BYTE);
        } catch (Throwable t) {
            throw new RuntimeException("kv_encode_normal_route_key failed", t);
        }
    }

    /**
     * Encode a group route key.
     *
     * @param tenantId         tenant ID bytes (UTF-8)
     * @param filterLevelsData pre-encoded filter levels (each followed by 0x00)
     * @param group            group name bytes (UTF-8)
     * @param isOrdered        true for ordered share, false for unordered
     */
    public static byte[] encodeGroupRouteKey(byte[] tenantId, byte[] filterLevelsData,
                                             byte[] group, boolean isOrdered) {
        int cap = tenantId.length + filterLevelsData.length + group.length + 16;
        try (var arena = Arena.ofConfined()) {
            var tenantSeg = arena.allocateFrom(ValueLayout.JAVA_BYTE, tenantId);
            var filterSeg = arena.allocateFrom(ValueLayout.JAVA_BYTE, filterLevelsData);
            var groupSeg = arena.allocateFrom(ValueLayout.JAVA_BYTE, group);
            var outBuf = arena.allocate(cap);
            byte orderedFlag = isOrdered ? (byte) 1 : (byte) 0;
            int written = (int) ENCODE_GROUP_ROUTE.invokeExact(
                tenantSeg, tenantId.length,
                filterSeg, filterLevelsData.length,
                groupSeg, group.length,
                orderedFlag,
                outBuf, cap
            );
            if (written < 0) {
                int needed = -written;
                outBuf = arena.allocate(needed);
                written = (int) ENCODE_GROUP_ROUTE.invokeExact(
                    tenantSeg, tenantId.length,
                    filterSeg, filterLevelsData.length,
                    groupSeg, group.length,
                    orderedFlag,
                    outBuf, needed
                );
            }
            return outBuf.asSlice(0, written).toArray(ValueLayout.JAVA_BYTE);
        } catch (Throwable t) {
            throw new RuntimeException("kv_encode_group_route_key failed", t);
        }
    }

    /**
     * Encode a tenant route start key (for range scans).
     * <p>
     * Layout: {@code [tenantPrefix] [level1] [0x00] ... [levelN] [0x00] [0x00]}
     *
     * @param tenantId         tenant ID bytes (UTF-8)
     * @param filterLevelsData pre-encoded filter levels (each followed by 0x00)
     */
    public static byte[] encodeRouteStartKey(byte[] tenantId, byte[] filterLevelsData) {
        int cap = tenantId.length + filterLevelsData.length + 16;
        try (var arena = Arena.ofConfined()) {
            var tenantSeg = arena.allocateFrom(ValueLayout.JAVA_BYTE, tenantId);
            var filterSeg = arena.allocateFrom(ValueLayout.JAVA_BYTE, filterLevelsData);
            var outBuf = arena.allocate(cap);
            int written = (int) ENCODE_ROUTE_START.invokeExact(
                tenantSeg, tenantId.length,
                filterSeg, filterLevelsData.length,
                outBuf, cap
            );
            if (written < 0) {
                int needed = -written;
                outBuf = arena.allocate(needed);
                written = (int) ENCODE_ROUTE_START.invokeExact(
                    tenantSeg, tenantId.length,
                    filterSeg, filterLevelsData.length,
                    outBuf, needed
                );
            }
            return outBuf.asSlice(0, written).toArray(ValueLayout.JAVA_BYTE);
        } catch (Throwable t) {
            throw new RuntimeException("kv_encode_route_start_key failed", t);
        }
    }

    private NativeKVEncoder() {
    }
}
