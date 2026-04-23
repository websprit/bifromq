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

package org.apache.bifromq.native_binding.compressor;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import org.apache.bifromq.native_binding.NativeArenaPool;
import org.apache.bifromq.native_binding.NativeLoader;

/**
 * Java FFM binding for Rust GZIP compress/decompress using flate2 (zlib-ng backend).
 */
public final class NativeCompressor {
    private static final Linker LINKER = Linker.nativeLinker();

    private static final MethodHandle GZIP_COMPRESS;
    private static final MethodHandle GZIP_DECOMPRESS;

    static {
        var symbols = NativeLoader.symbols();

        GZIP_COMPRESS = LINKER.downcallHandle(
            symbols.find("gzip_compress").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,       // return: bytes written or negative needed capacity
                ValueLayout.ADDRESS,        // input_ptr
                ValueLayout.JAVA_INT,       // input_len
                ValueLayout.ADDRESS,        // output_ptr
                ValueLayout.JAVA_INT        // output_cap
            )
        );

        GZIP_DECOMPRESS = LINKER.downcallHandle(
            symbols.find("gzip_decompress").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_INT
            )
        );
    }

    /**
     * Compress data using native GZIP (Rust flate2 with zlib-ng backend).
     *
     * @param input the data to compress
     * @return compressed data
     */
    public static byte[] compress(byte[] input) {
        // Estimate output size: worst case for gzip is input + headers (~18 bytes + 0.1%)
        int estimatedCap = input.length + 64;
        try {
            var ctx = NativeArenaPool.get();
            var inputSeg = ctx.input(input.length);
            inputSeg.copyFrom(MemorySegment.ofArray(input));
            var outputSeg = ctx.output(estimatedCap);

            int written = (int) GZIP_COMPRESS.invokeExact(
                inputSeg, input.length, outputSeg, estimatedCap
            );

            if (written < 0) {
                int needed = -written;
                outputSeg = ctx.output(needed);
                written = (int) GZIP_COMPRESS.invokeExact(
                    inputSeg, input.length, outputSeg, needed
                );
            }

            if (written < 0) {
                throw new RuntimeException("gzip_compress failed: unexpected retry failure");
            }

            byte[] result = new byte[written];
            MemorySegment.copy(outputSeg, ValueLayout.JAVA_BYTE, 0, result, 0, written);
            return result;
        } catch (Throwable t) {
            throw new RuntimeException("gzip_compress failed", t);
        }
    }

    /**
     * Decompress GZIP data using native decompressor (Rust flate2 with zlib-ng backend).
     *
     * @param input the compressed data
     * @return decompressed data
     */
    public static byte[] decompress(byte[] input) {
        // Estimate output size: 4x input is a reasonable initial guess
        int estimatedCap = input.length * 4 + 64;
        try {
            var ctx = NativeArenaPool.get();
            var inputSeg = ctx.input(input.length);
            inputSeg.copyFrom(MemorySegment.ofArray(input));
            var outputSeg = ctx.output(estimatedCap);

            int written = (int) GZIP_DECOMPRESS.invokeExact(
                inputSeg, input.length, outputSeg, estimatedCap
            );

            if (written < 0) {
                int needed = -written;
                outputSeg = ctx.output(needed);
                written = (int) GZIP_DECOMPRESS.invokeExact(
                    inputSeg, input.length, outputSeg, needed
                );
            }

            if (written < 0) {
                throw new RuntimeException("gzip_decompress failed: unexpected retry failure");
            }

            byte[] result = new byte[written];
            MemorySegment.copy(outputSeg, ValueLayout.JAVA_BYTE, 0, result, 0, written);
            return result;
        } catch (Throwable t) {
            throw new RuntimeException("gzip_decompress failed", t);
        }
    }

    private NativeCompressor() {
        // utility class
    }
}
