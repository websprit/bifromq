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

package org.apache.bifromq.native_binding;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/**
 * Thread-local Arena pool for FFM calls.
 * <p>
 * Eliminates the ~100ns overhead of {@code Arena.ofConfined()} per call by
 * reusing a long-lived Arena per thread. Each thread gets pre-allocated
 * input and output buffers that grow on demand.
 * <p>
 * Usage:
 * <pre>{@code
 * NativeArenaPool.Ctx ctx = NativeArenaPool.get();
 * MemorySegment input = ctx.input(dataLength);
 * MemorySegment output = ctx.output(estimatedOutputSize);
 * // ... invoke native function ...
 * ctx.reset(); // reuse for next call (no allocation)
 * }</pre>
 * <p>
 * Thread-safety: each thread has its own Arena and buffers. The Arena is
 * confined to the owning thread, matching FFM's confined arena semantics.
 */
public final class NativeArenaPool {
    private static final int DEFAULT_INPUT_SIZE = 4096;
    private static final int DEFAULT_OUTPUT_SIZE = 8192;

    private static final ThreadLocal<Ctx> CTX = ThreadLocal.withInitial(Ctx::new);

    /**
     * Get the thread-local context (Arena + pre-allocated buffers).
     */
    public static Ctx get() {
        return CTX.get();
    }

    /**
     * Per-thread context holding a reusable Arena and pre-allocated buffers.
     */
    public static final class Ctx {
        private final Arena arena;
        private MemorySegment inputBuf;
        private MemorySegment outputBuf;

        Ctx() {
            this.arena = Arena.ofAuto();
            this.inputBuf = arena.allocate(DEFAULT_INPUT_SIZE);
            this.outputBuf = arena.allocate(DEFAULT_OUTPUT_SIZE);
        }

        /**
         * Get the backing Arena for allocating additional segments if needed.
         */
        public Arena arena() {
            return arena;
        }

        /**
         * Get an input buffer of at least {@code minSize} bytes.
         * The buffer is reused across calls; if the current buffer is too small,
         * a new larger one is allocated (and cached for future calls).
         */
        public MemorySegment input(long minSize) {
            if (inputBuf.byteSize() < minSize) {
                inputBuf = arena.allocate(Math.max(minSize, inputBuf.byteSize() * 2));
            }
            return inputBuf;
        }

        /**
         * Get an output buffer of at least {@code minSize} bytes.
         */
        public MemorySegment output(long minSize) {
            if (outputBuf.byteSize() < minSize) {
                outputBuf = arena.allocate(Math.max(minSize, outputBuf.byteSize() * 2));
            }
            return outputBuf;
        }

        /**
         * Allocate a fresh segment from the thread-local arena.
         * Use this for small, per-call allocations (e.g., CLevel struct arrays).
         */
        public MemorySegment allocate(long byteSize) {
            return arena.allocate(byteSize);
        }

        /**
         * Allocate and copy bytes into the thread-local arena.
         */
        public MemorySegment allocateFrom(byte[] data) {
            var seg = arena.allocate(data.length);
            seg.copyFrom(MemorySegment.ofArray(data));
            return seg;
        }
    }

    private NativeArenaPool() {
    }
}
