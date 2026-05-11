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

package org.apache.bifromq.native_binding.topic;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.bifromq.native_binding.NativeArenaPool;
import org.apache.bifromq.native_binding.NativeLoader;

public final class NativeTopicFilterIterator implements AutoCloseable {
    private static final Linker LINKER = Linker.nativeLinker();
    private static final MemoryLayout C_LEVEL_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.ADDRESS.withName("ptr"),
        ValueLayout.JAVA_INT.withName("len"),
        MemoryLayout.paddingLayout(4)
    );
    private static final MethodHandle ITER_NEW;
    private static final MethodHandle ITER_FREE;
    private static final MethodHandle ITER_SEEK;
    private static final MethodHandle ITER_SEEK_PREV;
    private static final MethodHandle ITER_NEXT;
    private static final MethodHandle ITER_PREV;
    private static final MethodHandle ITER_IS_VALID;
    private static final MethodHandle ITER_KEY;
    private static final MethodHandle ITER_VALUES;

    static {
        var symbols = NativeLoader.symbols();
        ITER_NEW = LINKER.downcallHandle(
            symbols.find("topic_filter_iter_new").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS)
        );
        ITER_FREE = LINKER.downcallHandle(
            symbols.find("topic_filter_iter_free").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
        );
        ITER_SEEK = LINKER.downcallHandle(
            symbols.find("topic_filter_iter_seek").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT)
        );
        ITER_SEEK_PREV = LINKER.downcallHandle(
            symbols.find("topic_filter_iter_seek_prev").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT)
        );
        ITER_NEXT = LINKER.downcallHandle(
            symbols.find("topic_filter_iter_next").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS)
        );
        ITER_PREV = LINKER.downcallHandle(
            symbols.find("topic_filter_iter_prev").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS)
        );
        ITER_IS_VALID = LINKER.downcallHandle(
            symbols.find("topic_filter_iter_is_valid").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS)
        );
        ITER_KEY = LINKER.downcallHandle(
            symbols.find("topic_filter_iter_key").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT)
        );
        ITER_VALUES = LINKER.downcallHandle(
            symbols.find("topic_filter_iter_values").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT)
        );
    }

    private static final ThreadLocal<CharsetEncoder> UTF8_ENCODER = ThreadLocal.withInitial(() ->
        StandardCharsets.UTF_8.newEncoder()
            .onMalformedInput(CodingErrorAction.REPLACE)
            .onUnmappableCharacter(CodingErrorAction.REPLACE));

    private MemorySegment iterPtr;

    public NativeTopicFilterIterator(NativeTopicTrie trie) {
        try {
            iterPtr = (MemorySegment) ITER_NEW.invokeExact(trie.triePtr());
        } catch (Throwable t) {
            throw new RuntimeException("topic_filter_iter_new failed", t);
        }
    }

    public void seek(List<String> levels) {
        invokeWithLevels(ITER_SEEK, levels, "topic_filter_iter_seek failed");
    }

    public void seekPrev(List<String> levels) {
        invokeWithLevels(ITER_SEEK_PREV, levels, "topic_filter_iter_seek_prev failed");
    }

    public void next() {
        try {
            int ignored = (int) ITER_NEXT.invokeExact(iterPtr);
        } catch (Throwable t) {
            throw new RuntimeException("topic_filter_iter_next failed", t);
        }
    }

    public void prev() {
        try {
            int ignored = (int) ITER_PREV.invokeExact(iterPtr);
        } catch (Throwable t) {
            throw new RuntimeException("topic_filter_iter_prev failed", t);
        }
    }

    public boolean isValid() {
        try {
            return (int) ITER_IS_VALID.invokeExact(iterPtr) == 1;
        } catch (Throwable t) {
            throw new RuntimeException("topic_filter_iter_is_valid failed", t);
        }
    }

    public List<String> key() {
        int initialCap = 16;
        try (var arena = Arena.ofConfined()) {
            var out = arena.allocate(C_LEVEL_LAYOUT, initialCap);
            int count = (int) ITER_KEY.invokeExact(iterPtr, out, initialCap);
            if (count < 0) {
                int needed = -count;
                out = arena.allocate(C_LEVEL_LAYOUT, needed);
                count = (int) ITER_KEY.invokeExact(iterPtr, out, needed);
            }
            if (count < 0) {
                throw new IllegalStateException("Native iterator is invalid");
            }
            List<String> levels = new ArrayList<>(count);
            long cLevelSize = C_LEVEL_LAYOUT.byteSize();
            for (int i = 0; i < count; i++) {
                long offset = i * cLevelSize;
                MemorySegment ptr = out.get(ValueLayout.ADDRESS, offset);
                int len = out.get(ValueLayout.JAVA_INT, offset + ValueLayout.ADDRESS.byteSize());
                byte[] bytes = ptr.reinterpret(len).toArray(ValueLayout.JAVA_BYTE);
                levels.add(new String(bytes, StandardCharsets.UTF_8));
            }
            return levels;
        } catch (Throwable t) {
            throw new RuntimeException("topic_filter_iter_key failed", t);
        }
    }

    public Set<Long> values() {
        int initialCap = 64;
        try {
            var ctx = NativeArenaPool.get();
            var resultBuf = ctx.output((long) initialCap * ValueLayout.JAVA_LONG.byteSize());
            int count = (int) ITER_VALUES.invokeExact(iterPtr, resultBuf, initialCap);
            if (count < 0) {
                int needed = -count;
                resultBuf = ctx.output((long) needed * ValueLayout.JAVA_LONG.byteSize());
                count = (int) ITER_VALUES.invokeExact(iterPtr, resultBuf, needed);
            }
            if (count < 0) {
                throw new IllegalStateException("Native iterator is invalid");
            }
            Set<Long> values = new HashSet<>(count);
            for (int i = 0; i < count; i++) {
                values.add(resultBuf.getAtIndex(ValueLayout.JAVA_LONG, i));
            }
            return values;
        } catch (Throwable t) {
            throw new RuntimeException("topic_filter_iter_values failed", t);
        }
    }

    private void invokeWithLevels(MethodHandle handle, List<String> levels, String message) {
        try {
            var ctx = NativeArenaPool.get();
            var levelsSegment = allocateLevels(ctx.arena(), levels);
            int ignored = (int) handle.invokeExact(iterPtr, levelsSegment, levels.size());
        } catch (Throwable t) {
            throw new RuntimeException(message, t);
        }
    }

    private MemorySegment allocateLevels(Arena arena, List<String> levels) {
        long cLevelSize = C_LEVEL_LAYOUT.byteSize();
        var segment = arena.allocate(C_LEVEL_LAYOUT, levels.size());
        CharsetEncoder encoder = UTF8_ENCODER.get();
        for (int i = 0; i < levels.size(); i++) {
            String level = levels.get(i);
            int maxBytes = level.length() * 3;
            var strSegment = arena.allocate(maxBytes);
            ByteBuffer buf = strSegment.asByteBuffer();
            buf.clear();
            encoder.reset();
            encoder.encode(CharBuffer.wrap(level), buf, true);
            encoder.flush(buf);
            int len = buf.position();
            long offset = i * cLevelSize;
            segment.set(ValueLayout.ADDRESS, offset, strSegment);
            segment.set(ValueLayout.JAVA_INT, offset + ValueLayout.ADDRESS.byteSize(), len);
        }
        return segment;
    }

    @Override
    public void close() {
        if (iterPtr != null && !iterPtr.equals(MemorySegment.NULL)) {
            try {
                ITER_FREE.invokeExact(iterPtr);
            } catch (Throwable t) {
                throw new RuntimeException("topic_filter_iter_free failed", t);
            } finally {
                iterPtr = MemorySegment.NULL;
            }
        }
    }
}
