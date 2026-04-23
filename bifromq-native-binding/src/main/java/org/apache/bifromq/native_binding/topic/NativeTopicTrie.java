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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.native_binding.NativeArenaPool;
import org.apache.bifromq.native_binding.NativeLoader;

/**
 * Java FFM binding for the Rust topic trie (TopicTrie).
 * <p>
 * This class manages the lifecycle of a native TopicTrie instance via opaque pointer,
 * and provides add/remove/match/get operations through FFM downcalls.
 */
@Slf4j
public class NativeTopicTrie implements AutoCloseable {
    private static final Linker LINKER = Linker.nativeLinker();

    // CLevel struct layout: { ptr: ADDRESS, len: u32 }
    private static final MemoryLayout C_LEVEL_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.ADDRESS.withName("ptr"),
        ValueLayout.JAVA_INT.withName("len"),
        MemoryLayout.paddingLayout(4)  // align to 8 bytes (after u32)
    );

    // Method handles (lazily resolved from NativeLoader symbols)
    private static final MethodHandle TRIE_NEW;
    private static final MethodHandle TRIE_FREE;
    private static final MethodHandle TRIE_ADD;
    private static final MethodHandle TRIE_REMOVE;
    private static final MethodHandle TRIE_MATCH;
    private static final MethodHandle TRIE_GET;

    static {
        var symbols = NativeLoader.symbols();

        TRIE_NEW = LINKER.downcallHandle(
            symbols.find("topic_trie_new").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.ADDRESS)
        );

        TRIE_FREE = LINKER.downcallHandle(
            symbols.find("topic_trie_free").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
        );

        TRIE_ADD = LINKER.downcallHandle(
            symbols.find("topic_trie_add").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,       // return: 1=new, 0=existed
                ValueLayout.ADDRESS,        // trie ptr
                ValueLayout.ADDRESS,        // levels array ptr
                ValueLayout.JAVA_INT,       // levels count
                ValueLayout.JAVA_LONG       // value id
            )
        );

        TRIE_REMOVE = LINKER.downcallHandle(
            symbols.find("topic_trie_remove").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_INT,
                ValueLayout.JAVA_LONG
            )
        );

        TRIE_MATCH = LINKER.downcallHandle(
            symbols.find("topic_trie_match").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,       // return: count or negative
                ValueLayout.ADDRESS,        // trie ptr (const)
                ValueLayout.ADDRESS,        // filter levels ptr
                ValueLayout.JAVA_INT,       // filter levels count
                ValueLayout.ADDRESS,        // result buffer ptr
                ValueLayout.JAVA_INT        // buffer capacity
            )
        );

        TRIE_GET = LINKER.downcallHandle(
            symbols.find("topic_trie_get").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_INT
            )
        );
    }

    private MemorySegment triePtr;

    public NativeTopicTrie() {
        try {
            this.triePtr = (MemorySegment) TRIE_NEW.invokeExact();
        } catch (Throwable t) {
            throw new RuntimeException("Failed to create native TopicTrie", t);
        }
    }

    /**
     * Add a topic (represented as levels) with an associated value id.
     *
     * @return true if newly inserted
     */
    public boolean add(List<String> levels, long valueId) {
        try {
            var ctx = NativeArenaPool.get();
            var levelsSegment = allocateLevels(ctx.arena(), levels);
            int result = (int) TRIE_ADD.invokeExact(triePtr, levelsSegment, levels.size(), valueId);
            return result == 1;
        } catch (Throwable t) {
            throw new RuntimeException("topic_trie_add failed", t);
        }
    }

    /**
     * Remove a topic with an associated value id.
     *
     * @return true if found and removed
     */
    public boolean remove(List<String> levels, long valueId) {
        try {
            var ctx = NativeArenaPool.get();
            var levelsSegment = allocateLevels(ctx.arena(), levels);
            int result = (int) TRIE_REMOVE.invokeExact(triePtr, levelsSegment, levels.size(), valueId);
            return result == 1;
        } catch (Throwable t) {
            throw new RuntimeException("topic_trie_remove failed", t);
        }
    }

    /**
     * Match a topic filter against all stored topics.
     */
    public Set<Long> match(List<String> filterLevels) {
        return queryTrie(TRIE_MATCH, filterLevels);
    }

    /**
     * Get exact-match values for a topic.
     */
    public Set<Long> get(List<String> levels) {
        return queryTrie(TRIE_GET, levels);
    }

    private Set<Long> queryTrie(MethodHandle handle, List<String> levels) {
        int initialCap = 64;
        try {
            var ctx = NativeArenaPool.get();
            var levelsSegment = allocateLevels(ctx.arena(), levels);
            var resultBuf = ctx.output((long) initialCap * ValueLayout.JAVA_LONG.byteSize());

            int count = (int) handle.invokeExact(
                triePtr, levelsSegment, levels.size(), resultBuf, initialCap
            );

            if (count < 0) {
                // Buffer too small, retry with exact size
                int needed = -count;
                var bigBuf = ctx.output((long) needed * ValueLayout.JAVA_LONG.byteSize());
                count = (int) handle.invokeExact(
                    triePtr, levelsSegment, levels.size(), bigBuf, needed
                );
                resultBuf = bigBuf;
            }

            Set<Long> result = new HashSet<>(count);
            for (int i = 0; i < count; i++) {
                result.add(resultBuf.getAtIndex(ValueLayout.JAVA_LONG, i));
            }
            return result;
        } catch (Throwable t) {
            throw new RuntimeException("topic_trie query failed", t);
        }
    }

    /**
     * Allocate a CLevel array in the given arena from Java string levels.
     */
    private MemorySegment allocateLevels(Arena arena, List<String> levels) {
        long cLevelSize = C_LEVEL_LAYOUT.byteSize();
        var segment = arena.allocate(C_LEVEL_LAYOUT, levels.size());

        for (int i = 0; i < levels.size(); i++) {
            byte[] bytes = levels.get(i).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            var strSegment = arena.allocateFrom(ValueLayout.JAVA_BYTE, bytes);
            long offset = i * cLevelSize;
            segment.set(ValueLayout.ADDRESS, offset, strSegment);
            segment.set(ValueLayout.JAVA_INT, offset + ValueLayout.ADDRESS.byteSize(), bytes.length);
        }

        return segment;
    }

    @Override
    public void close() {
        if (triePtr != null && !triePtr.equals(MemorySegment.NULL)) {
            try {
                TRIE_FREE.invokeExact(triePtr);
            } catch (Throwable t) {
                log.warn("Failed to free native TopicTrie", t);
            }
            triePtr = MemorySegment.NULL;
        }
    }
}
