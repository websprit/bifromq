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
    private static final MethodHandle TRIE_NEW_GLOBAL;
    private static final MethodHandle TRIE_FREE;
    private static final MethodHandle TRIE_ADD;
    private static final MethodHandle TRIE_REMOVE;
    private static final MethodHandle TRIE_MATCH;
    private static final MethodHandle TRIE_MATCH_BATCH;
    private static final MethodHandle TRIE_GET;

    static {
        var symbols = NativeLoader.symbols();

        TRIE_NEW = LINKER.downcallHandle(
            symbols.find("topic_trie_new").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.ADDRESS)
        );

        TRIE_NEW_GLOBAL = LINKER.downcallHandle(
            symbols.find("topic_trie_new_global").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE)
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

        TRIE_MATCH_BATCH = LINKER.downcallHandle(
            symbols.find("topic_trie_match_batch").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_INT
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
        this(false);
    }

    public NativeTopicTrie(boolean isGlobal) {
        try {
            this.triePtr = (MemorySegment) TRIE_NEW_GLOBAL.invokeExact(isGlobal ? (byte) 1 : (byte) 0);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to create native TopicTrie", t);
        }
    }

    MemorySegment triePtr() {
        return triePtr;
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
     * Match topic filters against all stored topics.
     */
    public BatchMatchResult matchBatch(List<List<String>> filters) {
        if (filters.isEmpty()) {
            return new BatchMatchResult(new int[0], new int[0], new long[0]);
        }
        int initialCap = Math.max(64, filters.size() * 4);
        return queryTrieBatch(filters, initialCap);
    }

    /**
     * Get exact-match values for a topic.
     */
    public Set<Long> get(List<String> levels) {
        return queryTrie(TRIE_GET, levels);
    }

    private BatchMatchResult queryTrieBatch(List<List<String>> filters, int topicIdsCap) {
        try {
            var ctx = NativeArenaPool.get();
            BatchInput input = allocateBatchInput(ctx, filters);
            var topicIdsSegment = ctx.output((long) topicIdsCap * ValueLayout.JAVA_LONG.byteSize());
            int count = invokeMatchBatch(input, topicIdsSegment, topicIdsCap);
            if (count < 0) {
                int needed = -count;
                topicIdsSegment = ctx.output((long) needed * ValueLayout.JAVA_LONG.byteSize());
                count = invokeMatchBatch(input, topicIdsSegment, needed);
            }
            if (count < 0) {
                throw new IllegalStateException("topic_trie_match_batch failed: " + count);
            }
            int[] resultOffsets = new int[filters.size()];
            int[] resultCounts = new int[filters.size()];
            for (int i = 0; i < filters.size(); i++) {
                resultOffsets[i] = input.resultOffsetsSegment.getAtIndex(ValueLayout.JAVA_INT, i);
                resultCounts[i] = input.resultCountsSegment.getAtIndex(ValueLayout.JAVA_INT, i);
            }
            long[] topicIds = new long[count];
            for (int i = 0; i < count; i++) {
                topicIds[i] = topicIdsSegment.getAtIndex(ValueLayout.JAVA_LONG, i);
            }
            return new BatchMatchResult(resultOffsets, resultCounts, topicIds);
        } catch (Throwable t) {
            throw new RuntimeException("topic_trie_match_batch failed", t);
        }
    }

    private int invokeMatchBatch(BatchInput input, MemorySegment topicIdsSegment, int topicIdsCap) throws Throwable {
        return (int) TRIE_MATCH_BATCH.invokeExact(
            triePtr,
            input.levelsSegment,
            input.levelCount,
            input.filterOffsetsSegment,
            input.filterCountsSegment,
            input.filterCount,
            input.resultOffsetsSegment,
            input.resultCountsSegment,
            topicIdsSegment,
            topicIdsCap);
    }

    private BatchInput allocateBatchInput(NativeArenaPool.Ctx ctx, List<List<String>> filters) {
        int levelCount = 0;
        long maxStringBytes = 0;
        for (List<String> filter : filters) {
            levelCount += filter.size();
            for (String level : filter) {
                maxStringBytes += (long) level.length() * 3;
            }
        }
        long cLevelBytes = C_LEVEL_LAYOUT.byteSize() * levelCount;
        long stringOffset = cLevelBytes;
        long filterOffsetsOffset = align(stringOffset + maxStringBytes, ValueLayout.JAVA_INT.byteSize());
        long filterCountsOffset = filterOffsetsOffset + (long) filters.size() * ValueLayout.JAVA_INT.byteSize();
        long resultOffsetsOffset = filterCountsOffset + (long) filters.size() * ValueLayout.JAVA_INT.byteSize();
        long resultCountsOffset = resultOffsetsOffset + (long) filters.size() * ValueLayout.JAVA_INT.byteSize();
        long totalBytes = resultCountsOffset + (long) filters.size() * ValueLayout.JAVA_INT.byteSize();
        MemorySegment input = ctx.input(totalBytes);
        MemorySegment levelsSegment = input.asSlice(0, cLevelBytes);
        MemorySegment filterOffsetsSegment = input.asSlice(filterOffsetsOffset,
            (long) filters.size() * ValueLayout.JAVA_INT.byteSize());
        MemorySegment filterCountsSegment = input.asSlice(filterCountsOffset,
            (long) filters.size() * ValueLayout.JAVA_INT.byteSize());
        MemorySegment resultOffsetsSegment = input.asSlice(resultOffsetsOffset,
            (long) filters.size() * ValueLayout.JAVA_INT.byteSize());
        MemorySegment resultCountsSegment = input.asSlice(resultCountsOffset,
            (long) filters.size() * ValueLayout.JAVA_INT.byteSize());
        CharsetEncoder encoder = UTF8_ENCODER.get();
        long cLevelSize = C_LEVEL_LAYOUT.byteSize();
        long stringWriteOffset = stringOffset;
        int levelIndex = 0;
        for (int i = 0; i < filters.size(); i++) {
            List<String> filter = filters.get(i);
            filterOffsetsSegment.setAtIndex(ValueLayout.JAVA_INT, i, levelIndex);
            filterCountsSegment.setAtIndex(ValueLayout.JAVA_INT, i, filter.size());
            for (String level : filter) {
                int maxBytes = level.length() * 3;
                MemorySegment strSegment = input.asSlice(stringWriteOffset, maxBytes);
                ByteBuffer buf = strSegment.asByteBuffer();
                buf.clear();
                encoder.reset();
                encoder.encode(CharBuffer.wrap(level), buf, true);
                encoder.flush(buf);
                int len = buf.position();
                long offset = levelIndex * cLevelSize;
                levelsSegment.set(ValueLayout.ADDRESS, offset, strSegment);
                levelsSegment.set(ValueLayout.JAVA_INT, offset + ValueLayout.ADDRESS.byteSize(), len);
                stringWriteOffset += len;
                levelIndex++;
            }
        }
        return new BatchInput(levelsSegment, levelCount, filterOffsetsSegment, filterCountsSegment, filters.size(),
            resultOffsetsSegment, resultCountsSegment);
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

    public record BatchMatchResult(int[] resultOffsets, int[] resultCounts, long[] topicIds) {
    }

    private record BatchInput(MemorySegment levelsSegment,
                              int levelCount,
                              MemorySegment filterOffsetsSegment,
                              MemorySegment filterCountsSegment,
                              int filterCount,
                              MemorySegment resultOffsetsSegment,
                              MemorySegment resultCountsSegment) {
    }

    private static long align(long value, long alignment) {
        return (value + alignment - 1) / alignment * alignment;
    }

    private static final ThreadLocal<CharsetEncoder> UTF8_ENCODER = ThreadLocal.withInitial(() ->
        StandardCharsets.UTF_8.newEncoder()
            .onMalformedInput(CodingErrorAction.REPLACE)
            .onUnmappableCharacter(CodingErrorAction.REPLACE));

    /**
     * Allocate a CLevel array in the given arena from Java string levels.
     * Uses CharsetEncoder to write UTF-8 bytes directly into native memory,
     * avoiding per-level byte[] allocation from {@code String.getBytes(UTF_8)}.
     */
    private MemorySegment allocateLevels(Arena arena, List<String> levels) {
        long cLevelSize = C_LEVEL_LAYOUT.byteSize();
        var segment = arena.allocate(C_LEVEL_LAYOUT, levels.size());
        CharsetEncoder encoder = UTF8_ENCODER.get();

        for (int i = 0; i < levels.size(); i++) {
            String level = levels.get(i);
            // Max 3 bytes per char in UTF-8; allocate directly in native arena
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
