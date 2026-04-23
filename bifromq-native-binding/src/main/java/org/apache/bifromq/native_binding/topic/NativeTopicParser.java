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
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.bifromq.native_binding.NativeLoader;

/**
 * Java FFM binding for Rust topic parsing and validation functions.
 */
public final class NativeTopicParser {
    private static final Linker LINKER = Linker.nativeLinker();

    private static final MethodHandle TOPIC_PARSE;
    private static final MethodHandle TOPIC_VALIDATE;
    private static final MethodHandle TOPIC_FILTER_VALIDATE;

    static {
        var symbols = NativeLoader.symbols();

        TOPIC_PARSE = LINKER.downcallHandle(
            symbols.find("topic_parse").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,       // return: level count or negative
                ValueLayout.ADDRESS,        // topic_ptr
                ValueLayout.JAVA_INT,       // topic_len
                ValueLayout.ADDRESS,        // out_offsets
                ValueLayout.ADDRESS,        // out_lengths
                ValueLayout.JAVA_INT        // cap
            )
        );

        TOPIC_VALIDATE = LINKER.downcallHandle(
            symbols.find("topic_validate").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,       // return: 1=valid, 0=invalid
                ValueLayout.ADDRESS,        // topic_ptr
                ValueLayout.JAVA_INT,       // topic_len
                ValueLayout.JAVA_INT,       // max_level_len
                ValueLayout.JAVA_INT,       // max_level
                ValueLayout.JAVA_INT        // max_len
            )
        );

        TOPIC_FILTER_VALIDATE = LINKER.downcallHandle(
            symbols.find("topic_filter_validate").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_INT,
                ValueLayout.JAVA_INT,
                ValueLayout.JAVA_INT,
                ValueLayout.JAVA_INT
            )
        );
    }

    /**
     * Parse a topic string into levels using the native parser.
     */
    public static List<String> parse(String topic) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        int initialCap = 32;

        try (var arena = Arena.ofConfined()) {
            var topicSeg = arena.allocateFrom(ValueLayout.JAVA_BYTE, topicBytes);
            var offsets = arena.allocate(ValueLayout.JAVA_INT, initialCap);
            var lengths = arena.allocate(ValueLayout.JAVA_INT, initialCap);

            int count = (int) TOPIC_PARSE.invokeExact(
                topicSeg, topicBytes.length, offsets, lengths, initialCap
            );

            if (count < 0) {
                int needed = -count;
                offsets = arena.allocate(ValueLayout.JAVA_INT, needed);
                lengths = arena.allocate(ValueLayout.JAVA_INT, needed);
                count = (int) TOPIC_PARSE.invokeExact(
                    topicSeg, topicBytes.length, offsets, lengths, needed
                );
            }

            List<String> result = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                int offset = offsets.getAtIndex(ValueLayout.JAVA_INT, i);
                int len = lengths.getAtIndex(ValueLayout.JAVA_INT, i);
                byte[] levelBytes = topicSeg.asSlice(offset, len).toArray(ValueLayout.JAVA_BYTE);
                result.add(new String(levelBytes, StandardCharsets.UTF_8));
            }
            return result;
        } catch (Throwable t) {
            throw new RuntimeException("topic_parse failed", t);
        }
    }

    /**
     * Validate an MQTT topic name.
     */
    public static boolean validateTopic(String topic, int maxLevelLen, int maxLevel, int maxLen) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        try (var arena = Arena.ofConfined()) {
            var topicSeg = arena.allocateFrom(ValueLayout.JAVA_BYTE, topicBytes);
            int result = (int) TOPIC_VALIDATE.invokeExact(
                topicSeg, topicBytes.length, maxLevelLen, maxLevel, maxLen
            );
            return result == 1;
        } catch (Throwable t) {
            throw new RuntimeException("topic_validate failed", t);
        }
    }

    /**
     * Validate an MQTT topic filter.
     */
    public static boolean validateTopicFilter(String filter, int maxLevelLen, int maxLevel, int maxLen) {
        byte[] filterBytes = filter.getBytes(StandardCharsets.UTF_8);
        try (var arena = Arena.ofConfined()) {
            var filterSeg = arena.allocateFrom(ValueLayout.JAVA_BYTE, filterBytes);
            int result = (int) TOPIC_FILTER_VALIDATE.invokeExact(
                filterSeg, filterBytes.length, maxLevelLen, maxLevel, maxLen
            );
            return result == 1;
        } catch (Throwable t) {
            throw new RuntimeException("topic_filter_validate failed", t);
        }
    }

    private NativeTopicParser() {
        // utility class
    }
}
