// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! C ABI exports for the topic engine.
//!
//! All functions use `extern "C"` and `#[unsafe(no_mangle)]` for FFM compatibility.
//! String arguments are passed as `(ptr, len)` pairs.
//! Results are written to pre-allocated buffers.

use std::slice;
use super::trie::{TopicFilterIterator, TopicTrie};
use super::parser;
use super::validator;

// ============================================================
// TopicTrie lifecycle
// ============================================================

/// Create a new TopicTrie. Returns an opaque pointer.
#[unsafe(no_mangle)]
pub extern "C" fn topic_trie_new() -> *mut TopicTrie {
    Box::into_raw(Box::new(TopicTrie::new()))
}

#[unsafe(no_mangle)]
pub extern "C" fn topic_trie_new_global(is_global: u8) -> *mut TopicTrie {
    Box::into_raw(Box::new(TopicTrie::new_with_global(is_global != 0)))
}

/// Free a TopicTrie.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn topic_trie_free(trie: *mut TopicTrie) {
    if !trie.is_null() {
        unsafe { drop(Box::from_raw(trie)); }
    }
}

// ============================================================
// Level array helpers
// ============================================================

/// A level string represented as (pointer, length).
#[repr(C)]
pub struct CLevel {
    pub ptr: *const u8,
    pub len: u32,
}

/// Convert a C levels array to Rust string slices.
unsafe fn levels_to_strs<'a>(levels_ptr: *const CLevel, levels_count: u32) -> Vec<&'a str> {
    let levels = unsafe { slice::from_raw_parts(levels_ptr, levels_count as usize) };
    levels
        .iter()
        .map(|l| {
            let bytes = unsafe { slice::from_raw_parts(l.ptr, l.len as usize) };
            std::str::from_utf8_unchecked(bytes)
        })
        .collect()
}

// ============================================================
// TopicTrie operations
// ============================================================

/// Add a topic (given as level array) with a value id.
/// Returns 1 if newly inserted, 0 if already existed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn topic_trie_add(
    trie: *mut TopicTrie,
    levels_ptr: *const CLevel,
    levels_count: u32,
    value_id: u64,
) -> i32 {
    let trie = unsafe { &mut *trie };
    let levels = unsafe { levels_to_strs(levels_ptr, levels_count) };
    let level_refs: Vec<&str> = levels.iter().map(|s| *s).collect();
    if trie.add(&level_refs, value_id) { 1 } else { 0 }
}

/// Remove a topic with a value id.
/// Returns 1 if found and removed, 0 otherwise.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn topic_trie_remove(
    trie: *mut TopicTrie,
    levels_ptr: *const CLevel,
    levels_count: u32,
    value_id: u64,
) -> i32 {
    let trie = unsafe { &mut *trie };
    let levels = unsafe { levels_to_strs(levels_ptr, levels_count) };
    let level_refs: Vec<&str> = levels.iter().map(|s| *s).collect();
    if trie.remove(&level_refs, value_id) { 1 } else { 0 }
}

/// Match a topic filter against all stored topics.
/// Writes matching value ids to `result_buf` (up to `buf_cap` entries).
/// Returns the number of matches written. If there are more matches than capacity,
/// returns -1 to signal the caller to retry with a larger buffer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn topic_trie_match(
    trie: *const TopicTrie,
    filter_levels_ptr: *const CLevel,
    filter_levels_count: u32,
    result_buf: *mut u64,
    buf_cap: u32,
) -> i32 {
    let trie = unsafe { &*trie };
    let levels = unsafe { levels_to_strs(filter_levels_ptr, filter_levels_count) };
    let level_refs: Vec<&str> = levels.iter().map(|s| *s).collect();
    let results = trie.match_filter(&level_refs);

    if results.len() > buf_cap as usize {
        return -(results.len() as i32);
    }

    let buf = unsafe { slice::from_raw_parts_mut(result_buf, buf_cap as usize) };
    for (i, &val) in results.iter().enumerate() {
        buf[i] = val;
    }
    results.len() as i32
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn topic_trie_match_batch(
    trie: *const TopicTrie,
    levels_ptr: *const CLevel,
    levels_count: u32,
    filter_offsets_ptr: *const u32,
    filter_counts_ptr: *const u32,
    filter_count: u32,
    result_offsets_ptr: *mut u32,
    result_counts_ptr: *mut u32,
    topic_ids_ptr: *mut u64,
    topic_ids_cap: u32,
) -> i32 {
    let trie = unsafe { &*trie };
    let levels = unsafe { levels_to_strs(levels_ptr, levels_count) };
    let filter_offsets = unsafe { slice::from_raw_parts(filter_offsets_ptr, filter_count as usize) };
    let filter_counts = unsafe { slice::from_raw_parts(filter_counts_ptr, filter_count as usize) };
    let result_offsets = unsafe { slice::from_raw_parts_mut(result_offsets_ptr, filter_count as usize) };
    let result_counts = unsafe { slice::from_raw_parts_mut(result_counts_ptr, filter_count as usize) };

    let mut all_results = Vec::new();
    let mut scratch = Vec::new();
    for i in 0..filter_count as usize {
        let offset = filter_offsets[i] as usize;
        let count = filter_counts[i] as usize;
        if offset > levels.len() || offset + count > levels.len() {
            return -1;
        }
        scratch.clear();
        trie.match_filter_into(&levels[offset..offset + count], &mut scratch);
        result_offsets[i] = all_results.len() as u32;
        result_counts[i] = scratch.len() as u32;
        all_results.extend_from_slice(&scratch);
    }

    if all_results.len() > topic_ids_cap as usize {
        return -(all_results.len() as i32);
    }

    let topic_ids = unsafe { slice::from_raw_parts_mut(topic_ids_ptr, topic_ids_cap as usize) };
    for (i, value_id) in all_results.into_iter().enumerate() {
        topic_ids[i] = value_id;
    }
    result_counts.iter().sum::<u32>() as i32
}

// ============================================================
// TopicFilterIterator operations
// ============================================================

#[unsafe(no_mangle)]
pub unsafe extern "C" fn topic_filter_iter_new(trie: *const TopicTrie) -> *mut TopicFilterIterator {
    let trie = unsafe { &*trie };
    Box::into_raw(Box::new(trie.filter_iterator()))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn topic_filter_iter_free(iter: *mut TopicFilterIterator) {
    if !iter.is_null() {
        unsafe { drop(Box::from_raw(iter)); }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn topic_filter_iter_seek(
    iter: *mut TopicFilterIterator,
    levels_ptr: *const CLevel,
    levels_count: u32,
) -> i32 {
    let iter = unsafe { &mut *iter };
    let levels = unsafe { levels_to_strs(levels_ptr, levels_count) };
    let level_refs: Vec<&str> = levels.iter().map(|s| *s).collect();
    iter.seek(&level_refs);
    if iter.is_valid() { 1 } else { 0 }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn topic_filter_iter_seek_prev(
    iter: *mut TopicFilterIterator,
    levels_ptr: *const CLevel,
    levels_count: u32,
) -> i32 {
    let iter = unsafe { &mut *iter };
    let levels = unsafe { levels_to_strs(levels_ptr, levels_count) };
    let level_refs: Vec<&str> = levels.iter().map(|s| *s).collect();
    iter.seek_prev(&level_refs);
    if iter.is_valid() { 1 } else { 0 }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn topic_filter_iter_next(iter: *mut TopicFilterIterator) -> i32 {
    let iter = unsafe { &mut *iter };
    iter.next();
    if iter.is_valid() { 1 } else { 0 }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn topic_filter_iter_prev(iter: *mut TopicFilterIterator) -> i32 {
    let iter = unsafe { &mut *iter };
    iter.prev();
    if iter.is_valid() { 1 } else { 0 }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn topic_filter_iter_is_valid(iter: *const TopicFilterIterator) -> i32 {
    let iter = unsafe { &*iter };
    if iter.is_valid() { 1 } else { 0 }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn topic_filter_iter_key(
    iter: *const TopicFilterIterator,
    out_levels: *mut CLevel,
    cap: u32,
) -> i32 {
    let iter = unsafe { &*iter };
    let Some(levels) = iter.key() else {
        return -1;
    };
    if levels.len() > cap as usize {
        return -(levels.len() as i32);
    }
    let out = unsafe { slice::from_raw_parts_mut(out_levels, cap as usize) };
    for (i, level) in levels.iter().enumerate() {
        out[i] = CLevel {
            ptr: level.as_ptr(),
            len: level.len() as u32,
        };
    }
    levels.len() as i32
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn topic_filter_iter_values(
    iter: *const TopicFilterIterator,
    result_buf: *mut u64,
    buf_cap: u32,
) -> i32 {
    let iter = unsafe { &*iter };
    let Some(values) = iter.values() else {
        return -1;
    };
    if values.len() > buf_cap as usize {
        return -(values.len() as i32);
    }
    let buf = unsafe { slice::from_raw_parts_mut(result_buf, buf_cap as usize) };
    for (i, &val) in values.iter().enumerate() {
        buf[i] = val;
    }
    values.len() as i32
}

/// Get exact-match values for a topic.
/// Writes matching value ids to `result_buf` (up to `buf_cap` entries).
/// Returns the number of matches written, or negative if buffer too small.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn topic_trie_get(
    trie: *const TopicTrie,
    levels_ptr: *const CLevel,
    levels_count: u32,
    result_buf: *mut u64,
    buf_cap: u32,
) -> i32 {
    let trie = unsafe { &*trie };
    let levels = unsafe { levels_to_strs(levels_ptr, levels_count) };
    let level_refs: Vec<&str> = levels.iter().map(|s| *s).collect();
    let results = trie.get(&level_refs);

    if results.len() > buf_cap as usize {
        return -(results.len() as i32);
    }

    let buf = unsafe { slice::from_raw_parts_mut(result_buf, buf_cap as usize) };
    for (i, &val) in results.iter().enumerate() {
        buf[i] = val;
    }
    results.len() as i32
}

// ============================================================
// Topic parsing
// ============================================================

/// Parse a topic string into levels.
/// Writes level offsets and lengths to `out_offsets` and `out_lengths`.
/// Returns the number of levels, or negative if buffer too small.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn topic_parse(
    topic_ptr: *const u8,
    topic_len: u32,
    out_offsets: *mut u32,
    out_lengths: *mut u32,
    cap: u32,
) -> i32 {
    let topic = unsafe { slice::from_raw_parts(topic_ptr, topic_len as usize) };
    let levels = parser::parse(topic);

    if levels.len() > cap as usize {
        return -(levels.len() as i32);
    }

    let offsets = unsafe { slice::from_raw_parts_mut(out_offsets, cap as usize) };
    let lengths = unsafe { slice::from_raw_parts_mut(out_lengths, cap as usize) };

    for (i, level) in levels.iter().enumerate() {
        // Calculate offset relative to topic_ptr
        let offset = level.as_ptr() as usize - topic_ptr as usize;
        offsets[i] = offset as u32;
        lengths[i] = level.len() as u32;
    }

    levels.len() as i32
}

// ============================================================
// Topic validation
// ============================================================

/// Validate an MQTT topic name.
/// Returns 1 if valid, 0 if invalid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn topic_validate(
    topic_ptr: *const u8,
    topic_len: u32,
    max_level_len: u32,
    max_level: u32,
    max_len: u32,
) -> i32 {
    let topic = unsafe { slice::from_raw_parts(topic_ptr, topic_len as usize) };
    if validator::validate_topic(topic, max_level_len as usize, max_level as usize, max_len as usize) {
        1
    } else {
        0
    }
}

/// Validate an MQTT topic filter.
/// Returns 1 if valid, 0 if invalid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn topic_filter_validate(
    filter_ptr: *const u8,
    filter_len: u32,
    max_level_len: u32,
    max_level: u32,
    max_len: u32,
) -> i32 {
    let filter = unsafe { slice::from_raw_parts(filter_ptr, filter_len as usize) };
    if validator::validate_topic_filter(filter, max_level_len as usize, max_level as usize, max_len as usize) {
        1
    } else {
        0
    }
}
