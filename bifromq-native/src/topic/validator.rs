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

//! MQTT topic and topic filter validation.
//!
//! Corresponds to `TopicUtil.isValidTopic()` and `TopicUtil.isValidTopicFilter()` in BifroMQ Java.

const TOPIC_SEPARATOR: u8 = b'/';
const SINGLE_WILDCARD: u8 = b'+';
const MULTI_WILDCARD: u8 = b'#';
const SHARED_PREFIX: &[u8] = b"$share/";

/// Validate an MQTT topic name (not a filter — no wildcards allowed).
///
/// Rules:
/// - Must not be empty
/// - Must not exceed `max_len` bytes
/// - Must not have more than `max_level` levels
/// - Each level must not exceed `max_level_len` bytes
/// - Must not contain `+` or `#`
/// - Must not contain null character (0x00)
pub fn validate_topic(
    topic: &[u8],
    max_level_len: usize,
    max_level: usize,
    max_len: usize,
) -> bool {
    if topic.is_empty() || topic.len() > max_len {
        return false;
    }

    let mut level_count: usize = 1;
    let mut level_start: usize = 0;

    for i in 0..topic.len() {
        match topic[i] {
            0x00 | SINGLE_WILDCARD | MULTI_WILDCARD => return false,
            TOPIC_SEPARATOR => {
                let level_len = i - level_start;
                if level_len > max_level_len {
                    return false;
                }
                level_count += 1;
                if level_count > max_level {
                    return false;
                }
                level_start = i + 1;
            }
            _ => {}
        }
    }

    // Check last level
    let last_level_len = topic.len() - level_start;
    if last_level_len > max_level_len {
        return false;
    }

    true
}

/// Validate an MQTT topic filter (may contain `+` and `#` wildcards).
///
/// Rules:
/// - Must not be empty
/// - Must not exceed `max_len` bytes
/// - Must not have more than `max_level` levels (excluding shared subscription prefix)
/// - Each level must not exceed `max_level_len` bytes
/// - `+` must occupy an entire level by itself
/// - `#` must be the last level and occupy it entirely
/// - Must not contain null character (0x00)
/// - If shared subscription (`$share/group/filter`), the group name must not be empty
///   and must not contain `+`, `#`, or `/`
pub fn validate_topic_filter(
    filter: &[u8],
    max_level_len: usize,
    max_level: usize,
    max_len: usize,
) -> bool {
    if filter.is_empty() || filter.len() > max_len {
        return false;
    }

    // Check for shared subscription
    let (actual_filter, _is_shared) = if filter.len() > SHARED_PREFIX.len()
        && &filter[..SHARED_PREFIX.len()] == SHARED_PREFIX
    {
        // Find the group name end (next /)
        let rest = &filter[SHARED_PREFIX.len()..];
        let group_end = match rest.iter().position(|&b| b == TOPIC_SEPARATOR) {
            Some(pos) => pos,
            None => return false, // No filter after group
        };
        let group = &rest[..group_end];
        // Group must not be empty and must not contain wildcards
        if group.is_empty() {
            return false;
        }
        for &b in group {
            if b == SINGLE_WILDCARD || b == MULTI_WILDCARD || b == TOPIC_SEPARATOR || b == 0x00 {
                return false;
            }
        }
        let actual = &rest[group_end + 1..];
        if actual.is_empty() {
            return false; // Must have filter after $share/group/
        }
        (actual, true)
    } else {
        (filter, false)
    };

    // Validate the actual filter part
    let mut level_count: usize = 1;
    let mut level_start: usize = 0;

    for i in 0..actual_filter.len() {
        match actual_filter[i] {
            0x00 => return false,
            TOPIC_SEPARATOR => {
                let level_len = i - level_start;
                if level_len > max_level_len {
                    return false;
                }
                // Check the level we just finished
                if !validate_filter_level(&actual_filter[level_start..i]) {
                    return false;
                }
                // `#` must be the last level, so if we see a separator after it, invalid
                if level_start < i && actual_filter[level_start] == MULTI_WILDCARD {
                    return false;
                }
                level_count += 1;
                if level_count > max_level {
                    return false;
                }
                level_start = i + 1;
            }
            _ => {}
        }
    }

    // Check last level
    let last_level = &actual_filter[level_start..];
    if last_level.len() > max_level_len {
        return false;
    }
    if !validate_filter_level(last_level) {
        return false;
    }

    true
}

/// Validate a single level within a topic filter.
/// `+` must be the sole character in its level.
/// `#` must be the sole character in its level.
fn validate_filter_level(level: &[u8]) -> bool {
    if level.is_empty() {
        return true; // Empty levels are allowed (e.g., "a//b")
    }
    if level.len() == 1 {
        return true; // Single char (including + or #) is fine
    }
    // Multi-char level must not contain wildcards
    for &b in level {
        if b == SINGLE_WILDCARD || b == MULTI_WILDCARD {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX_LEVEL_LEN: usize = 256;
    const MAX_LEVEL: usize = 128;
    const MAX_LEN: usize = 65535;

    // --- Topic validation ---

    #[test]
    fn test_valid_topic() {
        assert!(validate_topic(b"a/b/c", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
        assert!(validate_topic(b"hello", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
        assert!(validate_topic(b"/", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
        assert!(validate_topic(b"a/", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
        assert!(validate_topic(b"/a", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
        assert!(validate_topic(b"$SYS/broker", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
    }

    #[test]
    fn test_invalid_topic_empty() {
        assert!(!validate_topic(b"", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
    }

    #[test]
    fn test_invalid_topic_wildcards() {
        assert!(!validate_topic(b"a/+/c", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
        assert!(!validate_topic(b"a/#", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
        assert!(!validate_topic(b"+", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
        assert!(!validate_topic(b"#", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
    }

    #[test]
    fn test_invalid_topic_null() {
        assert!(!validate_topic(b"a\x00b", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
    }

    #[test]
    fn test_topic_max_levels() {
        assert!(!validate_topic(b"a/b/c", MAX_LEVEL_LEN, 2, MAX_LEN)); // 3 levels > max 2
        assert!(validate_topic(b"a/b", MAX_LEVEL_LEN, 2, MAX_LEN));
    }

    #[test]
    fn test_topic_max_length() {
        assert!(!validate_topic(b"abc", MAX_LEVEL_LEN, MAX_LEVEL, 2));
    }

    // --- Topic filter validation ---

    #[test]
    fn test_valid_filter() {
        assert!(validate_topic_filter(b"a/b/c", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
        assert!(validate_topic_filter(b"a/+/c", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
        assert!(validate_topic_filter(b"a/#", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
        assert!(validate_topic_filter(b"#", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
        assert!(validate_topic_filter(b"+", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
        assert!(validate_topic_filter(b"+/+/+", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
    }

    #[test]
    fn test_invalid_filter_hash_not_last() {
        assert!(!validate_topic_filter(b"a/#/c", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
    }

    #[test]
    fn test_invalid_filter_wildcard_in_level() {
        assert!(!validate_topic_filter(b"a/b+c", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
        assert!(!validate_topic_filter(b"a/b#c", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
    }

    #[test]
    fn test_valid_shared_sub() {
        assert!(validate_topic_filter(b"$share/group1/a/b", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
        assert!(validate_topic_filter(b"$share/g/+", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
        assert!(validate_topic_filter(b"$share/g/#", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
    }

    #[test]
    fn test_invalid_shared_sub_empty_group() {
        assert!(!validate_topic_filter(b"$share//topic", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
    }

    #[test]
    fn test_invalid_shared_sub_no_filter() {
        assert!(!validate_topic_filter(b"$share/group", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
    }

    #[test]
    fn test_invalid_shared_sub_wildcard_in_group() {
        assert!(!validate_topic_filter(b"$share/gr+up/topic", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
        assert!(!validate_topic_filter(b"$share/gr#up/topic", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
    }

    #[test]
    fn test_filter_empty() {
        assert!(!validate_topic_filter(b"", MAX_LEVEL_LEN, MAX_LEVEL, MAX_LEN));
    }
}
