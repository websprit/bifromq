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

//! MQTT topic parsing utilities.
//!
//! Corresponds to `TopicUtil.parse()` in BifroMQ Java.

/// Topic level separator
const TOPIC_SEPARATOR: u8 = b'/';

/// Parse a topic string into levels by splitting on `/`.
///
/// This returns byte-slice offsets for zero-copy operation.
/// For example, `"a/b/c"` returns `["a", "b", "c"]`.
/// Leading `/` produces an empty first level: `"/a"` → `["", "a"]`.
pub fn parse(topic: &[u8]) -> Vec<&[u8]> {
    if topic.is_empty() {
        return vec![topic];
    }

    let mut levels = Vec::new();
    let mut start = 0;
    for i in 0..topic.len() {
        if topic[i] == TOPIC_SEPARATOR {
            levels.push(&topic[start..i]);
            start = i + 1;
        }
    }
    levels.push(&topic[start..]);
    levels
}

/// Parse a topic string (as &str) into levels, returning string slices.
pub fn parse_str(topic: &str) -> Vec<&str> {
    topic.split('/').collect()
}

/// Check if a topic filter is a wildcard filter (contains `+` or `#`).
pub fn is_wildcard_filter(filter: &[u8]) -> bool {
    filter.contains(&b'+') || filter.contains(&b'#')
}

/// Check if a topic string starts with the system prefix `$`.
pub fn is_sys_topic(topic: &[u8]) -> bool {
    !topic.is_empty() && topic[0] == b'$'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple() {
        let result = parse(b"a/b/c");
        assert_eq!(result, vec![b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]);
    }

    #[test]
    fn test_parse_single_level() {
        let result = parse(b"hello");
        assert_eq!(result, vec![b"hello".as_slice()]);
    }

    #[test]
    fn test_parse_leading_separator() {
        let result = parse(b"/a/b");
        assert_eq!(result, vec![b"".as_slice(), b"a".as_slice(), b"b".as_slice()]);
    }

    #[test]
    fn test_parse_trailing_separator() {
        let result = parse(b"a/b/");
        assert_eq!(result, vec![b"a".as_slice(), b"b".as_slice(), b"".as_slice()]);
    }

    #[test]
    fn test_parse_empty() {
        let result = parse(b"");
        assert_eq!(result, vec![b"".as_slice()]);
    }

    #[test]
    fn test_parse_root_separator() {
        let result = parse(b"/");
        assert_eq!(result, vec![b"".as_slice(), b"".as_slice()]);
    }

    #[test]
    fn test_parse_str() {
        let result = parse_str("a/b/c");
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_is_wildcard_filter() {
        assert!(is_wildcard_filter(b"a/+/c"));
        assert!(is_wildcard_filter(b"a/#"));
        assert!(is_wildcard_filter(b"+"));
        assert!(is_wildcard_filter(b"#"));
        assert!(!is_wildcard_filter(b"a/b/c"));
    }

    #[test]
    fn test_is_sys_topic() {
        assert!(is_sys_topic(b"$SYS/broker"));
        assert!(is_sys_topic(b"$share/group/topic"));
        assert!(!is_sys_topic(b"normal/topic"));
        assert!(!is_sys_topic(b""));
    }
}
