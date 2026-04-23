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

//! Topic-level trie for MQTT topic matching.
//!
//! This is a Rust implementation of the Java `TopicLevelTrie` from BifroMQ,
//! supporting MQTT wildcard matching (`+` single-level, `#` multi-level).

use std::collections::{HashMap, HashSet};

/// Single-level wildcard
const SINGLE_WILDCARD: &str = "+";
/// Multi-level wildcard
const MULTI_WILDCARD: &str = "#";
/// System topic prefix
const SYS_PREFIX: &str = "$";

/// A node in the topic trie.
struct TrieNode {
    /// Values associated with this exact topic path
    values: HashSet<u64>,
    /// Child nodes keyed by topic level
    children: HashMap<String, TrieNode>,
}

impl TrieNode {
    fn new() -> Self {
        TrieNode {
            values: HashSet::new(),
            children: HashMap::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty() && self.children.is_empty()
    }
}

/// A trie data structure for efficient MQTT topic matching.
///
/// Supports:
/// - Exact topic lookup via `get()`
/// - Wildcard matching via `match_filter()` with `+` and `#`
/// - `$SYS` topics excluded from `+`/`#` at the root level
pub struct TopicTrie {
    root: TrieNode,
}

impl TopicTrie {
    /// Create a new empty trie.
    pub fn new() -> Self {
        TopicTrie {
            root: TrieNode::new(),
        }
    }

    /// Add a topic with associated value id.
    /// Returns `true` if the value was newly inserted, `false` if it already existed.
    pub fn add(&mut self, levels: &[&str], value_id: u64) -> bool {
        let mut node = &mut self.root;
        for &level in levels {
            node = node.children.entry(level.to_string()).or_insert_with(TrieNode::new);
        }
        node.values.insert(value_id)
    }

    /// Remove a topic with associated value id.
    /// Returns `true` if the value was found and removed.
    pub fn remove(&mut self, levels: &[&str], value_id: u64) -> bool {
        Self::remove_recursive(&mut self.root, levels, 0, value_id)
    }

    fn remove_recursive(node: &mut TrieNode, levels: &[&str], depth: usize, value_id: u64) -> bool {
        if depth == levels.len() {
            let removed = node.values.remove(&value_id);
            return removed;
        }

        let level = levels[depth];
        let removed = if let Some(child) = node.children.get_mut(level) {
            let r = Self::remove_recursive(child, levels, depth + 1, value_id);
            if child.is_empty() {
                node.children.remove(level);
            }
            r
        } else {
            false
        };
        removed
    }

    /// Get all values associated with an exact topic path.
    pub fn get(&self, levels: &[&str]) -> Vec<u64> {
        let mut node = &self.root;
        for &level in levels {
            match node.children.get(level) {
                Some(child) => node = child,
                None => return Vec::new(),
            }
        }
        node.values.iter().copied().collect()
    }

    /// Match a topic filter (may contain `+` and `#` wildcards) against all stored topics.
    /// Returns all matching value ids.
    pub fn match_filter(&self, filter_levels: &[&str]) -> Vec<u64> {
        let mut results = HashSet::new();
        self.match_recursive(&self.root, filter_levels, 0, true, &mut results);
        results.into_iter().collect()
    }

    fn match_recursive(
        &self,
        node: &TrieNode,
        filter_levels: &[&str],
        depth: usize,
        is_root: bool,
        results: &mut HashSet<u64>,
    ) {
        if depth == filter_levels.len() {
            results.extend(&node.values);
            return;
        }

        let level = filter_levels[depth];

        match level {
            "#" => {
                // `#` matches everything at this level and below
                self.collect_all(node, is_root, results);
            }
            "+" => {
                // `+` matches exactly one level (but not $SYS at root)
                for (child_level, child_node) in &node.children {
                    // At root level, `+` does not match topics starting with `$`
                    if is_root && child_level.starts_with(SYS_PREFIX) {
                        continue;
                    }
                    self.match_recursive(child_node, filter_levels, depth + 1, false, results);
                }
            }
            _ => {
                // Exact match for this level
                if let Some(child) = node.children.get(level) {
                    self.match_recursive(child, filter_levels, depth + 1, false, results);
                }
            }
        }
    }

    /// Collect all values from this node and all descendants.
    fn collect_all(&self, node: &TrieNode, is_root: bool, results: &mut HashSet<u64>) {
        results.extend(&node.values);
        for (child_level, child_node) in &node.children {
            // At root level, `#` does not match topics starting with `$`
            if is_root && child_level.starts_with(SYS_PREFIX) {
                continue;
            }
            self.collect_all(child_node, false, results);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_get() {
        let mut trie = TopicTrie::new();
        assert!(trie.add(&["a", "b", "c"], 1));
        assert!(trie.add(&["a", "b", "c"], 2));
        assert!(!trie.add(&["a", "b", "c"], 1)); // duplicate

        let values = trie.get(&["a", "b", "c"]);
        assert_eq!(values.len(), 2);
        assert!(values.contains(&1));
        assert!(values.contains(&2));
    }

    #[test]
    fn test_get_nonexistent() {
        let trie = TopicTrie::new();
        assert!(trie.get(&["a", "b"]).is_empty());
    }

    #[test]
    fn test_remove() {
        let mut trie = TopicTrie::new();
        trie.add(&["a", "b"], 1);
        trie.add(&["a", "b"], 2);

        assert!(trie.remove(&["a", "b"], 1));
        assert!(!trie.remove(&["a", "b"], 1)); // already removed

        let values = trie.get(&["a", "b"]);
        assert_eq!(values, vec![2]);
    }

    #[test]
    fn test_remove_cleans_up_empty_nodes() {
        let mut trie = TopicTrie::new();
        trie.add(&["a", "b", "c"], 1);
        trie.remove(&["a", "b", "c"], 1);

        // The entire branch should be cleaned up
        assert!(trie.root.children.is_empty());
    }

    #[test]
    fn test_match_exact() {
        let mut trie = TopicTrie::new();
        trie.add(&["a", "b", "c"], 1);
        trie.add(&["a", "b", "d"], 2);

        let results = trie.match_filter(&["a", "b", "c"]);
        assert_eq!(results, vec![1]);
    }

    #[test]
    fn test_match_single_wildcard() {
        let mut trie = TopicTrie::new();
        trie.add(&["a", "b", "c"], 1);
        trie.add(&["a", "x", "c"], 2);
        trie.add(&["a", "b", "d"], 3);

        let mut results = trie.match_filter(&["a", "+", "c"]);
        results.sort();
        assert_eq!(results, vec![1, 2]);
    }

    #[test]
    fn test_match_multi_wildcard() {
        let mut trie = TopicTrie::new();
        trie.add(&["a", "b", "c"], 1);
        trie.add(&["a", "b", "d"], 2);
        trie.add(&["a", "x"], 3);

        let mut results = trie.match_filter(&["a", "#"]);
        results.sort();
        assert_eq!(results, vec![1, 2, 3]);
    }

    #[test]
    fn test_match_multi_wildcard_root() {
        let mut trie = TopicTrie::new();
        trie.add(&["a", "b"], 1);
        trie.add(&["c"], 2);

        let mut results = trie.match_filter(&["#"]);
        results.sort();
        assert_eq!(results, vec![1, 2]);
    }

    #[test]
    fn test_sys_topic_not_matched_by_wildcards() {
        let mut trie = TopicTrie::new();
        trie.add(&["$SYS", "broker", "uptime"], 1);
        trie.add(&["normal", "topic"], 2);

        // `#` at root should NOT match $SYS
        let results = trie.match_filter(&["#"]);
        assert_eq!(results, vec![2]);

        // `+` at root should NOT match $SYS
        let results = trie.match_filter(&["+", "broker", "uptime"]);
        assert!(results.is_empty());

        // Explicit $SYS prefix should still match
        let results = trie.match_filter(&["$SYS", "#"]);
        assert_eq!(results, vec![1]);

        // Explicit $SYS with + wildcard
        let results = trie.match_filter(&["$SYS", "+", "uptime"]);
        assert_eq!(results, vec![1]);
    }

    #[test]
    fn test_match_combined_wildcards() {
        let mut trie = TopicTrie::new();
        trie.add(&["a", "b", "c", "d"], 1);
        trie.add(&["a", "x", "c", "e"], 2);
        trie.add(&["a", "b", "y"], 3);

        let mut results = trie.match_filter(&["a", "+", "#"]);
        results.sort();
        assert_eq!(results, vec![1, 2, 3]);
    }

    #[test]
    fn test_empty_level() {
        let mut trie = TopicTrie::new();
        trie.add(&["", "a", ""], 1);

        let results = trie.match_filter(&["", "a", ""]);
        assert_eq!(results, vec![1]);

        let results = trie.match_filter(&["", "+", ""]);
        assert_eq!(results, vec![1]);
    }

    #[test]
    fn test_single_level_topic() {
        let mut trie = TopicTrie::new();
        trie.add(&["hello"], 1);

        let results = trie.match_filter(&["hello"]);
        assert_eq!(results, vec![1]);

        let results = trie.match_filter(&["+"]);
        assert_eq!(results, vec![1]);

        let results = trie.match_filter(&["#"]);
        assert_eq!(results, vec![1]);
    }
}
