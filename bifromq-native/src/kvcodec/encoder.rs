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

//! KV key encoder for BifroMQ distribution worker schema.
//!
//! Binary layout matches `KVSchemaUtil.java` exactly:
//!
//! tenant_begin_key:
//!   [SCHEMA_VER(0x00)] [short:tenantLen] [tenantId bytes...]
//!
//! normal_route_key:
//!   [SCHEMA_VER(0x00)] [short:tenantLen] [tenantId bytes...]
//!   [level1] [0x00] [level2] [0x00] ... [levelN] [0x00]
//!   [0x00] [bucket] [0x01] [receiverUrl bytes...] [short:receiverUrlLen]
//!
//! group_route_key:
//!   [SCHEMA_VER(0x00)] [short:tenantLen] [tenantId bytes...]
//!   [level1] [0x00] [level2] [0x00] ... [levelN] [0x00]
//!   [0x00] [bucket] [flag] [group bytes...] [short:groupLen]

/// Schema version prefix byte (matches Java SCHEMA_VER = {0x00})
const SCHEMA_VER: u8 = 0x00;

/// Separator byte between components (matches Java SEPARATOR_BYTE = {0x00})
const SEPARATOR: u8 = 0x00;

/// Flag byte constants (matching Java KVSchemaConstants)
const FLAG_NORMAL: u8 = 0x01;
const FLAG_UNORDERED: u8 = 0x02;
const FLAG_ORDERED: u8 = 0x03;

/// Max receiver buckets mask (matching Java MAX_RECEIVER_BUCKETS = 0xFF)
const MAX_RECEIVER_BUCKETS: u32 = 0xFF;

/// Java-compatible String.hashCode() implementation.
/// Java iterates over UTF-16 code units (char values), not UTF-8 bytes.
/// For BMP characters: one u16 code unit per char.
/// For supplementary characters: two u16 code units (surrogate pair).
fn java_string_hashcode(utf8_bytes: &[u8]) -> i32 {
    let s = match std::str::from_utf8(utf8_bytes) {
        Ok(s) => s,
        Err(_) => return 0, // invalid UTF-8
    };
    let mut h: i32 = 0;
    for c in s.chars() {
        let mut buf = [0u16; 2];
        let encoded = c.encode_utf16(&mut buf);
        for &code_unit in encoded.iter() {
            h = h.wrapping_mul(31).wrapping_add(code_unit as i32);
        }
    }
    h
}

/// Compute the bucket byte for a receiver string (matches Java KVSchemaUtil.bucket()).
/// `bucket = (hash ^ (hash >>> 16)) & MAX_RECEIVER_BUCKETS`
fn compute_bucket(receiver: &[u8]) -> u8 {
    let hash = java_string_hashcode(receiver);
    let spread = hash ^ ((hash as u32 >> 16) as i32);
    (spread as u32 & MAX_RECEIVER_BUCKETS) as u8
}

/// Write schema version prefix.
fn write_schema_ver(buf: &mut Vec<u8>) {
    buf.push(SCHEMA_VER);
}

/// Write a Java short (big-endian, 2 bytes).
fn write_short(buf: &mut Vec<u8>, val: u16) {
    buf.extend_from_slice(&val.to_be_bytes());
}

/// Write the tenant begin key prefix:
/// `[SCHEMA_VER] [short:tenantLen] [tenantId]`
fn write_tenant_prefix(buf: &mut Vec<u8>, tenant_id: &[u8]) {
    write_schema_ver(buf);
    write_short(buf, tenant_id.len() as u16);
    buf.extend_from_slice(tenant_id);
}

/// Write topic filter levels, each followed by SEPARATOR (0x00):
/// `[level1] [0x00] [level2] [0x00] ... [levelN] [0x00]`
fn write_filter_levels(buf: &mut Vec<u8>, filter_levels_data: &[u8]) {
    // filter_levels_data comes pre-encoded from Java as:
    // level1 + 0x00 + level2 + 0x00 + ... + levelN + 0x00
    buf.extend_from_slice(filter_levels_data);
}

/// Write receiver bytes with trailing length:
/// `[receiverUrl] [short:receiverUrlLen]`
fn write_receiver_bytes(buf: &mut Vec<u8>, receiver: &[u8]) {
    buf.extend_from_slice(receiver);
    write_short(buf, receiver.len() as u16);
}

/// Encode a tenant begin key.
/// Layout: `[SCHEMA_VER(0x00)] [short:tenantLen] [tenantId]`
pub fn encode_tenant_begin_key(tenant_id: &[u8], buf: &mut Vec<u8>) {
    buf.clear();
    write_tenant_prefix(buf, tenant_id);
}

/// Encode a tenant route start key (for range scans).
/// Layout: `[tenantPrefix] [level1] [0x00] ... [levelN] [0x00] [0x00]`
///
/// `filter_levels_data` is the pre-encoded filter levels (each followed by 0x00).
pub fn encode_tenant_route_start_key(
    tenant_id: &[u8],
    filter_levels_data: &[u8],
    buf: &mut Vec<u8>,
) {
    buf.clear();
    write_tenant_prefix(buf, tenant_id);
    write_filter_levels(buf, filter_levels_data);
    buf.push(SEPARATOR); // extra separator after levels
}

/// Encode a normal route key.
/// Layout: `[tenantPrefix] [levels...] [0x00] [bucket] [FLAG_NORMAL] [receiverUrl] [short:receiverLen]`
///
/// `filter_levels_data` is the pre-encoded filter levels (each level followed by 0x00).
/// `receiver_url` is the receiver URL string bytes.
pub fn encode_normal_route_key(
    tenant_id: &[u8],
    filter_levels_data: &[u8],
    receiver_url: &[u8],
    buf: &mut Vec<u8>,
) {
    buf.clear();
    write_tenant_prefix(buf, tenant_id);
    write_filter_levels(buf, filter_levels_data);
    buf.push(SEPARATOR); // extra separator
    buf.push(compute_bucket(receiver_url));
    buf.push(FLAG_NORMAL);
    write_receiver_bytes(buf, receiver_url);
}

/// Encode a group route key.
/// Layout: `[tenantPrefix] [levels...] [0x00] [bucket] [flag] [group] [short:groupLen]`
///
/// `filter_levels_data` is the pre-encoded filter levels.
/// `group` is the group name bytes.
/// `is_ordered` determines if FLAG_ORDERED (0x03) or FLAG_UNORDERED (0x02) is used.
pub fn encode_group_route_key(
    tenant_id: &[u8],
    filter_levels_data: &[u8],
    group: &[u8],
    is_ordered: bool,
    buf: &mut Vec<u8>,
) {
    buf.clear();
    write_tenant_prefix(buf, tenant_id);
    write_filter_levels(buf, filter_levels_data);
    buf.push(SEPARATOR); // extra separator
    buf.push(compute_bucket(group));
    buf.push(if is_ordered { FLAG_ORDERED } else { FLAG_UNORDERED });
    write_receiver_bytes(buf, group);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_java_string_hashcode() {
        // "hello" in Java: h=0 → 104 → 3309 → 99162 → 3060183 → 99162322
        // Note: Java String.hashCode operates on chars (UTF-16), for ASCII this is the same
        assert_eq!(java_string_hashcode(b"hello"), 99162322);
        assert_eq!(java_string_hashcode(b""), 0);
    }

    #[test]
    fn test_compute_bucket() {
        let bucket = compute_bucket(b"receiver1");
        assert!(bucket <= MAX_RECEIVER_BUCKETS as u8);
    }

    #[test]
    fn test_encode_tenant_begin_key() {
        let mut buf = Vec::new();
        encode_tenant_begin_key(b"tenant1", &mut buf);
        // [SCHEMA_VER(0x00)] [short:7 = 0x00,0x07] [t,e,n,a,n,t,1]
        assert_eq!(buf[0], 0x00); // schema ver
        assert_eq!(&buf[1..3], &[0x00, 0x07]); // short(7)
        assert_eq!(&buf[3..], b"tenant1");
    }

    #[test]
    fn test_encode_normal_route_key() {
        let mut buf = Vec::new();
        // Filter levels pre-encoded: "a" + 0x00 + "b" + 0x00 + "c" + 0x00
        let filter_data = b"a\x00b\x00c\x00";
        encode_normal_route_key(b"t1", filter_data, b"recv", &mut buf);
        // Expected:
        // [0x00] [0x00, 0x02] [t,1] [a,0x00,b,0x00,c,0x00] [0x00] [bucket] [0x01] [r,e,c,v] [0x00, 0x04]
        assert_eq!(buf[0], 0x00); // schema ver
        assert_eq!(&buf[1..3], &[0x00, 0x02]); // short(2) = tenantLen
        assert_eq!(&buf[3..5], b"t1");
        assert_eq!(&buf[5..11], filter_data); // a\0b\0c\0
        assert_eq!(buf[11], 0x00); // extra separator
        // buf[12] = bucket (computed from "recv")
        assert_eq!(buf[13], FLAG_NORMAL); // 0x01
        assert_eq!(&buf[14..18], b"recv");
        assert_eq!(&buf[18..20], &[0x00, 0x04]); // short(4) = receiverLen
    }

    #[test]
    fn test_encode_group_route_key() {
        let mut buf = Vec::new();
        let filter_data = b"x\x00+\x00";
        encode_group_route_key(b"t2", filter_data, b"group1", false, &mut buf);
        // [0x00] [0x00, 0x02] [t,2] [x,0x00,+,0x00] [0x00] [bucket] [0x02] [g,r,o,u,p,1] [0x00, 0x06]
        assert_eq!(buf[0], 0x00);
        assert_eq!(&buf[1..3], &[0x00, 0x02]);
        assert_eq!(&buf[3..5], b"t2");
        assert_eq!(&buf[5..9], filter_data);
        assert_eq!(buf[9], 0x00); // extra separator
        // buf[10] = bucket
        assert_eq!(buf[11], FLAG_UNORDERED);
        assert_eq!(&buf[12..18], b"group1");
        assert_eq!(&buf[18..20], &[0x00, 0x06]);
    }

    #[test]
    fn test_encode_group_route_key_ordered() {
        let mut buf = Vec::new();
        encode_group_route_key(b"t", b"a\x00", b"g", true, &mut buf);
        // VER(1) + short(2) + tenant "t"(1) + filter "a\0"(2) + sep(1) + bucket(1) = 8
        let flag_pos = 1 + 2 + 1 + 2 + 1 + 1;
        assert_eq!(buf[flag_pos], FLAG_ORDERED);
    }
}
