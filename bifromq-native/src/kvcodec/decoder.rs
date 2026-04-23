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

//! KV key decoder for BifroMQ distribution worker schema.
//!
//! Matches the binary layout produced by `KVSchemaUtil.java`:
//!
//! Route key layout:
//!   [SCHEMA_VER(0x00)] [short:tenantLen] [tenantId bytes...]
//!   [level1] [0x00] [level2] [0x00] ... [levelN] [0x00]
//!   [0x00] [bucket] [flag] [receiverUrl bytes...] [short:receiverUrlLen]
//!
//! Returns offsets and lengths to enable zero-copy on the Java side.

/// Schema version byte
const SCHEMA_VER: u8 = 0x00;

/// Flag values
const FLAG_NORMAL: u8 = 0x01;
const FLAG_UNORDERED: u8 = 0x02;
const FLAG_ORDERED: u8 = 0x03;

/// Read a big-endian u16 (Java short) from key at position.
fn read_short(key: &[u8], pos: usize) -> Option<u16> {
    if pos + 2 > key.len() {
        return None;
    }
    Some(u16::from_be_bytes([key[pos], key[pos + 1]]))
}

/// Decoded route key components.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct DecodedRouteKey {
    /// Offset of tenant ID in the original key
    pub tenant_id_offset: u32,
    /// Length of tenant ID
    pub tenant_id_len: u32,
    /// Flag byte (FLAG_NORMAL/FLAG_UNORDERED/FLAG_ORDERED)
    pub flag: u8,
    /// Offset of receiver/group bytes
    pub payload_offset: u32,
    /// Length of receiver/group bytes
    pub payload_len: u32,
}

/// Decode a route key into its components.
///
/// Layout: `[VER(0x00)] [short:tenantLen] [tenant] [...levels...] [0x00] [bucket] [flag] [receiver] [short:receiverLen]`
///
/// Returns None if the key format is invalid.
pub fn decode_route_key(key: &[u8]) -> Option<DecodedRouteKey> {
    // Minimum: VER(1) + short(2) + tenant(1+) + sep(1) + bucket(1) + flag(1) + short(2) = 9+
    if key.len() < 9 {
        return None;
    }

    // [0]: SCHEMA_VER
    if key[0] != SCHEMA_VER {
        return None;
    }

    // [1..3]: short(tenantLen)
    let tenant_len = read_short(key, 1)? as usize;
    let tenant_offset = 3usize;
    if tenant_offset + tenant_len >= key.len() {
        return None;
    }

    // Read receiver length from the trailing short
    let receiver_len = read_short(key, key.len() - 2)? as usize;

    // Validate: receiverBytes start position
    // key.len() - 2 (receiverLenShort) - receiverLen = receiver start
    if key.len() < 2 + receiver_len {
        return None;
    }
    let receiver_offset = key.len() - 2 - receiver_len;

    // Flag byte is right before receiver bytes
    if receiver_offset == 0 {
        return None;
    }
    let flag_idx = receiver_offset - 1;
    let flag = key[flag_idx];

    if flag != FLAG_NORMAL && flag != FLAG_UNORDERED && flag != FLAG_ORDERED {
        return None;
    }

    Some(DecodedRouteKey {
        tenant_id_offset: tenant_offset as u32,
        tenant_id_len: tenant_len as u32,
        flag,
        payload_offset: receiver_offset as u32,
        payload_len: receiver_len as u32,
    })
}

/// Decode only the tenant ID from a route key (fast path).
///
/// Layout starts with: `[VER(0x00)] [short:tenantLen] [tenant...]`
///
/// Returns (offset, length) or None if invalid.
pub fn decode_tenant_id(key: &[u8]) -> Option<(u32, u32)> {
    if key.len() < 4 { // VER(1) + short(2) + tenant(1+)
        return None;
    }
    if key[0] != SCHEMA_VER {
        return None;
    }
    let tenant_len = read_short(key, 1)? as usize;
    let tenant_offset = 3usize;
    if tenant_offset + tenant_len > key.len() {
        return None;
    }
    Some((tenant_offset as u32, tenant_len as u32))
}

/// Decode only the flag byte from a route key.
///
/// Flag is at position: key.len() - 2 (receiverLenShort) - receiverLen - 1
///
/// Returns the flag byte or None if invalid.
pub fn decode_flag(key: &[u8]) -> Option<u8> {
    if key.len() < 6 { // minimum for a valid key with flag
        return None;
    }
    let receiver_len = read_short(key, key.len() - 2)? as usize;
    if key.len() < 2 + receiver_len + 1 {
        return None;
    }
    let flag_idx = key.len() - 2 - receiver_len - 1;
    let flag = key[flag_idx];
    if flag != FLAG_NORMAL && flag != FLAG_UNORDERED && flag != FLAG_ORDERED {
        return None;
    }
    Some(flag)
}

/// Check if a flag indicates a normal route.
pub fn is_normal_route(flag: u8) -> bool {
    flag == FLAG_NORMAL
}

/// Check if a flag indicates a group route (ordered or unordered).
pub fn is_group_route(flag: u8) -> bool {
    flag == FLAG_UNORDERED || flag == FLAG_ORDERED
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kvcodec::encoder;

    #[test]
    fn test_decode_normal_route_key() {
        let mut buf = Vec::new();
        // Filter levels pre-encoded: "a\0b\0"
        encoder::encode_normal_route_key(b"tenant1", b"a\x00b\x00", b"receiver1", &mut buf);

        let decoded = decode_route_key(&buf).unwrap();
        let start = decoded.tenant_id_offset as usize;
        let end = start + decoded.tenant_id_len as usize;
        assert_eq!(&buf[start..end], b"tenant1");
        assert_eq!(decoded.flag, FLAG_NORMAL);
        let p_start = decoded.payload_offset as usize;
        let p_end = p_start + decoded.payload_len as usize;
        assert_eq!(&buf[p_start..p_end], b"receiver1");
    }

    #[test]
    fn test_decode_group_route_key() {
        let mut buf = Vec::new();
        encoder::encode_group_route_key(b"t2", b"x\x00", b"grp", false, &mut buf);

        let decoded = decode_route_key(&buf).unwrap();
        assert_eq!(&buf[decoded.tenant_id_offset as usize..(decoded.tenant_id_offset + decoded.tenant_id_len) as usize], b"t2");
        assert!(is_group_route(decoded.flag));
        assert_eq!(decoded.flag, FLAG_UNORDERED);
        assert_eq!(&buf[decoded.payload_offset as usize..(decoded.payload_offset + decoded.payload_len) as usize], b"grp");
    }

    #[test]
    fn test_decode_ordered_group() {
        let mut buf = Vec::new();
        encoder::encode_group_route_key(b"t3", b"a\x00", b"g1", true, &mut buf);

        let decoded = decode_route_key(&buf).unwrap();
        assert_eq!(decoded.flag, FLAG_ORDERED);
    }

    #[test]
    fn test_decode_tenant_id() {
        let mut buf = Vec::new();
        encoder::encode_normal_route_key(b"myTenant", b"a\x00", b"r", &mut buf);

        let (offset, len) = decode_tenant_id(&buf).unwrap();
        assert_eq!(&buf[offset as usize..(offset + len) as usize], b"myTenant");
    }

    #[test]
    fn test_decode_flag() {
        let mut buf = Vec::new();
        encoder::encode_normal_route_key(b"t", b"a\x00", b"r", &mut buf);
        assert_eq!(decode_flag(&buf), Some(FLAG_NORMAL));

        encoder::encode_group_route_key(b"t", b"a\x00", b"g", false, &mut buf);
        assert_eq!(decode_flag(&buf), Some(FLAG_UNORDERED));
    }

    #[test]
    fn test_decode_tenant_begin_key_no_flag() {
        // Tenant begin key has no flag/receiver, so decode_route_key should return None
        let mut buf = Vec::new();
        encoder::encode_tenant_begin_key(b"t", &mut buf);
        assert!(decode_route_key(&buf).is_none());
    }
}
