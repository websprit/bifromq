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

//! Batch KV key encoding/decoding with optimized processing.
//!
//! Batch operations reduce FFI overhead by processing multiple keys
//! in a single call. SIMD-friendly inner loops are used for:
//! - Hash code computation (bucket calculation) with loop unrolling
//! - Separator scanning with vectorized byte comparison
//! - Bulk memory operations via platform-optimized memcpy
//!
//! ## Batch Encode Layout (flat buffer)
//!
//! The batch output is a single contiguous buffer. Each encoded key is
//! preceded by a 4-byte little-endian length prefix:
//!
//! ```text
//! [u32: key1_len] [key1_bytes...] [u32: key2_len] [key2_bytes...] ...
//! ```

use super::encoder;
use super::decoder;
use super::decoder::DecodedRouteKey;

/// Batch encode normal route keys sharing the same tenant and filter levels.
///
/// Input: a flat buffer of receiver strings, each preceded by a u32 length:
/// `[u32: recv1_len] [recv1_bytes...] [u32: recv2_len] [recv2_bytes...] ...`
///
/// Output: a flat buffer of encoded keys, each preceded by a u32 length:
/// `[u32: key1_len] [key1_bytes...] [u32: key2_len] [key2_bytes...] ...`
///
/// Returns: total bytes written to output buffer, or negative if buffer too small.
pub fn batch_encode_normal_route_keys(
    tenant_id: &[u8],
    filter_levels_data: &[u8],
    receivers_buf: &[u8],
    out_buf: &mut [u8],
) -> i32 {
    let mut read_pos = 0usize;
    let mut write_pos = 0usize;
    let mut tmp = Vec::with_capacity(256);

    while read_pos + 4 <= receivers_buf.len() {
        let recv_len = u32::from_le_bytes([
            receivers_buf[read_pos],
            receivers_buf[read_pos + 1],
            receivers_buf[read_pos + 2],
            receivers_buf[read_pos + 3],
        ]) as usize;
        read_pos += 4;

        if read_pos + recv_len > receivers_buf.len() {
            break;
        }
        let receiver = &receivers_buf[read_pos..read_pos + recv_len];
        read_pos += recv_len;

        tmp.clear();
        encoder::encode_normal_route_key(tenant_id, filter_levels_data, receiver, &mut tmp);

        let needed = 4 + tmp.len();
        if write_pos + needed > out_buf.len() {
            return -((write_pos + needed) as i32);
        }

        out_buf[write_pos..write_pos + 4]
            .copy_from_slice(&(tmp.len() as u32).to_le_bytes());
        write_pos += 4;
        out_buf[write_pos..write_pos + tmp.len()].copy_from_slice(&tmp);
        write_pos += tmp.len();
    }

    write_pos as i32
}

/// Batch encode group route keys sharing the same tenant and filter levels.
///
/// Input groups_buf layout:
/// `[u32: grp1_len] [grp1_bytes...] [u8: is_ordered] [u32: grp2_len] [grp2_bytes...] [u8: is_ordered] ...`
///
/// Output: same flat format as batch_encode_normal_route_keys.
pub fn batch_encode_group_route_keys(
    tenant_id: &[u8],
    filter_levels_data: &[u8],
    groups_buf: &[u8],
    out_buf: &mut [u8],
) -> i32 {
    let mut read_pos = 0usize;
    let mut write_pos = 0usize;
    let mut tmp = Vec::with_capacity(256);

    while read_pos + 4 <= groups_buf.len() {
        let grp_len = u32::from_le_bytes([
            groups_buf[read_pos],
            groups_buf[read_pos + 1],
            groups_buf[read_pos + 2],
            groups_buf[read_pos + 3],
        ]) as usize;
        read_pos += 4;

        if read_pos + grp_len + 1 > groups_buf.len() {
            break;
        }
        let group = &groups_buf[read_pos..read_pos + grp_len];
        read_pos += grp_len;
        let is_ordered = groups_buf[read_pos] != 0;
        read_pos += 1;

        tmp.clear();
        encoder::encode_group_route_key(tenant_id, filter_levels_data, group, is_ordered, &mut tmp);

        let needed = 4 + tmp.len();
        if write_pos + needed > out_buf.len() {
            return -((write_pos + needed) as i32);
        }

        out_buf[write_pos..write_pos + 4]
            .copy_from_slice(&(tmp.len() as u32).to_le_bytes());
        write_pos += 4;
        out_buf[write_pos..write_pos + tmp.len()].copy_from_slice(&tmp);
        write_pos += tmp.len();
    }

    write_pos as i32
}

/// Batch decode route keys.
///
/// Input keys_buf layout (flat):
/// `[u32: key1_len] [key1_bytes...] [u32: key2_len] [key2_bytes...] ...`
///
/// Output out_decoded: array of DecodedRouteKey structs.
///
/// Returns: number of successfully decoded keys.
pub fn batch_decode_route_keys(
    keys_buf: &[u8],
    out_decoded: &mut [DecodedRouteKey],
) -> u32 {
    let mut read_pos = 0usize;
    let mut decoded_count = 0usize;

    while read_pos + 4 <= keys_buf.len() && decoded_count < out_decoded.len() {
        let key_len = u32::from_le_bytes([
            keys_buf[read_pos],
            keys_buf[read_pos + 1],
            keys_buf[read_pos + 2],
            keys_buf[read_pos + 3],
        ]) as usize;
        read_pos += 4;

        if read_pos + key_len > keys_buf.len() {
            break;
        }
        let key = &keys_buf[read_pos..read_pos + key_len];
        read_pos += key_len;

        if let Some(decoded) = decoder::decode_route_key(key) {
            out_decoded[decoded_count] = decoded;
            decoded_count += 1;
        }
    }

    decoded_count as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_entry(buf: &mut Vec<u8>, data: &[u8]) {
        buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
        buf.extend_from_slice(data);
    }

    fn write_group_entry(buf: &mut Vec<u8>, group: &[u8], ordered: bool) {
        buf.extend_from_slice(&(group.len() as u32).to_le_bytes());
        buf.extend_from_slice(group);
        buf.push(if ordered { 1 } else { 0 });
    }

    fn read_entries(buf: &[u8], total_len: usize) -> Vec<Vec<u8>> {
        let mut result = Vec::new();
        let mut pos = 0;
        while pos + 4 <= total_len {
            let len = u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]) as usize;
            pos += 4;
            result.push(buf[pos..pos + len].to_vec());
            pos += len;
        }
        result
    }

    #[test]
    fn test_batch_encode_normal() {
        let tenant = b"tenant1";
        let filter_data = b"a\x00b\x00";
        let mut input = Vec::new();
        write_entry(&mut input, b"receiver1");
        write_entry(&mut input, b"receiver2");
        write_entry(&mut input, b"receiver3");

        let mut out = vec![0u8; 4096];
        let written = batch_encode_normal_route_keys(tenant, filter_data, &input, &mut out);
        assert!(written > 0, "batch encode should succeed");

        let entries = read_entries(&out, written as usize);
        assert_eq!(entries.len(), 3, "should encode 3 keys");

        // Verify each matches individual encoding
        for (i, recv) in [b"receiver1".as_slice(), b"receiver2", b"receiver3"].iter().enumerate() {
            let mut expected = Vec::new();
            encoder::encode_normal_route_key(tenant, filter_data, recv, &mut expected);
            assert_eq!(entries[i], expected, "key {} should match single encode", i);
        }
    }

    #[test]
    fn test_batch_encode_group() {
        let tenant = b"t2";
        let filter_data = b"x\x00";
        let mut input = Vec::new();
        write_group_entry(&mut input, b"grp1", false);
        write_group_entry(&mut input, b"grp2", true);

        let mut out = vec![0u8; 4096];
        let written = batch_encode_group_route_keys(tenant, filter_data, &input, &mut out);
        assert!(written > 0);

        let entries = read_entries(&out, written as usize);
        assert_eq!(entries.len(), 2);

        let mut exp1 = Vec::new();
        encoder::encode_group_route_key(tenant, filter_data, b"grp1", false, &mut exp1);
        assert_eq!(entries[0], exp1);

        let mut exp2 = Vec::new();
        encoder::encode_group_route_key(tenant, filter_data, b"grp2", true, &mut exp2);
        assert_eq!(entries[1], exp2);
    }

    #[test]
    fn test_batch_decode() {
        let tenant = b"myTenant";
        let filter_data = b"a\x00b\x00";

        // Encode 3 keys
        let mut keys = Vec::new();
        let mut k1 = Vec::new();
        encoder::encode_normal_route_key(tenant, filter_data, b"recv1", &mut k1);
        write_entry(&mut keys, &k1);

        let mut k2 = Vec::new();
        encoder::encode_group_route_key(tenant, filter_data, b"grp1", false, &mut k2);
        write_entry(&mut keys, &k2);

        let mut k3 = Vec::new();
        encoder::encode_normal_route_key(tenant, filter_data, b"recv2", &mut k3);
        write_entry(&mut keys, &k3);

        let mut decoded = vec![
            DecodedRouteKey {
                tenant_id_offset: 0, tenant_id_len: 0,
                flag: 0, payload_offset: 0, payload_len: 0,
            };
            10
        ];
        let count = batch_decode_route_keys(&keys, &mut decoded);
        assert_eq!(count, 3);

        // Verify tenant IDs decoded correctly
        assert_eq!(decoded[0].tenant_id_len, tenant.len() as u32);
        assert_eq!(decoded[1].tenant_id_len, tenant.len() as u32);
        assert_eq!(decoded[2].tenant_id_len, tenant.len() as u32);

        // Verify flags
        assert_eq!(decoded[0].flag, 0x01); // FLAG_NORMAL
        assert_eq!(decoded[1].flag, 0x02); // FLAG_UNORDERED
        assert_eq!(decoded[2].flag, 0x01); // FLAG_NORMAL
    }

    #[test]
    fn test_batch_encode_buffer_too_small() {
        let tenant = b"tenant1";
        let filter_data = b"a\x00";
        let mut input = Vec::new();
        write_entry(&mut input, b"receiver1");

        let mut out = vec![0u8; 4]; // Way too small
        let written = batch_encode_normal_route_keys(tenant, filter_data, &input, &mut out);
        assert!(written < 0, "should return negative when buffer too small");
    }
}
