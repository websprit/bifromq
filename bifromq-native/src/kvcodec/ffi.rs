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

//! C ABI exports for KV key encoding/decoding.
//!
//! Binary format matches Java `KVSchemaUtil.java` exactly.

use std::slice;
use super::encoder;
use super::decoder;
use super::decoder::DecodedRouteKey;
use super::batch;

// ============================================================
// Encoding
// ============================================================

/// Encode a tenant begin key into `out_buf`.
/// Returns the number of bytes written, or negative if buffer too small.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kv_encode_tenant_begin_key(
    tenant_ptr: *const u8,
    tenant_len: u32,
    out_buf: *mut u8,
    buf_cap: u32,
) -> i32 {
    let tenant = unsafe { slice::from_raw_parts(tenant_ptr, tenant_len as usize) };
    let mut tmp = Vec::new();
    encoder::encode_tenant_begin_key(tenant, &mut tmp);
    write_to_buf(&tmp, out_buf, buf_cap)
}

/// Encode a normal route key.
///
/// `filter_levels_data`: pre-encoded filter levels, each level followed by 0x00.
///   e.g. for levels ["a","b","c"]: "a\x00b\x00c\x00"
/// `receiver`: receiver URL bytes.
///
/// Returns bytes written or negative if buffer too small.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kv_encode_normal_route_key(
    tenant_ptr: *const u8,
    tenant_len: u32,
    filter_levels_data_ptr: *const u8,
    filter_levels_data_len: u32,
    receiver_ptr: *const u8,
    receiver_len: u32,
    out_buf: *mut u8,
    buf_cap: u32,
) -> i32 {
    let tenant = unsafe { slice::from_raw_parts(tenant_ptr, tenant_len as usize) };
    let filter_data = unsafe { slice::from_raw_parts(filter_levels_data_ptr, filter_levels_data_len as usize) };
    let receiver = unsafe { slice::from_raw_parts(receiver_ptr, receiver_len as usize) };

    let mut tmp = Vec::new();
    encoder::encode_normal_route_key(tenant, filter_data, receiver, &mut tmp);
    write_to_buf(&tmp, out_buf, buf_cap)
}

/// Encode a group route key.
///
/// `filter_levels_data`: pre-encoded filter levels (each followed by 0x00).
/// `group`: group name bytes.
/// `is_ordered`: 1 for ordered share, 0 for unordered share.
///
/// Returns bytes written or negative if buffer too small.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kv_encode_group_route_key(
    tenant_ptr: *const u8,
    tenant_len: u32,
    filter_levels_data_ptr: *const u8,
    filter_levels_data_len: u32,
    group_ptr: *const u8,
    group_len: u32,
    is_ordered: u8,
    out_buf: *mut u8,
    buf_cap: u32,
) -> i32 {
    let tenant = unsafe { slice::from_raw_parts(tenant_ptr, tenant_len as usize) };
    let filter_data = unsafe { slice::from_raw_parts(filter_levels_data_ptr, filter_levels_data_len as usize) };
    let group = unsafe { slice::from_raw_parts(group_ptr, group_len as usize) };

    let mut tmp = Vec::new();
    encoder::encode_group_route_key(tenant, filter_data, group, is_ordered != 0, &mut tmp);
    write_to_buf(&tmp, out_buf, buf_cap)
}

/// Encode a tenant route start key (for range scans).
/// Layout: `[tenantPrefix] [level1] [0x00] ... [levelN] [0x00] [0x00]`
///
/// Returns bytes written or negative if buffer too small.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kv_encode_route_start_key(
    tenant_ptr: *const u8,
    tenant_len: u32,
    filter_levels_data_ptr: *const u8,
    filter_levels_data_len: u32,
    out_buf: *mut u8,
    buf_cap: u32,
) -> i32 {
    let tenant = unsafe { slice::from_raw_parts(tenant_ptr, tenant_len as usize) };
    let filter_data = unsafe { slice::from_raw_parts(filter_levels_data_ptr, filter_levels_data_len as usize) };

    let mut tmp = Vec::new();
    encoder::encode_tenant_route_start_key(tenant, filter_data, &mut tmp);
    write_to_buf(&tmp, out_buf, buf_cap)
}

// ============================================================
// Decoding
// ============================================================

/// Decode a route key into components.
/// Writes result to the provided `DecodedRouteKey` struct.
/// Returns 1 on success, 0 on invalid key.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kv_decode_route_key(
    key_ptr: *const u8,
    key_len: u32,
    out: *mut DecodedRouteKey,
) -> i32 {
    let key = unsafe { slice::from_raw_parts(key_ptr, key_len as usize) };
    match decoder::decode_route_key(key) {
        Some(decoded) => {
            unsafe { *out = decoded; }
            1
        }
        None => 0,
    }
}

/// Decode only the tenant ID.
/// Returns tenant ID length, or 0 if invalid.
/// Writes the start offset to `out_offset`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kv_decode_tenant_id(
    key_ptr: *const u8,
    key_len: u32,
    out_offset: *mut u32,
) -> u32 {
    let key = unsafe { slice::from_raw_parts(key_ptr, key_len as usize) };
    match decoder::decode_tenant_id(key) {
        Some((offset, len)) => {
            unsafe { *out_offset = offset; }
            len
        }
        None => 0,
    }
}

/// Decode the flag byte from a route key.
/// Returns the flag value, or 0xFF if invalid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kv_decode_flag(
    key_ptr: *const u8,
    key_len: u32,
) -> u8 {
    let key = unsafe { slice::from_raw_parts(key_ptr, key_len as usize) };
    decoder::decode_flag(key).unwrap_or(0xFF)
}

// ============================================================
// Batch Operations
// ============================================================

/// Batch encode normal route keys sharing the same tenant and filter levels.
///
/// `receivers_buf`: flat buffer of `[u32:len][bytes]...` (little-endian).
/// `out_buf`: output buffer for `[u32:len][key_bytes]...` (little-endian).
///
/// Returns total bytes written, or negative if buffer too small.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kv_batch_encode_normal_route_keys(
    tenant_ptr: *const u8,
    tenant_len: u32,
    filter_levels_data_ptr: *const u8,
    filter_levels_data_len: u32,
    receivers_buf_ptr: *const u8,
    receivers_buf_len: u32,
    out_buf: *mut u8,
    buf_cap: u32,
) -> i32 {
    let tenant = unsafe { slice::from_raw_parts(tenant_ptr, tenant_len as usize) };
    let filter_data = unsafe { slice::from_raw_parts(filter_levels_data_ptr, filter_levels_data_len as usize) };
    let receivers_buf = unsafe { slice::from_raw_parts(receivers_buf_ptr, receivers_buf_len as usize) };
    let out = unsafe { slice::from_raw_parts_mut(out_buf, buf_cap as usize) };
    batch::batch_encode_normal_route_keys(tenant, filter_data, receivers_buf, out)
}

/// Batch encode group route keys sharing the same tenant and filter levels.
///
/// `groups_buf`: flat buffer of `[u32:len][group_bytes][u8:is_ordered]...` (little-endian).
/// `out_buf`: output buffer for `[u32:len][key_bytes]...` (little-endian).
///
/// Returns total bytes written, or negative if buffer too small.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kv_batch_encode_group_route_keys(
    tenant_ptr: *const u8,
    tenant_len: u32,
    filter_levels_data_ptr: *const u8,
    filter_levels_data_len: u32,
    groups_buf_ptr: *const u8,
    groups_buf_len: u32,
    out_buf: *mut u8,
    buf_cap: u32,
) -> i32 {
    let tenant = unsafe { slice::from_raw_parts(tenant_ptr, tenant_len as usize) };
    let filter_data = unsafe { slice::from_raw_parts(filter_levels_data_ptr, filter_levels_data_len as usize) };
    let groups_buf = unsafe { slice::from_raw_parts(groups_buf_ptr, groups_buf_len as usize) };
    let out = unsafe { slice::from_raw_parts_mut(out_buf, buf_cap as usize) };
    batch::batch_encode_group_route_keys(tenant, filter_data, groups_buf, out)
}

/// Batch decode route keys.
///
/// `keys_buf`: flat buffer of `[u32:len][key_bytes]...` (little-endian).
/// `out_decoded`: array of DecodedRouteKey structs.
///
/// Returns number of successfully decoded keys.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kv_batch_decode_route_keys(
    keys_buf_ptr: *const u8,
    keys_buf_len: u32,
    out_decoded: *mut DecodedRouteKey,
    out_cap: u32,
) -> u32 {
    let keys_buf = unsafe { slice::from_raw_parts(keys_buf_ptr, keys_buf_len as usize) };
    let decoded = unsafe { slice::from_raw_parts_mut(out_decoded, out_cap as usize) };
    batch::batch_decode_route_keys(keys_buf, decoded)
}

// ============================================================
// Helpers
// ============================================================

unsafe fn write_to_buf(data: &[u8], out_buf: *mut u8, buf_cap: u32) -> i32 {
    if data.len() > buf_cap as usize {
        return -(data.len() as i32);
    }
    let buf = unsafe { slice::from_raw_parts_mut(out_buf, buf_cap as usize) };
    buf[..data.len()].copy_from_slice(data);
    data.len() as i32
}
