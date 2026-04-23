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

use flate2::read::{GzDecoder, GzEncoder};
use flate2::Compression;
use std::io;
use std::io::Read;

/// GZIP compress input data.
///
/// Returns the number of bytes written to output on success,
/// or a negative number indicating the required capacity if output is too small.
#[unsafe(no_mangle)]
pub extern "C" fn gzip_compress(
    input_ptr: *const u8,
    input_len: i32,
    output_ptr: *mut u8,
    output_cap: i32,
) -> i32 {
    if input_ptr.is_null() || output_ptr.is_null() || input_len < 0 || output_cap < 0 {
        return -1;
    }

    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len as usize) };

    // Fast path: compress directly into the provided output buffer.
    let output = unsafe { std::slice::from_raw_parts_mut(output_ptr, output_cap as usize) };
    let mut cursor = std::io::Cursor::new(output);
    let mut encoder = GzEncoder::new(input, Compression::default());

    if io::copy(&mut encoder, &mut cursor).is_ok() {
        return cursor.position() as i32;
    }

    // Slow path: buffer into a Vec to determine exact size, then copy if it fits.
    let mut encoder = GzEncoder::new(input, Compression::default());
    let mut compressed = Vec::with_capacity(input.len());
    if encoder.read_to_end(&mut compressed).is_err() {
        return -1;
    }

    let out_len = compressed.len();
    if out_len > output_cap as usize {
        return -(out_len as i32);
    }

    let output = unsafe { std::slice::from_raw_parts_mut(output_ptr, output_cap as usize) };
    output[..out_len].copy_from_slice(&compressed);
    out_len as i32
}

/// GZIP decompress input data.
///
/// Returns the number of bytes written to output on success,
/// or a negative number indicating the required capacity if output is too small.
#[unsafe(no_mangle)]
pub extern "C" fn gzip_decompress(
    input_ptr: *const u8,
    input_len: i32,
    output_ptr: *mut u8,
    output_cap: i32,
) -> i32 {
    if input_ptr.is_null() || output_ptr.is_null() || input_len < 0 || output_cap < 0 {
        return -1;
    }

    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len as usize) };

    // Fast path: decompress directly into the provided output buffer.
    let output = unsafe { std::slice::from_raw_parts_mut(output_ptr, output_cap as usize) };
    let mut cursor = std::io::Cursor::new(output);
    let mut decoder = GzDecoder::new(input);

    if io::copy(&mut decoder, &mut cursor).is_ok() {
        return cursor.position() as i32;
    }

    // Slow path: buffer into a Vec to determine exact size, then copy if it fits.
    let mut decoder = GzDecoder::new(input);
    let mut decompressed = Vec::with_capacity(input.len() * 2);
    if decoder.read_to_end(&mut decompressed).is_err() {
        return -1;
    }

    let out_len = decompressed.len();
    if out_len > output_cap as usize {
        return -(out_len as i32);
    }

    let output = unsafe { std::slice::from_raw_parts_mut(output_ptr, output_cap as usize) };
    output[..out_len].copy_from_slice(&decompressed);
    out_len as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress_roundtrip() {
        let input = b"Hello, BifroMQ! This is a test message for GZIP compression via Rust flate2.";
        let mut compressed = vec![0u8; 1024];
        let mut decompressed = vec![0u8; 1024];

        let comp_len = gzip_compress(
            input.as_ptr(), input.len() as i32,
            compressed.as_mut_ptr(), compressed.len() as i32,
        );
        assert!(comp_len > 0, "Compression failed");

        let decomp_len = gzip_decompress(
            compressed.as_ptr(), comp_len,
            decompressed.as_mut_ptr(), decompressed.len() as i32,
        );
        assert!(decomp_len > 0, "Decompression failed");
        assert_eq!(decomp_len as usize, input.len());
        assert_eq!(&decompressed[..decomp_len as usize], input);
    }

    #[test]
    fn test_compress_output_too_small() {
        let input = b"Hello, BifroMQ! This is a test.";
        let mut tiny_output = vec![0u8; 4];

        let result = gzip_compress(
            input.as_ptr(), input.len() as i32,
            tiny_output.as_mut_ptr(), tiny_output.len() as i32,
        );
        assert!(result < 0, "Should indicate buffer too small");
    }

    #[test]
    fn test_decompress_output_too_small() {
        let input = b"Hello, BifroMQ! This is a test message for GZIP compression via Rust flate2.";
        let mut compressed = vec![0u8; 1024];
        let comp_len = gzip_compress(
            input.as_ptr(), input.len() as i32,
            compressed.as_mut_ptr(), compressed.len() as i32,
        );
        assert!(comp_len > 0);

        let mut tiny_output = vec![0u8; 4];
        let result = gzip_decompress(
            compressed.as_ptr(), comp_len,
            tiny_output.as_mut_ptr(), tiny_output.len() as i32,
        );
        assert!(result < 0, "Should indicate buffer too small");
    }

    #[test]
    fn test_empty_input() {
        let input = b"";
        let mut output = vec![0u8; 256];

        let comp_len = gzip_compress(
            input.as_ptr(), 0,
            output.as_mut_ptr(), output.len() as i32,
        );
        assert!(comp_len > 0, "Empty compress should produce gzip header");

        let mut decompressed = vec![0u8; 256];
        let decomp_len = gzip_decompress(
            output.as_ptr(), comp_len,
            decompressed.as_mut_ptr(), decompressed.len() as i32,
        );
        assert_eq!(decomp_len, 0);
    }
}
