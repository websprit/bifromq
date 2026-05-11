#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

BINDING_RES="../bifromq-native-binding/src/main/resources/native"

HOST_OS="$(uname -s)"
HOST_ARCH="$(uname -m)"

echo "=== BifroMQ Native Build ==="
echo "Host: ${HOST_OS} ${HOST_ARCH}"

# Build for native platform
echo "--- Building for native platform ---"
cargo build --release

# Copy native platform artifact
if [[ "$HOST_OS" == "Darwin" ]]; then
    if [[ "$HOST_ARCH" == "arm64" ]]; then
        NATIVE_DIR="$BINDING_RES/osx-aarch_64"
    else
        NATIVE_DIR="$BINDING_RES/osx-x86_64"
    fi
    mkdir -p "$NATIVE_DIR"
    cp target/release/libbifromq_native.dylib "$NATIVE_DIR/"
    echo "Copied dylib to $NATIVE_DIR"
elif [[ "$HOST_OS" == "Linux" ]]; then
    if [[ "$HOST_ARCH" == "x86_64" ]]; then
        NATIVE_DIR="$BINDING_RES/linux-x86_64"
    else
        NATIVE_DIR="$BINDING_RES/linux-aarch_64"
    fi
    mkdir -p "$NATIVE_DIR"
    cp target/release/libbifromq_native.so "$NATIVE_DIR/"
    echo "Copied .so to $NATIVE_DIR"
fi

# Cross-compile for Linux x86_64 (if not already on that platform)
if [[ "$HOST_OS" != "Linux" || "$HOST_ARCH" != "x86_64" ]]; then
    echo "--- Cross-compiling for x86_64-unknown-linux-gnu ---"
    if command -v cross &> /dev/null; then
        cross build --release --target x86_64-unknown-linux-gnu
        CROSS_DIR="$BINDING_RES/linux-x86_64"
        mkdir -p "$CROSS_DIR"
        cp target/x86_64-unknown-linux-gnu/release/libbifromq_native.so "$CROSS_DIR/"
        echo "Copied cross-compiled .so to $CROSS_DIR"
    elif command -v docker &> /dev/null; then
        echo "Using Docker for Linux x86_64 cross-compilation..."
        docker run --rm \
            --platform linux/amd64 \
            -v "$(pwd):/workspace" \
            -w /workspace \
            rust:1.83-slim \
            bash -c "apt-get update && apt-get install -y --no-install-recommends cmake g++ make && cargo build --release"
        CROSS_DIR="$BINDING_RES/linux-x86_64"
        mkdir -p "$CROSS_DIR"
        cp target/release/libbifromq_native.so "$CROSS_DIR/"
        echo "Copied Docker-built .so to $CROSS_DIR"
    else
        echo "WARNING: Neither 'cross' nor 'docker' found."
        echo "Install Docker or cross: cargo install cross"
        echo "Skipping Linux x86_64 cross-compilation."
    fi
fi

echo "=== Build complete ==="
ls -la "$BINDING_RES"/*/ 2>/dev/null || echo "No artifacts found in $BINDING_RES"
