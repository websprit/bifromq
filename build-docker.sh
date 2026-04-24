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

IMAGE_TAG="apache/bifromq:4.0.0-incubating"
PUSH=false
LOAD=true
PLATFORMS="linux/amd64"
MAVEN_OPTS=""
NO_CACHE=""

usage() {
    cat <<EOF
Usage: $0 [options]

Build a BifroMQ Docker image for linux/amd64 from any host platform
(macOS ARM64, Linux x86_64, etc.) using Docker buildx.

This script compiles:
  - Rust native code (bifromq-native) → libbifromq_native.so
  - RocksDB JNI (with our write_batch.cc modifications) → librocksdbjni-linux64.so
  - Java code via Maven → apache-bifromq-4.0.0-incubating.tar.gz
  - Final runtime image → eclipse-temurin:21-jre

Options:
  -t, --tag <tag>       Docker image tag (default: $IMAGE_TAG)
      --push             Push image to registry after build
      --load             Load image to local docker daemon (default)
      --platform <plat>  Target platform(s) (default: $PLATFORMS)
      --no-cache         Disable Docker build cache
      --maven-opts       Extra Maven options (e.g. "-Pbuild-release")
  -h, --help           Show this help

Examples:
  # Build and load to local docker
  $0

  # Build and push to registry
  $0 --tag registry.example.com/bifromq:v1 --push

  # Build with release profile and no cache
  $0 --no-cache --maven-opts "-Pbuild-release"
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--tag) IMAGE_TAG="$2"; shift 2 ;;
        --push) PUSH=true; LOAD=false; shift ;;
        --load) LOAD=true; PUSH=false; shift ;;
        --platform) PLATFORMS="$2"; shift 2 ;;
        --no-cache) NO_CACHE="--no-cache"; shift ;;
        --maven-opts) MAVEN_OPTS="$2"; shift 2 ;;
        -h|--help) usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

# Validate docker buildx
if ! docker buildx inspect >/dev/null 2>&1; then
    echo "Creating docker buildx builder..."
    docker buildx create --use --name bifromq-builder --driver docker-container
fi

# Validate prerequisites
if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: docker is not installed"
    exit 1
fi

# Ensure config files exist (Dockerfile requires them)
if [[ ! -f conf/standalone.yml ]]; then
    echo "WARNING: conf/standalone.yml not found. Creating minimal config..."
    mkdir -p conf
    cat > conf/standalone.yml <<'EOF'
# Minimal standalone config for Docker build
# Replace with your production configuration before deploying
EOF
fi

if [[ ! -f conf/certs/server.crt ]]; then
    echo "WARNING: conf/certs/ not found. Creating self-signed dummy certs..."
    mkdir -p conf/certs
    openssl req -x509 -newkey rsa:2048 \
        -keyout conf/certs/server.key \
        -out conf/certs/server.crt \
        -days 365 -nodes \
        -subj "/CN=bifromq" 2>/dev/null || {
        echo "WARNING: openssl not available, creating empty cert files"
        touch conf/certs/server.crt conf/certs/server.key
    }
fi

# Ensure m2-cache directory exists (empty is fine) so Dockerfile COPY succeeds
if [[ ! -d m2-cache ]]; then
    mkdir -p m2-cache
    echo "Maven cache: created empty m2-cache/ (online download)"
else
    echo "Maven cache: using existing m2-cache/"
fi

BUILD_ARGS=()
if [[ "$LOAD" == "true" ]]; then
    BUILD_ARGS+=("--load")
fi
if [[ "$PUSH" == "true" ]]; then
    BUILD_ARGS+=("--push")
fi

# Print build info
cat <<EOF

=== BifroMQ Cross-Platform Docker Build ===
Tag:       $IMAGE_TAG
Platform:  $PLATFORMS
Push:      $PUSH
Load:      $LOAD
Cache:     $([[ -n "$NO_CACHE" ]] && echo disabled || echo enabled)
Maven:     $MAVEN_OPTS

This will take 10-30 minutes (RocksDB C++ compilation is slow).
EOF

# Build
docker buildx build \
    --platform "$PLATFORMS" \
    --tag "$IMAGE_TAG" \
    --file Dockerfile.build \
    ${NO_CACHE} \
    --build-arg "MAVEN_OPTS=${MAVEN_OPTS}" \
    "${BUILD_ARGS[@]}" \
    "$SCRIPT_DIR"

echo ""
echo "=== Build complete ==="
if [[ "$LOAD" == "true" ]]; then
    echo "Image loaded locally: $IMAGE_TAG"
    docker images --filter reference="$IMAGE_TAG" --format "table {{.Repository}}:{{.Tag}}\t{{.Size}}\t{{.CreatedAt}}"
fi
if [[ "$PUSH" == "true" ]]; then
    echo "Image pushed: $IMAGE_TAG"
fi
