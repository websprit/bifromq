#!/bin/bash
#
# End-to-end test: NanoSDK QUIC Producer → BifroMQ Broker → NanoSDK QUIC Consumer
#
# Prerequisites:
#   - Docker installed and running
#   - BifroMQ running with QUIC listener on port 14567
#
# Usage:
#   ./run_test.sh [broker_host] [broker_port] [multi_stream]
#
# Default:
#   ./run_test.sh host.docker.internal 14567 0
#

set -e

BROKER_HOST="${1:-host.docker.internal}"
BROKER_PORT="${2:-14567}"
MULTI_STREAM="${3:-0}"
BROKER_URL="mqtt-quic://${BROKER_HOST}:${BROKER_PORT}"

IMAGE_NAME="nanosdk-quic-test"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  NanoSDK MQTT over QUIC End-to-End Test                     ║"
echo "║  Broker: ${BROKER_URL}"
echo "║  Multi-stream: ${MULTI_STREAM}"
echo "╚══════════════════════════════════════════════════════════════╝"

# Step 1: Build Docker image
echo ""
echo "=== Step 1: Building Docker image (this may take several minutes) ==="
docker build -t "${IMAGE_NAME}" "${SCRIPT_DIR}"

# Step 2: Start consumer in background
echo ""
echo "=== Step 2: Starting consumer ==="
CONSUMER_ID=$(docker run -d --rm \
    --name quic-consumer \
    --add-host=host.docker.internal:host-gateway \
    "${IMAGE_NAME}" \
    -c "/opt/test/build/quic_consumer '${BROKER_URL}' ${MULTI_STREAM}")

echo "Consumer container: ${CONSUMER_ID:0:12}"

# Wait for consumer to connect and subscribe
sleep 3

# Step 3: Run producer
echo ""
echo "=== Step 3: Running producer ==="
docker run --rm \
    --name quic-producer \
    --add-host=host.docker.internal:host-gateway \
    "${IMAGE_NAME}" \
    -c "/opt/test/build/quic_producer '${BROKER_URL}' ${MULTI_STREAM}"

# Step 4: Wait and collect consumer output
echo ""
echo "=== Step 4: Collecting consumer results ==="
sleep 5
docker logs quic-consumer 2>&1

# Cleanup
echo ""
echo "=== Cleanup ==="
docker stop quic-consumer 2>/dev/null || true
echo "Done."
