<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# FlowSDK RocksDB Stress

This is a standalone FlowSDK-based stress harness for a packaged BifroMQ Docker image.
It targets the RocksDB-backed local engine paths that are not practical to validate with
local rocksdbjni-only unit tests.

The harness starts a BifroMQ container, waits for MQTT and KV range bootstrap, then:

1. Creates MQTT subscribers and subscriptions.
2. Publishes QoS1 retained messages through concurrent FlowSDK publishers.
3. Re-subscribes and verifies retained messages can be read back.

The workload exercises the packaged RocksDB JNI runtime, retain store writes, dist subscription
writes, WAL/data directories, and group commit paths used by RocksDB-backed KV stores.

## Run TCP

```bash
BIFROMQ_DOCKER_CMD='rdctl shell docker' \
BIFROMQ_STRESS_CLIENTS=80 \
BIFROMQ_STRESS_PUBLISHERS=8 \
BIFROMQ_STRESS_MESSAGES_PER_CLIENT=60 \
BIFROMQ_STRESS_PAYLOAD_BYTES=512 \
cargo run --release --manifest-path tests/flowsdk-rocksdb-stress/Cargo.toml
```

Use plain Docker by omitting `BIFROMQ_DOCKER_CMD` when the local Docker socket is available.

## Run QUIC

The QUIC listener uses TLS and ALPN `mqtt`. The harness configures FlowSDK to skip certificate
verification because the packaged image uses a self-signed test certificate.

QUIC defaults to MQTT 3.1.1 for the retained-write stress path. `BIFROMQ_STRESS_MQTT_VERSION=5`
currently reproduces a FlowSDK/BifroMQ interoperability stall after roughly 200 QoS1 publishes per
QUIC publisher, while the same workload passes over TCP with MQTT 5 and over QUIC with MQTT 3.1.1.

On Docker Desktop or native Linux Docker, this can usually run through the published UDP port:

```bash
BIFROMQ_DOCKER_CMD='rdctl shell docker' \
BIFROMQ_TRANSPORT=quic \
BIFROMQ_STRESS_CLIENTS=20 \
BIFROMQ_STRESS_PUBLISHERS=4 \
BIFROMQ_STRESS_MESSAGES_PER_CLIENT=20 \
cargo run --release --manifest-path tests/flowsdk-rocksdb-stress/Cargo.toml
```

If QUIC times out on macOS/Rancher Desktop, run the FlowSDK client in a Linux container that shares
the broker container network namespace. This bypasses host UDP port forwarding and connects to the
broker's container-local UDP 1884 directly:

```bash
BIFROMQ_DOCKER_CMD='rdctl shell docker' \
BIFROMQ_TRANSPORT=quic \
BIFROMQ_STRESS_CLIENTS=20 \
BIFROMQ_STRESS_PUBLISHERS=4 \
BIFROMQ_STRESS_MESSAGES_PER_CLIENT=20 \
cargo run --release --manifest-path tests/flowsdk-rocksdb-stress/Cargo.toml

rdctl shell docker run --rm --network container:bifromq-flowsdk-rocksdb-stress \
  -v "$PWD:/work" -w /work \
  -e CARGO_TARGET_DIR=/tmp/flowsdk-target \
  -e BIFROMQ_START_CONTAINER=false \
  -e BIFROMQ_TRANSPORT=quic \
  -e BIFROMQ_PEER=quic://127.0.0.1:1884 \
  -e BIFROMQ_STRESS_CLIENTS=20 \
  -e BIFROMQ_STRESS_PUBLISHERS=4 \
  -e BIFROMQ_STRESS_MESSAGES_PER_CLIENT=20 \
  rust:1.95-bookworm \
  cargo run --release --manifest-path tests/flowsdk-rocksdb-stress/Cargo.toml
```

## Useful Options

- `BIFROMQ_IMAGE`: image to test, defaults to `ghcr.io/websprit/bifromq:feature-perf-optimization`.
- `BIFROMQ_CONTAINER`: temporary container name, defaults to `bifromq-flowsdk-rocksdb-stress`.
- `BIFROMQ_PEER`: MQTT endpoint, defaults to `localhost:11883`.
- `BIFROMQ_TRANSPORT`: `tcp` or `quic`, defaults to `tcp`.
- `BIFROMQ_START_CONTAINER`: set to `false` to test an already-running broker.
- `BIFROMQ_STRESS_READY_GRACE_SECS`: extra wait after MQTT accepts connections so KV ranges can finish bootstrap.
- `BIFROMQ_STRESS_OP_TIMEOUT_MS`: per-operation timeout, defaults to `60000`.
- `BIFROMQ_STRESS_MQTT_VERSION`: MQTT version. Defaults to `5` for TCP and `3` for QUIC.
