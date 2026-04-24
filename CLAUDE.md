# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache BifroMQ is a high-performance, distributed MQTT broker with native multi-tenancy support. It is built as a Maven multi-module Java project (JDK 17+, this branch targets JDK 25 with Compact Object Headers) with Protobuf/gRPC for inter-service communication, Netty for I/O, RocksDB for local storage, and a custom Raft implementation for distributed state. This branch adds MQTT over QUIC support (Phase 1-3) via `netty-incubator-codec-native-quic`.

## Build System

The project uses Maven with the wrapper script `./mvnw`. Key build commands:

- **Compile and install locally (skip tests):**
  ```bash
  ./mvnw clean install -DskipTests
  ```

- **Run unit tests only:**
  ```bash
  ./mvnw test
  ```
  Uses `testsuites/UnitTests.xml`, which excludes TestNG tests annotated with `@Test(groups = "integration")`.

- **Run all tests with coverage (unit + integration):**
  ```bash
  ./mvnw test -Pbuild-coverage
  ```
  Uses `testsuites/CoverageTests.xml`, which runs everything. This takes significantly longer.

- **Build release artifacts:**
  ```bash
  ./mvnw -U clean verify -DskipTests -Pbuild-release
  ```
  Output archives are placed in `target/output/`.

- **Run tests for a single module and its dependencies:**
  ```bash
  ./mvnw -pl <module-name> -am test
  ```
  Example: `./mvnw -pl bifromq-dist/bifromq-dist-worker -am test`

- **Run a specific test class:**
  ```bash
  ./mvnw -pl <module> -am test -Dtest=<TestClassName>
  ```

- **Run benchmarks (JMH):**
  Benchmark classes live in `src/test/java` and can be executed via their module's test classpath. Look for classes with `*Benchmark.java`.

- **Run checkstyle only:**
  Checkstyle is bound to the `validate` phase by default, so it runs automatically on most goals. To run it explicitly:
  ```bash
  ./mvnw checkstyle:check
  ```

## Code Style and Conventions

- **Checkstyle** is enforced with `checkstyle.xml` at the project root. It is Google-inspired: 120-character line limit, no star imports, consistent indentation, and specific whitespace rules.
- **Lombok** is used pervasively (`@Slf4j`, `@Builder`, `@Getter`, `@Setter`, etc.). Config is in `lombok.config`.
- All source files must include the Apache 2.0 license header. The `apache-rat-plugin` enforces this during release builds.
- Test classes use **TestNG**, not JUnit. Integration tests are grouped with `@Test(groups = "integration")`.
- Retry logic is configured via `testsuites/src/main/java/org/apache/bifromq/test/RetryTransformer.java` and `RetryListener.java`.

## Architecture

### Module Categories

- **`base-*`**: Foundational libraries used by the broker services.
  - `base-cluster` — Membership and failure detection (UDP/TCP transport).
  - `base-crdt` — Conflict-free replicated data types (ORMap, CCounter) with anti-entropy gossip.
  - `base-hlc` — Hybrid logical clocks.
  - `base-kv` — Distributed KV store built on Raft. Key submodules:
    - `base-kv-raft` / `base-kv-raft-type` — Raft consensus implementation.
    - `base-kv-store-server` / `base-kv-store-client` — Store server and client.
    - `base-kv-local-engine-rocksdb` / `base-kv-local-engine-memory` — Storage backends.
    - `base-kv-store-coproc-api` — Coprocessor API for pushing compute into the KV store.
    - `base-kv-meta-service` — Metadata management atop the KV store.
  - `base-rpc` — gRPC-based RPC framework with traffic governing and in-process optimization.
  - `base-scheduler` — Batching and scheduling primitives.
- **`bifromq-*` (business services)**:
  - `bifromq-dist` — Pub/sub message distribution (topic matching, subscription routing).
  - `bifromq-inbox` — Per-tenant/client inbox/message queueing.
  - `bifromq-retain` — Retained message storage.
  - `bifromq-session-dict` — MQTT session registry.
  - `bifromq-deliverer` — Message delivery batching and pipelining.
  - `bifromq-mqtt` — MQTT protocol implementation (MQTT 3.1/3.1.1/5.0 over TCP/TLS/WS/WSS/QUIC) on Netty. QUIC support uses `netty-incubator-codec-native-quic` with multi-stream routing.
  - `bifromq-apiserver` — Administrative HTTP/gRPC API server.
- **`bifromq-plugin-*`**: Extension points for auth, client balancing, event collection, resource throttling, settings, and sub-broker delegation. Uses PF4J for plugin lifecycle.
- **`build/`**: Assembly modules and the standalone server starter (`StandaloneStarter`).
- **`Dockerfile.broker`**: Dedicated broker-only Docker image build file (multi-stage with local Maven cache support).
- **`testsuites/`**: Shared TestNG suite XMLs and retry utilities.

### Key Architectural Patterns

1. **Coprocessor Pattern**: The KV store (`base-kv`) supports coprocessors (defined in `base-kv-store-coproc-api`). Business logic like `bifromq-dist-worker` runs as a coprocessor inside the KV store process, reducing RPC hops.
2. **Guice Dependency Injection**: The standalone server (`StandaloneStarter` in `build/build-bifromq-starter`) wires services together via Guice modules (`build/build-bifromq-starter/src/main/java/org/apache/bifromq/starter/module/*`).
3. **Batch Scheduling**: RPC calls across services are heavily batched. Look for `Batch*Call`, `*Scheduler`, and `Batch*CallBuilderFactory` classes.
4. **Protobuf/GRPC**: Inter-service contracts are defined in `.proto` files. Generated code is produced by the `protobuf-maven-plugin` during the `compile` phase.
5. **Native Rust Acceleration**: `bifromq-native` contains Rust code (compiled via Maven) for performance-critical paths like topic matching, compression, and KV encoding. It is exposed via JNI through `bifromq-native-binding`. The build requires a Rust toolchain if building the native components.
6. **RocksDB Group Commit**: `base-kv-local-engine-rocksdb` includes `GroupCommitWriteQueue` to coalesce concurrent write batches into a single WAL sync, reducing write amplification under high load.
7. **MQTT over QUIC**: `bifromq-mqtt-server` supports QUIC transport via `netty-incubator-codec-native-quic`, with dedicated handlers (`QUICConnectionHandler`, `ControlStreamHandler`, `DataStreamHandler`, `QUICStreamRouter`).

## Important Files and Locations

- **`pom.xml`** — Root POM defining all modules, dependency versions, and profiles (`build`, `build-coverage`, `build-release`).
- **`checkstyle.xml`** — Checkstyle rules enforced on every build.
- **`testsuites/UnitTests.xml`** — TestNG suite for unit tests.
- **`testsuites/IntegrationTests.xml`** — TestNG suite for integration tests.
- **`testsuites/CoverageTests.xml`** — TestNG suite running all tests for coverage.
- **`build/build-bifromq-starter/src/main/java/org/apache/bifromq/starter/StandaloneStarter.java`** — Main entry point for the standalone server.
- **`bifromq-plugin/`** — All plugin interfaces and the plugin manager. If you are adding extensibility, start here.

## Testing Notes

- Unit tests should not spawn embedded clusters or open listening sockets unless necessary. Mark heavier tests with `@Test(groups = "integration")`.
- The project uses Mockito (`mockito-core`) and Awaitility for async assertions.
- JMH benchmarks exist in several modules under `src/test/java/**/benchmark/`. They are excluded from normal surefire execution (`**/benchmark/**` is excluded in the root POM surefire config).
- The `testsuites` module is added as an `additionalClasspathDependency` to surefire so its listeners are available to all modules.

## Tips for Productive Development

- When modifying Protobuf definitions, run `./mvnw clean compile -pl <module> -am` to regenerate sources.
- When working on a single module, use `-pl <module> -am` to avoid building the entire tree.
- If checkstyle fails during validation, fix style issues before running tests — it blocks the build early.
- The native Rust module (`bifromq-native`) may fail to compile if the Rust toolchain is missing. For pure Java development, you can usually skip it with `-DskipTests` or by excluding it from the reactor, though the starter depends on it at runtime.
- **Docker builds**: `Dockerfile.broker` supports multi-stage builds with local `m2-cache` to avoid downloading dependencies inside the container. Use `-Pbuild-release` to include linux x86_64 RocksDB native library.
- **JDK 25**: This branch is prepared for JDK 25 with Compact Object Headers. Ensure your local JDK matches if building natively.
