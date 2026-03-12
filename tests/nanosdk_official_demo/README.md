# NanoSDK Official Demo Notes

这个目录保存的是联调用过的 NanoSDK 官方 demo 修改版，以及为了让它在本机环境中编译通过而改过的两处 QUIC CMake 文件。

它不是一个完整的 NanoSDK 仓库副本，只包含需要覆盖到官方 NanoSDK checkout 中的补丁文件。

## 目录说明

- `demo/quic_mqttv5/quic_client_v5.c`
  - 本机临时改过的官方 QUIC MQTT v5 demo。
  - 保留了 `conn` / `sub` / `pub` 三个子命令。
  - `sub` 和 `pub` 会在连接回调触发后再发 MQTT 报文，避免 CONNECT 尚未完成时抢跑。

- `src/supplemental/quic/CMakeLists.txt`
  - 调整 NanoSDK QUIC 构建逻辑，显式链接 `OpenSSL::SSL` / `OpenSSL::Crypto`，并接入 msquic。

- `src/supplemental/quic/msquic/CMakeLists.txt`
  - 调整 msquic 的查找方式，优先使用系统安装的 msquic。

## 准备依赖

### macOS

建议先安装这些依赖：

```bash
brew install cmake ninja pkg-config openssl@3 msquic
```

如果 `msquic` 没有装在默认路径，后面的 CMake 命令里需要显式传 `MSQUIC_ROOT_DIR`。

### Linux

至少需要：

```bash
cmake
ninja
pkg-config
OpenSSL 开发包
msquic
```

如果 `msquic` 安装在自定义位置，也需要传 `MSQUIC_ROOT_DIR`。

## 获取官方 NanoSDK

```bash
git clone https://github.com/nanomq/NanoSDK.git
cd NanoSDK
git submodule update --init --recursive
```

## 覆盖补丁文件

假设当前仓库路径为 `BIFROMQ_REPO=/path/to/bifromq`，NanoSDK checkout 在 `NANOSDK_DIR=/path/to/NanoSDK`，可以直接覆盖：

```bash
cp "$BIFROMQ_REPO/tests/nanosdk_official_demo/demo/quic_mqttv5/quic_client_v5.c" \
   "$NANOSDK_DIR/demo/quic_mqttv5/quic_client_v5.c"

cp "$BIFROMQ_REPO/tests/nanosdk_official_demo/src/supplemental/quic/CMakeLists.txt" \
   "$NANOSDK_DIR/src/supplemental/quic/CMakeLists.txt"

cp "$BIFROMQ_REPO/tests/nanosdk_official_demo/src/supplemental/quic/msquic/CMakeLists.txt" \
   "$NANOSDK_DIR/src/supplemental/quic/msquic/CMakeLists.txt"
```

## 编译 NanoSDK demo

### macOS 示例

```bash
cd /path/to/NanoSDK
rm -rf build
cmake -S . -B build -G Ninja \
  -DNNG_ENABLE_QUIC=ON \
  -DMSQUIC_ROOT_DIR="$(brew --prefix msquic)" \
  -DOPENSSL_ROOT_DIR="$(brew --prefix openssl@3)"

cmake --build build -j
```

如果你的 msquic 或 openssl 不在 Homebrew 默认路径，把上面的目录换成实际路径即可。

### Linux 示例

```bash
cd /path/to/NanoSDK
rm -rf build
cmake -S . -B build -G Ninja \
  -DNNG_ENABLE_QUIC=ON \
  -DMSQUIC_ROOT_DIR=/usr/local

cmake --build build -j
```

## 使用方式

demo 二进制一般会出现在：

```bash
build/demo/quic_mqttv5/quic_client_v5
```

### 连接测试

```bash
./build/demo/quic_mqttv5/quic_client_v5 conn 'mqtt-quic://127.0.0.1:14567'
```

### 订阅

```bash
./build/demo/quic_mqttv5/quic_client_v5 sub 'mqtt-quic://127.0.0.1:14567' 0 bifromq/test
```

### 发布

```bash
./build/demo/quic_mqttv5/quic_client_v5 pub 'mqtt-quic://127.0.0.1:14567' 0 bifromq/test hello
```

## 行为说明

- `sub` / `pub` 命令不会在 `main` 里立刻发 MQTT 报文，而是先发 CONNECT。
- CONNECT 完成后，由 `connect_cb` 回调继续发送 SUBSCRIBE 或 PUBLISH。
- 这能避免 QUIC 连接已建立但 MQTT 会话尚未完成时，业务报文过早发送导致的联调问题。

## 已验证的本地场景

这个 demo 在本机联调里主要用于连接本地 BifroMQ QUIC 监听器：

```bash
mqtt-quic://127.0.0.1:14567
```

配合 BifroMQ 当前分支里的 QUIC 修复，可以完成基本的 pub/sub 验证。

## 注意事项

- 这个目录只保存“改过的文件”，不会自动帮你拉取或构建 NanoSDK。
- 如果你重新切换 NanoSDK 分支或重新 clone，需要重新覆盖这 3 个文件。
- 如果系统里没有可用的 `msquic` 和 OpenSSL 开发环境，CMake 会失败。