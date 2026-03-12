FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    git cmake ninja-build gcc g++ pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Clone NanoSDK
RUN cd /opt && \
    git clone https://github.com/nanomq/NanoSDK && \
    cd NanoSDK && \
    git submodule update --init --recursive

# Configure
RUN cd /opt/NanoSDK && \
    mkdir build && cd build && \
    cmake -G Ninja -DBUILD_SHARED_LIBS=OFF -DNNG_ENABLE_QUIC=ON ..

# Build NanoSDK
RUN cd /opt/NanoSDK/build && \
    ninja 

# Copy test client source code
COPY quic_producer.c /opt/test/quic_producer.c
COPY quic_consumer.c /opt/test/quic_consumer.c
COPY CMakeLists.txt /opt/test/CMakeLists.txt
COPY ca.crt /opt/certs/ca.crt

# Build test clients
RUN cd /opt/test && \
    mkdir build && cd build && \
    cmake -G Ninja .. && \
    ninja

# Both executables will be in /opt/test/build/
ENTRYPOINT ["/bin/bash"]

