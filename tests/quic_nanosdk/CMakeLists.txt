cmake_minimum_required(VERSION 3.13)
project(quic_nanosdk_test C)

set(CMAKE_C_STANDARD 99)

# NanoSDK directory (passed via -DNANOSDK_DIR=...)
if(NOT DEFINED NANOSDK_DIR)
    set(NANOSDK_DIR "/opt/NanoSDK")
endif()

# Include paths
include_directories(
    ${NANOSDK_DIR}/include
    ${NANOSDK_DIR}/nng/include
)

# Find the NanoSDK static library
set(NNG_LIB ${NANOSDK_DIR}/build/libnng.a)

# Dependency libraries built by NanoSDK
set(MSQUIC_LIB ${NANOSDK_DIR}/build/src/supplemental/quic/msquic/msquic/obj/Release/libcore.a)
set(MSQUIC_PLATFORM_LIB ${NANOSDK_DIR}/build/src/supplemental/quic/msquic/msquic/obj/Release/libplatform.a)
# Note msquic generates libcore.a for msquic itself, and OpenSSL libraries 
set(OPENSSL_LIB ${NANOSDK_DIR}/build/_deps/opensslquic-build/openssl/lib/libssl.a)
set(CRYPTO_LIB ${NANOSDK_DIR}/build/_deps/opensslquic-build/openssl/lib/libcrypto.a)

message(STATUS "NNG library: ${NNG_LIB}")
message(STATUS "MSQUIC library: ${MSQUIC_LIB}")
message(STATUS "MSQUIC PLATFORM library: ${MSQUIC_PLATFORM_LIB}")

find_package(Threads REQUIRED)

# Producer
add_executable(quic_producer quic_producer.c)
target_link_libraries(quic_producer
    ${NNG_LIB}
    ${MSQUIC_LIB}
    ${MSQUIC_PLATFORM_LIB}
    ${OPENSSL_LIB}
    ${CRYPTO_LIB}
    Threads::Threads
    dl
    m
)

# Consumer
add_executable(quic_consumer quic_consumer.c)
target_link_libraries(quic_consumer
    ${NNG_LIB}
    ${MSQUIC_LIB}
    ${MSQUIC_PLATFORM_LIB}
    ${OPENSSL_LIB}
    ${CRYPTO_LIB}
    Threads::Threads
    dl
    m
)

