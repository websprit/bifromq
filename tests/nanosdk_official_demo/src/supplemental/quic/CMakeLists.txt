#
# This software is supplied under the terms of the MIT License, a
# copy of which should be located in the distribution where this
# file was obtained (LICENSE.txt).  A copy of the license may also be
# found online at https://opensource.org/licenses/MIT.
#

if (NNG_ENABLE_QUIC)
    set(NNG_QUIC_LIBS msquic none)

    # We assume msquic for now.
    set(NNG_QUIC_LIB msquic CACHE STRING "QUIC lib to use.")
    add_subdirectory(msquic)
    find_package(OpenSSL REQUIRED)

    nng_sources(quic_api.c)
    nng_sources(quic_api.h)

    set(INTERNAL_MSQUIC_INCLUDE_DIR ${MSQUIC_INCLUDE_DIR})

    if(NNG_PLATFORM_POSIX)
        nng_link_libraries(dl)
    elseif(NNG_PLATFORM_WINDOWS)
        nng_link_libraries(ws2_32)
    endif()

    if(TARGET msquic)
        add_dependencies(nng msquic)
    endif()

    nng_link_libraries(${MSQUIC_LIBRARIES})
    nng_link_libraries(OpenSSL::SSL OpenSSL::Crypto)

    nng_include_directories(${INTERNAL_MSQUIC_INCLUDE_DIR})
    
    # set_property(CACHE NNG_QUIC_LIB PROPERTY STRINGS ${NNG_TLS_ENGINES})
else ()
    set(NNG_QUIC_LIB none)
endif ()

# if (NOT NNG_QUIC_LIB STREQUAL "none")
#     nng_test(quic_test)
# endif ()