/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bifromq.native_binding;

import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.SymbolLookup;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.sysprops.props.NativeEngineEnabled;

/**
 * Loads the native Rust library and provides access to its symbols via Java FFM.
 * <p>
 * Platform detection logic:
 * <ul>
 *   <li>Linux x86_64 → {@code native/linux-x86_64/libbifromq_native.so}</li>
 *   <li>macOS aarch64 → {@code native/osx-aarch_64/libbifromq_native.dylib}</li>
 *   <li>macOS x86_64 → {@code native/osx-x86_64/libbifromq_native.dylib}</li>
 * </ul>
 * <p>
 * The library is extracted from the classpath to a temp file and loaded via
 * {@link SymbolLookup#libraryLookup(Path, Arena)}.
 */
@Slf4j
public final class NativeLoader {
    private static final boolean AVAILABLE;
    private static final SymbolLookup SYMBOLS;

    static {
        boolean loaded = false;
        SymbolLookup symbols = null;

        if (NativeEngineEnabled.INSTANCE.get()) {
            try {
                String resourcePath = detectLibraryResourcePath();
                Path tempLib = extractLibrary(resourcePath);
                symbols = SymbolLookup.libraryLookup(tempLib, Arena.global());
                loaded = true;
                log.info("Native engine loaded successfully from: {}", resourcePath);
            } catch (Throwable t) {
                log.warn("Native engine unavailable, using Java fallback: {}", t.getMessage());
                log.debug("Native engine load failure details:", t);
            }
        } else {
            log.info("Native engine disabled via configuration (native_engine_enabled=false)");
        }

        AVAILABLE = loaded;
        SYMBOLS = symbols;
    }

    /**
     * Returns true if the native library is loaded and available.
     */
    public static boolean isAvailable() {
        return AVAILABLE;
    }

    /**
     * Returns the symbol lookup for the loaded native library.
     *
     * @throws IllegalStateException if the native library is not available
     */
    public static SymbolLookup symbols() {
        if (!AVAILABLE) {
            throw new IllegalStateException("Native library not available");
        }
        return SYMBOLS;
    }

    /**
     * Detect the correct library resource path based on OS and architecture.
     */
    static String detectLibraryResourcePath() {
        String os = System.getProperty("os.name", "").toLowerCase();
        String arch = System.getProperty("os.arch", "").toLowerCase();

        String osDir;
        String libName;

        if (os.contains("linux")) {
            libName = "libbifromq_native.so";
            if (arch.equals("amd64") || arch.equals("x86_64")) {
                osDir = "linux-x86_64";
            } else if (arch.equals("aarch64")) {
                osDir = "linux-aarch_64";
            } else {
                throw new UnsupportedOperationException("Unsupported Linux architecture: " + arch);
            }
        } else if (os.contains("mac") || os.contains("darwin")) {
            libName = "libbifromq_native.dylib";
            if (arch.equals("aarch64") || arch.equals("arm64")) {
                osDir = "osx-aarch_64";
            } else if (arch.equals("amd64") || arch.equals("x86_64")) {
                osDir = "osx-x86_64";
            } else {
                throw new UnsupportedOperationException("Unsupported macOS architecture: " + arch);
            }
        } else {
            throw new UnsupportedOperationException("Unsupported OS: " + os);
        }

        return "native/" + osDir + "/" + libName;
    }

    /**
     * Extract the library from classpath resources to a temp file.
     */
    private static Path extractLibrary(String resourcePath) throws IOException {
        try (InputStream is = NativeLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IOException("Native library resource not found: " + resourcePath);
            }
            String suffix = resourcePath.endsWith(".dylib") ? ".dylib" : ".so";
            Path tempFile = Files.createTempFile("bifromq_native_", suffix);
            tempFile.toFile().deleteOnExit();
            Files.copy(is, tempFile, StandardCopyOption.REPLACE_EXISTING);
            return tempFile;
        }
    }

    private NativeLoader() {
        // utility class
    }
}
