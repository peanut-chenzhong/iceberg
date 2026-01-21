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
package org.apache.iceberg.rust;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * JNI wrapper for Rust ManifestGroup implementation.
 *
 * <p>ManifestGroup reads multiple manifest files in parallel using a thread pool,
 * achieving significant speedups for large tables with many manifests.
 *
 * <p>Example usage:
 * <pre>{@code
 * try (ManifestGroupJNI group = new ManifestGroupJNI(4)) {
 *     group.addManifest("manifest1.avro", 0);
 *     group.addManifest("manifest2.avro", 10000);
 *     List<Map<String, Object>> entries = group.readAllDataEntries();
 * }
 * }</pre>
 */
public class ManifestGroupJNI implements Closeable {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static boolean libraryLoaded = false;

    static {
        loadLibrary();
    }

    private static synchronized void loadLibrary() {
        if (!libraryLoaded) {
            try {
                System.loadLibrary("iceberg_rust_native");
                libraryLoaded = true;
            } catch (UnsatisfiedLinkError e) {
                throw new RuntimeException(
                    "Failed to load Rust native library. " +
                    "Make sure libiceberg_rust_native.so/dll is in java.library.path", e);
            }
        }
    }

    private long handle;
    private boolean closed = false;
    private final int parallelism;

    /**
     * Create a new ManifestGroup with the specified parallelism.
     *
     * @param parallelism Number of threads to use for parallel reading
     */
    public ManifestGroupJNI(int parallelism) {
        this.parallelism = parallelism;
        this.handle = nativeCreate(parallelism);
        if (this.handle == 0) {
            throw new RuntimeException("Failed to create ManifestGroup");
        }
    }

    /**
     * Create a new ManifestGroup with default parallelism (number of CPU cores).
     */
    public ManifestGroupJNI() {
        this(Runtime.getRuntime().availableProcessors());
    }

    /**
     * Add a manifest file to the group.
     *
     * @param path Path to the manifest file
     * @param firstRowId First row ID for row lineage assignment
     */
    public void addManifest(String path, long firstRowId) {
        checkNotClosed();
        nativeAddManifest(handle, path, firstRowId);
    }

    /**
     * Read all data entries from all manifests in parallel.
     *
     * @return List of all data file entries
     * @throws IOException if reading fails
     */
    public List<Map<String, Object>> readAllDataEntries() throws IOException {
        checkNotClosed();
        byte[] bytes = nativeReadAllDataEntries(handle, parallelism);
        if (bytes == null) {
            throw new IOException("Failed to read data entries");
        }
        return MAPPER.readValue(bytes, new TypeReference<List<Map<String, Object>>>() {});
    }

    @Override
    public void close() {
        if (!closed) {
            nativeClose(handle);
            closed = true;
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("ManifestGroup is closed");
        }
    }

    // Native methods
    private static native long nativeCreate(int parallelism);
    private static native void nativeAddManifest(long handle, String path, long firstRowId);
    private static native byte[] nativeReadAllDataEntries(long handle, int parallelism);
    private static native void nativeClose(long handle);
}
