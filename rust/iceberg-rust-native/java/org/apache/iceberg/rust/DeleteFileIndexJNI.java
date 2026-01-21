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
import java.util.List;
import java.util.Map;

/**
 * JNI wrapper for Rust DeleteFileIndex implementation.
 *
 * <p>DeleteFileIndex provides an efficient index for looking up delete files
 * that apply to a given data file. It supports both position deletes and
 * equality deletes, with optimized binary search and statistics-based filtering.
 *
 * <p>Example usage:
 * <pre>{@code
 * // Build from delete files
 * String deleteFilesJson = "[{\"file_path\": \"delete1.parquet\", ...}]";
 * try (DeleteFileIndexJNI index = DeleteFileIndexJNI.build(deleteFilesJson, 0)) {
 *     if (!index.isEmpty()) {
 *         String dataFileJson = "{\"file_path\": \"data.parquet\", ...}";
 *         List<Map<String, Object>> deletes = index.forDataFile(dataFileJson);
 *     }
 * }
 * }</pre>
 */
public class DeleteFileIndexJNI implements Closeable {

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
                    "Failed to load Rust native library.", e);
            }
        }
    }

    private long handle;
    private boolean closed = false;

    private DeleteFileIndexJNI(long handle) {
        this.handle = handle;
    }

    /**
     * Build a DeleteFileIndex from JSON delete files.
     *
     * @param deleteFilesJson JSON array of delete file objects
     * @param minSequenceNumber Minimum sequence number filter (0 for no filter)
     * @return A new DeleteFileIndex
     * @throws IOException if building fails
     */
    public static DeleteFileIndexJNI build(String deleteFilesJson, long minSequenceNumber) throws IOException {
        byte[] bytes = deleteFilesJson.getBytes();
        long handle = nativeBuildWithFiles(bytes, minSequenceNumber);
        if (handle == 0) {
            throw new IOException("Failed to build DeleteFileIndex");
        }
        return new DeleteFileIndexJNI(handle);
    }

    /**
     * Build a DeleteFileIndex from a list of delete file maps.
     *
     * @param deleteFiles List of delete file data as maps
     * @param minSequenceNumber Minimum sequence number filter (0 for no filter)
     * @return A new DeleteFileIndex
     * @throws IOException if building fails
     */
    public static DeleteFileIndexJNI build(List<Map<String, Object>> deleteFiles, long minSequenceNumber)
            throws IOException {
        String json = MAPPER.writeValueAsString(deleteFiles);
        return build(json, minSequenceNumber);
    }

    /**
     * Check if the index is empty.
     *
     * @return true if no delete files are indexed
     */
    public boolean isEmpty() {
        checkNotClosed();
        return nativeIsEmpty(handle);
    }

    /**
     * Check if the index has equality deletes.
     *
     * @return true if there are equality delete files
     */
    public boolean hasEqualityDeletes() {
        checkNotClosed();
        return nativeHasEqualityDeletes(handle);
    }

    /**
     * Check if the index has position deletes.
     *
     * @return true if there are position delete files
     */
    public boolean hasPositionDeletes() {
        checkNotClosed();
        return nativeHasPositionDeletes(handle);
    }

    /**
     * Get the total number of delete files in the index.
     *
     * @return File count
     */
    public int getFileCount() {
        checkNotClosed();
        return nativeGetFileCount(handle);
    }

    /**
     * Find delete files that apply to the given data file.
     *
     * @param dataFileJson JSON representation of the data file
     * @return List of applicable delete files
     * @throws IOException if the lookup fails
     */
    public List<Map<String, Object>> forDataFile(String dataFileJson) throws IOException {
        checkNotClosed();
        byte[] result = nativeForDataFile(handle, dataFileJson.getBytes());
        if (result == null) {
            throw new IOException("Failed to find delete files");
        }
        return MAPPER.readValue(result, new TypeReference<List<Map<String, Object>>>() {});
    }

    /**
     * Find delete files that apply to the given data file.
     *
     * @param dataFile Data file as a map
     * @return List of applicable delete files
     * @throws IOException if the lookup fails
     */
    public List<Map<String, Object>> forDataFile(Map<String, Object> dataFile) throws IOException {
        String json = MAPPER.writeValueAsString(dataFile);
        return forDataFile(json);
    }

    /**
     * Get all delete files in the index.
     *
     * @return List of all delete files
     * @throws IOException if retrieval fails
     */
    public List<Map<String, Object>> getAllDeleteFiles() throws IOException {
        checkNotClosed();
        byte[] result = nativeGetAllDeleteFiles(handle);
        if (result == null) {
            throw new IOException("Failed to get delete files");
        }
        return MAPPER.readValue(result, new TypeReference<List<Map<String, Object>>>() {});
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
            throw new IllegalStateException("DeleteFileIndex is closed");
        }
    }

    // Native methods
    private static native long nativeBuildWithFiles(byte[] deleteFilesJson, long minSequenceNumber);
    private static native boolean nativeIsEmpty(long handle);
    private static native boolean nativeHasEqualityDeletes(long handle);
    private static native boolean nativeHasPositionDeletes(long handle);
    private static native int nativeGetFileCount(long handle);
    private static native byte[] nativeForDataFile(long handle, byte[] dataFileJson);
    private static native byte[] nativeGetAllDeleteFiles(long handle);
    private static native void nativeClose(long handle);
}
