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

import java.io.Closeable;

/**
 * JNI wrapper for Rust Schema.
 *
 * <p>This class provides a Java interface to build Iceberg schemas
 * for expression binding.
 *
 * <p>Example usage:
 * <pre>{@code
 * try (SchemaJNI schema = new SchemaJNI()) {
 *     schema.addField(1, "id", "long", true);
 *     schema.addField(2, "name", "string", false);
 *     schema.addField(3, "age", "int", false);
 *     long schemaHandle = schema.build();
 *     // Use schemaHandle with ExpressionBuilderJNI
 * }
 * }</pre>
 */
public class SchemaJNI implements Closeable {

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

    private long builderHandle;
    private long schemaHandle;
    private boolean closed = false;
    private boolean built = false;

    /**
     * Create a new Schema builder.
     */
    public SchemaJNI() {
        this.builderHandle = nativeCreate();
        if (this.builderHandle == 0) {
            throw new RuntimeException("Failed to create Schema builder");
        }
    }

    /**
     * Add a field to the schema.
     *
     * @param id Field ID
     * @param name Field name
     * @param type Field type (e.g., "int", "long", "string", "boolean", "float", "double")
     * @param required Whether the field is required
     */
    public void addField(int id, String name, String type, boolean required) {
        checkNotClosed();
        checkNotBuilt();
        nativeAddField(builderHandle, id, name, type, required);
    }

    /**
     * Build the schema and return its handle.
     *
     * <p>After calling this method, no more fields can be added.
     * The returned handle should be used with ExpressionBuilderJNI.
     *
     * @return Handle to the built schema
     */
    public long build() {
        checkNotClosed();
        checkNotBuilt();
        this.schemaHandle = nativeBuild(builderHandle);
        this.built = true;
        this.builderHandle = 0; // Builder is consumed
        return this.schemaHandle;
    }

    /**
     * Get the schema handle if already built.
     *
     * @return Handle to the built schema
     */
    public long getHandle() {
        if (!built) {
            throw new IllegalStateException("Schema not built yet");
        }
        return this.schemaHandle;
    }

    @Override
    public void close() {
        if (!closed) {
            if (!built && builderHandle != 0) {
                // Builder was not consumed, close it
                // Note: This would need a separate native method
            }
            if (schemaHandle != 0) {
                nativeClose(schemaHandle);
            }
            closed = true;
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Schema is closed");
        }
    }

    private void checkNotBuilt() {
        if (built) {
            throw new IllegalStateException("Schema already built");
        }
    }

    // Native methods
    private static native long nativeCreate();
    private static native void nativeAddField(long handle, int id, String name, String type, boolean required);
    private static native long nativeBuild(long handle);
    private static native void nativeClose(long handle);
}
