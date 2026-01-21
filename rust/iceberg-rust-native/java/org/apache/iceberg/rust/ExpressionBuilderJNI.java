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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Closeable;
import java.io.IOException;

/**
 * JNI wrapper for Rust Expression builder.
 *
 * <p>This class provides a Java interface to build and evaluate Iceberg expressions
 * using the high-performance Rust implementation.
 *
 * <p>Example usage:
 * <pre>{@code
 * try (SchemaJNI schema = new SchemaJNI()) {
 *     schema.addField(1, "id", "long", true);
 *     schema.addField(2, "name", "string", false);
 *     long schemaHandle = schema.build();
 *
 *     try (ExpressionBuilderJNI builder = new ExpressionBuilderJNI(schemaHandle)) {
 *         byte[] expr = builder.greaterThan("id", 100L);
 *         boolean result = EvaluatorJNI.evaluate(expr, partitionJson);
 *     }
 * }
 * }</pre>
 */
public class ExpressionBuilderJNI implements Closeable {

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

    /**
     * Create a new ExpressionBuilder with the given schema.
     *
     * @param schemaHandle Handle to a built Schema
     */
    public ExpressionBuilderJNI(long schemaHandle) {
        this.handle = nativeCreate(schemaHandle);
        if (this.handle == 0) {
            throw new RuntimeException("Failed to create ExpressionBuilder");
        }
    }

    /**
     * Create an equality expression: column = value
     */
    public byte[] equal(String column, Object value) throws IOException {
        checkNotClosed();
        byte[] valueJson = MAPPER.writeValueAsBytes(value);
        return nativeEqual(handle, column, valueJson);
    }

    /**
     * Create a not-equal expression: column != value
     */
    public byte[] notEqual(String column, Object value) throws IOException {
        checkNotClosed();
        byte[] valueJson = MAPPER.writeValueAsBytes(value);
        return nativeNotEqual(handle, column, valueJson);
    }

    /**
     * Create a less-than expression: column < value
     */
    public byte[] lessThan(String column, Object value) throws IOException {
        checkNotClosed();
        byte[] valueJson = MAPPER.writeValueAsBytes(value);
        return nativeLessThan(handle, column, valueJson);
    }

    /**
     * Create a less-than-or-equal expression: column <= value
     */
    public byte[] lessThanOrEqual(String column, Object value) throws IOException {
        checkNotClosed();
        byte[] valueJson = MAPPER.writeValueAsBytes(value);
        return nativeLessThanOrEqual(handle, column, valueJson);
    }

    /**
     * Create a greater-than expression: column > value
     */
    public byte[] greaterThan(String column, Object value) throws IOException {
        checkNotClosed();
        byte[] valueJson = MAPPER.writeValueAsBytes(value);
        return nativeGreaterThan(handle, column, valueJson);
    }

    /**
     * Create a greater-than-or-equal expression: column >= value
     */
    public byte[] greaterThanOrEqual(String column, Object value) throws IOException {
        checkNotClosed();
        byte[] valueJson = MAPPER.writeValueAsBytes(value);
        return nativeGreaterThanOrEqual(handle, column, valueJson);
    }

    /**
     * Create an is-null expression
     */
    public byte[] isNull(String column) {
        checkNotClosed();
        return nativeIsNull(handle, column);
    }

    /**
     * Create a not-null expression
     */
    public byte[] notNull(String column) {
        checkNotClosed();
        return nativeNotNull(handle, column);
    }

    /**
     * Create an AND expression
     */
    public byte[] and(byte[] left, byte[] right) {
        checkNotClosed();
        return nativeAnd(handle, left, right);
    }

    /**
     * Create an OR expression
     */
    public byte[] or(byte[] left, byte[] right) {
        checkNotClosed();
        return nativeOr(handle, left, right);
    }

    /**
     * Create a NOT expression
     */
    public byte[] not(byte[] child) {
        checkNotClosed();
        return nativeNot(handle, child);
    }

    /**
     * Create an always-true expression
     */
    public byte[] alwaysTrue() {
        checkNotClosed();
        return nativeAlwaysTrue(handle);
    }

    /**
     * Create an always-false expression
     */
    public byte[] alwaysFalse() {
        checkNotClosed();
        return nativeAlwaysFalse(handle);
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
            throw new IllegalStateException("ExpressionBuilder is closed");
        }
    }

    // Native methods
    private static native long nativeCreate(long schemaHandle);
    private static native byte[] nativeEqual(long handle, String column, byte[] valueJson);
    private static native byte[] nativeNotEqual(long handle, String column, byte[] valueJson);
    private static native byte[] nativeLessThan(long handle, String column, byte[] valueJson);
    private static native byte[] nativeLessThanOrEqual(long handle, String column, byte[] valueJson);
    private static native byte[] nativeGreaterThan(long handle, String column, byte[] valueJson);
    private static native byte[] nativeGreaterThanOrEqual(long handle, String column, byte[] valueJson);
    private static native byte[] nativeIsNull(long handle, String column);
    private static native byte[] nativeNotNull(long handle, String column);
    private static native byte[] nativeAnd(long handle, byte[] left, byte[] right);
    private static native byte[] nativeOr(long handle, byte[] left, byte[] right);
    private static native byte[] nativeNot(long handle, byte[] child);
    private static native byte[] nativeAlwaysTrue(long handle);
    private static native byte[] nativeAlwaysFalse(long handle);
    private static native void nativeClose(long handle);
}
