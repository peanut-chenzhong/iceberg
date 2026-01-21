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
import java.io.IOException;
import java.util.Map;

/**
 * JNI wrapper for Rust Expression Evaluator.
 *
 * <p>This class provides static methods to evaluate Iceberg expressions
 * against partition data using the high-performance Rust implementation.
 *
 * <p>Example usage:
 * <pre>{@code
 * // Build an expression
 * byte[] expression = builder.greaterThan("id", 100L);
 *
 * // Evaluate against partition data
 * String partitionJson = "{\"values\": [123]}";
 * boolean matches = EvaluatorJNI.evaluate(expression, partitionJson);
 * }</pre>
 */
public class EvaluatorJNI {

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

    // Private constructor - utility class
    private EvaluatorJNI() {}

    /**
     * Evaluate an expression against partition data.
     *
     * @param expressionJson JSON bytes of the bound expression
     * @param partitionJson JSON bytes of the partition data
     * @return true if the expression evaluates to true
     */
    public static boolean evaluate(byte[] expressionJson, byte[] partitionJson) {
        return nativeEvaluate(expressionJson, partitionJson);
    }

    /**
     * Evaluate an expression against partition data.
     *
     * @param expressionJson JSON bytes of the bound expression
     * @param partitionJson JSON string of the partition data
     * @return true if the expression evaluates to true
     */
    public static boolean evaluate(byte[] expressionJson, String partitionJson) {
        return evaluate(expressionJson, partitionJson.getBytes());
    }

    /**
     * Evaluate an expression against partition data.
     *
     * @param expressionJson JSON bytes of the bound expression
     * @param partition Partition data as a map
     * @return true if the expression evaluates to true
     * @throws IOException if serialization fails
     */
    public static boolean evaluate(byte[] expressionJson, Map<String, Object> partition)
            throws IOException {
        byte[] partitionJson = MAPPER.writeValueAsBytes(partition);
        return evaluate(expressionJson, partitionJson);
    }

    // Native method
    private static native boolean nativeEvaluate(byte[] expressionJson, byte[] partitionJson);
}
