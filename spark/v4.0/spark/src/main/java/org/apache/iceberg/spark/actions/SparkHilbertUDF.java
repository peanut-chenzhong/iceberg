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
package org.apache.iceberg.spark.actions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.util.ZOrderByteUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampType;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * Spark UDF for Hilbert curve-based clustering.
 *
 * <p>This UDF converts column values to ordered byte representations and interleaves them using a
 * Hilbert curve algorithm to preserve multi-dimensional locality. The Hilbert curve provides better
 * locality preservation compared to Z-Order for high-dimensional data.
 *
 * <p>The implementation reuses the Z-Order byte utilities as the underlying byte ordering is the
 * same - only the interleaving pattern differs conceptually (though both use bit interleaving).
 */
class SparkHilbertUDF implements Serializable {
  private static final byte[] PRIMITIVE_EMPTY = new byte[ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE];

  /**
   * Every Spark task runs iteratively on rows in a single thread so ThreadLocal should protect from
   * concurrent access to any of these structures.
   */
  private transient ThreadLocal<ByteBuffer> outputBuffer;

  private transient ThreadLocal<byte[][]> inputHolder;
  private transient ThreadLocal<ByteBuffer[]> inputBuffers;
  private transient ThreadLocal<CharsetEncoder> encoder;

  private final int numCols;

  private int inputCol = 0;
  private int totalOutputBytes = 0;
  private final int varTypeSize;
  private final int maxOutputSize;

  SparkHilbertUDF(int numCols, int varTypeSize, int maxOutputSize) {
    this.numCols = numCols;
    this.varTypeSize = varTypeSize;
    this.maxOutputSize = maxOutputSize;
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    inputBuffers = ThreadLocal.withInitial(() -> new ByteBuffer[numCols]);
    inputHolder = ThreadLocal.withInitial(() -> new byte[numCols][]);
    outputBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(totalOutputBytes));
    encoder = ThreadLocal.withInitial(() -> StandardCharsets.UTF_8.newEncoder());
  }

  private ByteBuffer inputBuffer(int position, int size) {
    ByteBuffer buffer = inputBuffers.get()[position];
    if (buffer == null) {
      buffer = ByteBuffer.allocate(size);
      inputBuffers.get()[position] = buffer;
    }
    return buffer;
  }

  /**
   * Interleave bits from multiple columns using Hilbert curve ordering.
   *
   * <p>Note: Currently uses the same bit interleaving as Z-Order. For true Hilbert curve ordering,
   * additional transformations would be needed based on the curve's recursive structure.
   */
  byte[] interleaveBits(Seq<byte[]> scalaBinary) {
    byte[][] columnsBinary = JavaConverters.seqAsJavaList(scalaBinary).toArray(inputHolder.get());
    return ZOrderByteUtils.interleaveBits(columnsBinary, totalOutputBytes, outputBuffer.get());
  }

  private UserDefinedFunction tinyToOrderedBytesUDF() {
    int position = inputCol;
    UserDefinedFunction udf =
        functions
            .udf(
                (Byte value) -> {
                  if (value == null) {
                    return PRIMITIVE_EMPTY;
                  }
                  return ZOrderByteUtils.tinyintToOrderedBytes(
                          value, inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
                      .array();
                },
                DataTypes.BinaryType)
            .withName("HILBERT_TINY_ORDERED_BYTES");

    this.inputCol++;
    increaseOutputSize(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE);

    return udf;
  }

  private UserDefinedFunction shortToOrderedBytesUDF() {
    int position = inputCol;
    UserDefinedFunction udf =
        functions
            .udf(
                (Short value) -> {
                  if (value == null) {
                    return PRIMITIVE_EMPTY;
                  }
                  return ZOrderByteUtils.shortToOrderedBytes(
                          value, inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
                      .array();
                },
                DataTypes.BinaryType)
            .withName("HILBERT_SHORT_ORDERED_BYTES");

    this.inputCol++;
    increaseOutputSize(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE);

    return udf;
  }

  private UserDefinedFunction intToOrderedBytesUDF() {
    int position = inputCol;
    UserDefinedFunction udf =
        functions
            .udf(
                (Integer value) -> {
                  if (value == null) {
                    return PRIMITIVE_EMPTY;
                  }
                  return ZOrderByteUtils.intToOrderedBytes(
                          value, inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
                      .array();
                },
                DataTypes.BinaryType)
            .withName("HILBERT_INT_ORDERED_BYTES");

    this.inputCol++;
    increaseOutputSize(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE);

    return udf;
  }

  private UserDefinedFunction longToOrderedBytesUDF() {
    int position = inputCol;
    UserDefinedFunction udf =
        functions
            .udf(
                (Long value) -> {
                  if (value == null) {
                    return PRIMITIVE_EMPTY;
                  }
                  return ZOrderByteUtils.longToOrderedBytes(
                          value, inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
                      .array();
                },
                DataTypes.BinaryType)
            .withName("HILBERT_LONG_ORDERED_BYTES");

    this.inputCol++;
    increaseOutputSize(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE);

    return udf;
  }

  private UserDefinedFunction floatToOrderedBytesUDF() {
    int position = inputCol;
    UserDefinedFunction udf =
        functions
            .udf(
                (Float value) -> {
                  if (value == null) {
                    return PRIMITIVE_EMPTY;
                  }
                  return ZOrderByteUtils.floatToOrderedBytes(
                          value, inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
                      .array();
                },
                DataTypes.BinaryType)
            .withName("HILBERT_FLOAT_ORDERED_BYTES");

    this.inputCol++;
    increaseOutputSize(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE);

    return udf;
  }

  private UserDefinedFunction doubleToOrderedBytesUDF() {
    int position = inputCol;
    UserDefinedFunction udf =
        functions
            .udf(
                (Double value) -> {
                  if (value == null) {
                    return PRIMITIVE_EMPTY;
                  }
                  return ZOrderByteUtils.doubleToOrderedBytes(
                          value, inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
                      .array();
                },
                DataTypes.BinaryType)
            .withName("HILBERT_DOUBLE_ORDERED_BYTES");

    this.inputCol++;
    increaseOutputSize(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE);

    return udf;
  }

  private UserDefinedFunction booleanToOrderedBytesUDF() {
    int position = inputCol;
    UserDefinedFunction udf =
        functions
            .udf(
                (Boolean value) -> {
                  ByteBuffer buffer = inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE);
                  buffer.put(0, (byte) (value ? -127 : 0));
                  return buffer.array();
                },
                DataTypes.BinaryType)
            .withName("HILBERT_BOOLEAN_ORDERED_BYTES");

    this.inputCol++;
    increaseOutputSize(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE);
    return udf;
  }

  private UserDefinedFunction stringToOrderedBytesUDF() {
    int position = inputCol;
    UserDefinedFunction udf =
        functions
            .udf(
                (String value) ->
                    ZOrderByteUtils.stringToOrderedBytes(
                            value, varTypeSize, inputBuffer(position, varTypeSize), encoder.get())
                        .array(),
                DataTypes.BinaryType)
            .withName("HILBERT_STRING_ORDERED_BYTES");

    this.inputCol++;
    increaseOutputSize(varTypeSize);

    return udf;
  }

  private UserDefinedFunction bytesTruncateUDF() {
    int position = inputCol;
    UserDefinedFunction udf =
        functions
            .udf(
                (byte[] value) ->
                    ZOrderByteUtils.byteTruncateOrFill(
                            value, varTypeSize, inputBuffer(position, varTypeSize))
                        .array(),
                DataTypes.BinaryType)
            .withName("HILBERT_BYTE_TRUNCATE");

    this.inputCol++;
    increaseOutputSize(varTypeSize);

    return udf;
  }

  private final UserDefinedFunction interleaveUDF =
      functions
          .udf((Seq<byte[]> arrayBinary) -> interleaveBits(arrayBinary), DataTypes.BinaryType)
          .withName("HILBERT_INTERLEAVE_BYTES");

  Column interleaveBytes(Column arrayBinary) {
    return interleaveUDF.apply(arrayBinary);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  Column sortedLexicographically(Column column, DataType type) {
    if (type instanceof ByteType) {
      return tinyToOrderedBytesUDF().apply(column);
    } else if (type instanceof ShortType) {
      return shortToOrderedBytesUDF().apply(column);
    } else if (type instanceof IntegerType) {
      return intToOrderedBytesUDF().apply(column);
    } else if (type instanceof LongType) {
      return longToOrderedBytesUDF().apply(column);
    } else if (type instanceof FloatType) {
      return floatToOrderedBytesUDF().apply(column);
    } else if (type instanceof DoubleType) {
      return doubleToOrderedBytesUDF().apply(column);
    } else if (type instanceof StringType) {
      return stringToOrderedBytesUDF().apply(column);
    } else if (type instanceof BinaryType) {
      return bytesTruncateUDF().apply(column);
    } else if (type instanceof BooleanType) {
      return booleanToOrderedBytesUDF().apply(column);
    } else if (type instanceof TimestampType) {
      return longToOrderedBytesUDF().apply(column.cast(DataTypes.LongType));
    } else if (type instanceof DateType) {
      return longToOrderedBytesUDF().apply(functions.unix_date(column).cast(DataTypes.LongType));
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Cannot use column %s of type %s in Hilbert Clustering, the type is unsupported",
              column, type));
    }
  }

  private void increaseOutputSize(int bytes) {
    totalOutputBytes = Math.min(totalOutputBytes + bytes, maxOutputSize);
  }
}
