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
package org.apache.iceberg.util;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Utility class for Hilbert curve calculations used in liquid clustering.
 *
 * <p>The Hilbert curve is a continuous space-filling curve that visits every point in a square grid
 * with a path that never crosses itself. It has the property that points that are close on the
 * curve are also close in the original space, which makes it excellent for multi-dimensional
 * indexing.
 *
 * <p>This implementation supports:
 *
 * <ul>
 *   <li>Computing Hilbert indices for multi-dimensional coordinates
 *   <li>Converting between coordinates and Hilbert indices
 *   <li>Generating byte representations suitable for sorting
 * </ul>
 *
 * <p>References:
 *
 * <ul>
 *   <li>https://en.wikipedia.org/wiki/Hilbert_curve
 *   <li>Hamilton, C. H. and Rau-Chaplin, A. (2008). Compact Hilbert Indices: Space-filling curves
 *       for domains with unequal side lengths.
 * </ul>
 */
public class HilbertByteUtils {

  /** Default number of bits per dimension for Hilbert index computation. */
  public static final int DEFAULT_BITS_PER_DIMENSION = 10;

  /** Maximum supported dimensions. */
  public static final int MAX_DIMENSIONS = 4;

  /** Size of the output buffer for Hilbert indices. */
  public static final int HILBERT_INDEX_SIZE = 16; // 128 bits for up to 4 dimensions with 32 bits each

  private HilbertByteUtils() {}

  /**
   * Computes the Hilbert index for the given coordinates.
   *
   * @param coordinates the coordinates in each dimension (values should be in [0, 2^bits))
   * @param bitsPerDimension the number of bits per dimension
   * @return the Hilbert index as a BigInteger
   */
  public static BigInteger hilbertIndex(int[] coordinates, int bitsPerDimension) {
    Preconditions.checkArgument(
        coordinates != null && coordinates.length > 0,
        "Coordinates must not be null or empty");
    Preconditions.checkArgument(
        coordinates.length <= MAX_DIMENSIONS,
        "Cannot compute Hilbert index for more than %s dimensions, got %s",
        MAX_DIMENSIONS,
        coordinates.length);
    Preconditions.checkArgument(
        bitsPerDimension > 0 && bitsPerDimension <= 32,
        "Bits per dimension must be between 1 and 32, got %s",
        bitsPerDimension);

    int n = coordinates.length;
    int maxCoord = (1 << bitsPerDimension) - 1;

    // Verify coordinates are in valid range
    for (int i = 0; i < n; i++) {
      Preconditions.checkArgument(
          coordinates[i] >= 0 && coordinates[i] <= maxCoord,
          "Coordinate %s out of range [0, %s]: %s",
          i,
          maxCoord,
          coordinates[i]);
    }

    // Copy coordinates since we'll modify them
    int[] coords = Arrays.copyOf(coordinates, n);

    // Compute Hilbert index using the algorithm from Hamilton & Rau-Chaplin
    BigInteger index = BigInteger.ZERO;
    int rx, ry, s;

    for (s = bitsPerDimension - 1; s >= 0; s--) {
      // Get the position in each dimension at this level
      int[] position = new int[n];
      for (int i = 0; i < n; i++) {
        position[i] = (coords[i] >> s) & 1;
      }

      // Add contribution from this level to the index
      int contribution = positionToIndex(position, n);
      index = index.shiftLeft(n).add(BigInteger.valueOf(contribution));

      // Rotate coordinates for next iteration
      if (s > 0) {
        rotateCoordinates(coords, s, position, n);
      }
    }

    return index;
  }

  /**
   * Computes the Hilbert index and returns it as a byte array for sorting.
   *
   * @param coordinates the coordinates in each dimension
   * @param bitsPerDimension the number of bits per dimension
   * @param reuse a ByteBuffer to reuse for output (optional, may be null)
   * @return a ByteBuffer containing the Hilbert index in big-endian byte order
   */
  public static ByteBuffer hilbertIndexToBytes(
      int[] coordinates, int bitsPerDimension, ByteBuffer reuse) {
    BigInteger index = hilbertIndex(coordinates, bitsPerDimension);
    byte[] indexBytes = index.toByteArray();

    ByteBuffer buffer = ByteBuffers.reuse(reuse, HILBERT_INDEX_SIZE);
    Arrays.fill(buffer.array(), 0, HILBERT_INDEX_SIZE, (byte) 0);

    // Copy index bytes to the end of the buffer (big-endian, right-aligned)
    int startPos = HILBERT_INDEX_SIZE - indexBytes.length;
    if (startPos < 0) {
      // Index is larger than buffer, take the most significant bytes
      buffer.put(indexBytes, -startPos, HILBERT_INDEX_SIZE);
    } else {
      // Pad with zeros on the left
      buffer.position(startPos);
      buffer.put(indexBytes);
    }

    buffer.rewind();
    return buffer;
  }

  /**
   * Computes the Hilbert index for long coordinates.
   *
   * @param coordinates the coordinates in each dimension (values should be in [0, 2^bits))
   * @param bitsPerDimension the number of bits per dimension
   * @return the Hilbert index as a BigInteger
   */
  public static BigInteger hilbertIndex(long[] coordinates, int bitsPerDimension) {
    int[] intCoords = new int[coordinates.length];
    for (int i = 0; i < coordinates.length; i++) {
      intCoords[i] = (int) (coordinates[i] & 0xFFFFFFFFL);
    }
    return hilbertIndex(intCoords, bitsPerDimension);
  }

  /**
   * Converts a column value to a range partition ID for use in Hilbert index computation.
   *
   * <p>This maps the value to an integer in [0, numRanges) using the provided min/max bounds.
   *
   * @param value the column value
   * @param min the minimum value (inclusive)
   * @param max the maximum value (inclusive)
   * @param numRanges the number of ranges to partition into
   * @return the range partition ID
   */
  public static int rangePartitionId(long value, long min, long max, int numRanges) {
    if (max == min) {
      return 0;
    }

    // Normalize to [0, 1] then scale to [0, numRanges)
    double normalized = (double) (value - min) / (max - min);
    int rangeId = (int) (normalized * numRanges);

    // Clamp to valid range
    return Math.max(0, Math.min(numRanges - 1, rangeId));
  }

  /**
   * Converts a double value to a range partition ID for use in Hilbert index computation.
   *
   * @param value the column value
   * @param min the minimum value (inclusive)
   * @param max the maximum value (inclusive)
   * @param numRanges the number of ranges to partition into
   * @return the range partition ID
   */
  public static int rangePartitionId(double value, double min, double max, int numRanges) {
    if (max == min) {
      return 0;
    }

    double normalized = (value - min) / (max - min);
    int rangeId = (int) (normalized * numRanges);

    return Math.max(0, Math.min(numRanges - 1, rangeId));
  }

  /**
   * Converts a position in n-dimensional space to an index in the Hilbert curve.
   *
   * @param position binary position in each dimension (0 or 1)
   * @param n number of dimensions
   * @return the index contribution from this position
   */
  private static int positionToIndex(int[] position, int n) {
    if (n == 2) {
      // Optimized 2D case using Gray code
      int d = 0;
      for (int i = 0; i < n; i++) {
        d = (d << 1) | position[i];
      }
      // Apply Gray code transformation
      return d ^ (d >> 1);
    }

    // General n-dimensional case
    int index = 0;
    for (int i = 0; i < n; i++) {
      index = (index << 1) | position[i];
    }

    // Apply n-dimensional Gray code
    return index ^ (index >> 1);
  }

  /**
   * Rotates coordinates for the next level of recursion in the Hilbert curve algorithm.
   *
   * @param coords the coordinates to rotate (modified in place)
   * @param s the current bit level
   * @param position the current position
   * @param n number of dimensions
   */
  private static void rotateCoordinates(int[] coords, int s, int[] position, int n) {
    int mask = (1 << s) - 1;

    if (n == 2) {
      // Optimized 2D rotation
      int rx = position[0];
      int ry = position[1];

      if (ry == 0) {
        if (rx == 1) {
          coords[0] = mask - coords[0];
          coords[1] = mask - coords[1];
        }
        // Swap x and y
        int temp = coords[0];
        coords[0] = coords[1];
        coords[1] = temp;
      }
    } else {
      // General n-dimensional rotation
      // Apply rotation based on the quadrant we're in
      int quadrant = 0;
      for (int i = 0; i < n; i++) {
        quadrant = (quadrant << 1) | position[i];
      }

      // Different rotations for different quadrants
      if ((quadrant & 1) == 0) {
        // Swap dimensions
        int temp = coords[0];
        for (int i = 0; i < n - 1; i++) {
          coords[i] = coords[i + 1];
        }
        coords[n - 1] = temp;
      }

      if ((quadrant & (1 << (n - 1))) != 0) {
        // Invert some coordinates
        for (int i = 0; i < n; i++) {
          if ((quadrant & (1 << i)) != 0) {
            coords[i] = mask - coords[i];
          }
        }
      }
    }
  }

  /**
   * Interleaves bytes from multiple columns for composite sorting key generation.
   *
   * <p>This is similar to Z-Order interleaving but used as a fallback when Hilbert curve is not
   * needed.
   *
   * @param columnBytes array of byte arrays, one per column
   * @param outputSize the desired output size in bytes
   * @return the interleaved bytes
   */
  public static byte[] interleaveBits(byte[][] columnBytes, int outputSize) {
    // Delegate to ZOrderByteUtils for bit interleaving
    return ZOrderByteUtils.interleaveBits(columnBytes, outputSize);
  }
}
