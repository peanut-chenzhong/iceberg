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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

public class TestHilbertByteUtils {

  @Test
  public void testHilbertIndex2D() {
    // Test basic 2D Hilbert index computation
    int bits = 4; // 16x16 grid
    int maxCoord = (1 << bits) - 1;

    // Origin should have index 0
    BigInteger originIndex = HilbertByteUtils.hilbertIndex(new int[] {0, 0}, bits);
    assertThat(originIndex).isEqualTo(BigInteger.ZERO);

    // Test that nearby points have nearby indices
    BigInteger index1 = HilbertByteUtils.hilbertIndex(new int[] {0, 1}, bits);
    BigInteger index2 = HilbertByteUtils.hilbertIndex(new int[] {1, 1}, bits);
    BigInteger index3 = HilbertByteUtils.hilbertIndex(new int[] {1, 0}, bits);

    // Adjacent points should have indices within a small range
    assertThat(index1.subtract(originIndex).abs().intValue()).isLessThanOrEqualTo(3);
    assertThat(index2.subtract(index1).abs().intValue()).isLessThanOrEqualTo(3);
    assertThat(index3.subtract(index2).abs().intValue()).isLessThanOrEqualTo(3);
  }

  @Test
  public void testHilbertIndex3D() {
    int bits = 3; // 8x8x8 cube

    BigInteger index1 = HilbertByteUtils.hilbertIndex(new int[] {0, 0, 0}, bits);
    BigInteger index2 = HilbertByteUtils.hilbertIndex(new int[] {7, 7, 7}, bits);

    // Indices should be different
    assertThat(index1).isNotEqualTo(index2);

    // Maximum index for 3D with 3 bits is 2^(3*3) - 1 = 511
    assertThat(index2.compareTo(BigInteger.valueOf(512))).isLessThan(0);
  }

  @Test
  public void testHilbertIndexToBytes() {
    int[] coords = {100, 200};
    int bits = 10;

    ByteBuffer buffer = HilbertByteUtils.hilbertIndexToBytes(coords, bits, null);

    assertThat(buffer.remaining()).isEqualTo(HilbertByteUtils.HILBERT_INDEX_SIZE);

    // Verify the buffer can be used for comparison
    byte[] bytes = buffer.array();
    assertThat(bytes).hasSize(HilbertByteUtils.HILBERT_INDEX_SIZE);
  }

  @Test
  public void testHilbertIndexPreservesLocality() {
    int bits = 8;

    // Points that are close in 2D space
    int[] p1 = {100, 100};
    int[] p2 = {101, 100};
    int[] p3 = {100, 101};

    // Points that are far apart
    int[] p4 = {0, 0};
    int[] p5 = {255, 255};

    BigInteger h1 = HilbertByteUtils.hilbertIndex(p1, bits);
    BigInteger h2 = HilbertByteUtils.hilbertIndex(p2, bits);
    BigInteger h3 = HilbertByteUtils.hilbertIndex(p3, bits);
    BigInteger h4 = HilbertByteUtils.hilbertIndex(p4, bits);
    BigInteger h5 = HilbertByteUtils.hilbertIndex(p5, bits);

    // Close points should have close indices (within a reasonable range)
    BigInteger closeDistance = h2.subtract(h1).abs().max(h3.subtract(h1).abs());
    BigInteger farDistance = h5.subtract(h4).abs();

    // The Hilbert curve property: close points should have smaller index differences
    // than far points (on average, though not guaranteed for all cases)
    assertThat(closeDistance.compareTo(BigInteger.valueOf(100))).isLessThan(0);
  }

  @Test
  public void testRangePartitionIdLong() {
    // Test basic range partitioning
    assertThat(HilbertByteUtils.rangePartitionId(0, 0, 100, 10)).isEqualTo(0);
    assertThat(HilbertByteUtils.rangePartitionId(100, 0, 100, 10)).isEqualTo(9);
    assertThat(HilbertByteUtils.rangePartitionId(50, 0, 100, 10)).isEqualTo(5);

    // Edge cases
    assertThat(HilbertByteUtils.rangePartitionId(50, 50, 50, 10)).isEqualTo(0);
    assertThat(HilbertByteUtils.rangePartitionId(-100, -100, 100, 10)).isEqualTo(0);
  }

  @Test
  public void testRangePartitionIdDouble() {
    assertThat(HilbertByteUtils.rangePartitionId(0.0, 0.0, 1.0, 10)).isEqualTo(0);
    assertThat(HilbertByteUtils.rangePartitionId(1.0, 0.0, 1.0, 10)).isEqualTo(9);
    assertThat(HilbertByteUtils.rangePartitionId(0.5, 0.0, 1.0, 10)).isEqualTo(5);
  }

  @Test
  public void testInvalidCoordinates() {
    assertThatThrownBy(() -> HilbertByteUtils.hilbertIndex(null, 10))
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> HilbertByteUtils.hilbertIndex(new int[] {}, 10))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testInvalidBitsPerDimension() {
    assertThatThrownBy(() -> HilbertByteUtils.hilbertIndex(new int[] {1, 2}, 0))
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> HilbertByteUtils.hilbertIndex(new int[] {1, 2}, 33))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testTooManyDimensions() {
    assertThatThrownBy(
            () -> HilbertByteUtils.hilbertIndex(new int[] {1, 2, 3, 4, 5}, 10))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("more than");
  }

  @Test
  public void testCoordinateOutOfRange() {
    int bits = 4; // max coord is 15

    assertThatThrownBy(() -> HilbertByteUtils.hilbertIndex(new int[] {16, 0}, bits))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("out of range");

    assertThatThrownBy(() -> HilbertByteUtils.hilbertIndex(new int[] {-1, 0}, bits))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("out of range");
  }

  @Test
  public void testDeterministic() {
    int[] coords = {42, 73, 19};
    int bits = 8;

    BigInteger index1 = HilbertByteUtils.hilbertIndex(coords, bits);
    BigInteger index2 = HilbertByteUtils.hilbertIndex(coords, bits);

    assertThat(index1).isEqualTo(index2);
  }

  @Test
  public void testBufferReuse() {
    ByteBuffer reusable = ByteBuffer.allocate(HilbertByteUtils.HILBERT_INDEX_SIZE);

    ByteBuffer result1 = HilbertByteUtils.hilbertIndexToBytes(new int[] {1, 2}, 8, reusable);
    byte[] bytes1 = result1.array().clone();

    ByteBuffer result2 = HilbertByteUtils.hilbertIndexToBytes(new int[] {3, 4}, 8, reusable);
    byte[] bytes2 = result2.array();

    // Results should be different
    assertThat(bytes1).isNotEqualTo(bytes2);

    // Buffer should be reused
    assertThat(result1.array()).isSameAs(result2.array());
  }
}
