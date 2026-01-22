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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestClusteringSpec {

  private static final Schema TEST_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "user_id", Types.StringType.get()),
          Types.NestedField.required(3, "event_type", Types.StringType.get()),
          Types.NestedField.optional(4, "event_time", Types.TimestampType.withZone()));

  @Test
  public void testUnclusteredSpec() {
    ClusteringSpec spec = ClusteringSpec.unclustered();

    assertThat(spec.isUnclustered()).isTrue();
    assertThat(spec.isClustered()).isFalse();
    assertThat(spec.specId()).isEqualTo(ClusteringSpec.UNCLUSTERED_SPEC_ID);
    assertThat(spec.columns()).isEmpty();
  }

  @Test
  public void testBasicClusteringSpec() {
    ClusteringSpec spec =
        ClusteringSpec.builderFor(TEST_SCHEMA).clusterBy("user_id", "event_type").build();

    assertThat(spec.isClustered()).isTrue();
    assertThat(spec.isUnclustered()).isFalse();
    assertThat(spec.columns()).hasSize(2);
    assertThat(spec.columnNames()).containsExactly("user_id", "event_type");
    assertThat(spec.algorithm()).isEqualTo(ClusteringSpec.Algorithm.HILBERT);
    assertThat(spec.targetZCubeSizeBytes()).isEqualTo(ClusteringSpec.DEFAULT_TARGET_ZCUBE_SIZE_BYTES);
  }

  @Test
  public void testClusteringSpecWithCustomSettings() {
    ClusteringSpec spec =
        ClusteringSpec.builderFor(TEST_SCHEMA)
            .clusterBy("user_id")
            .algorithm(ClusteringSpec.Algorithm.ZORDER)
            .targetZCubeSize(1024L * 1024 * 1024) // 1GB
            .minZCubeSize(256L * 1024 * 1024) // 256MB
            .build();

    assertThat(spec.columns()).hasSize(1);
    assertThat(spec.columnNames()).containsExactly("user_id");
    assertThat(spec.algorithm()).isEqualTo(ClusteringSpec.Algorithm.ZORDER);
    assertThat(spec.targetZCubeSizeBytes()).isEqualTo(1024L * 1024 * 1024);
    assertThat(spec.minZCubeSizeBytes()).isEqualTo(256L * 1024 * 1024);
  }

  @Test
  public void testMaxClusteringColumns() {
    // 4 columns should work
    ClusteringSpec spec =
        ClusteringSpec.builderFor(TEST_SCHEMA)
            .clusterBy("id", "user_id", "event_type", "event_time")
            .build();

    assertThat(spec.columns()).hasSize(4);
  }

  @Test
  public void testTooManyClusteringColumns() {
    Schema largeSchema =
        new Schema(
            Types.NestedField.required(1, "col1", Types.StringType.get()),
            Types.NestedField.required(2, "col2", Types.StringType.get()),
            Types.NestedField.required(3, "col3", Types.StringType.get()),
            Types.NestedField.required(4, "col4", Types.StringType.get()),
            Types.NestedField.required(5, "col5", Types.StringType.get()));

    assertThatThrownBy(
            () ->
                ClusteringSpec.builderFor(largeSchema)
                    .clusterBy("col1", "col2", "col3", "col4", "col5")
                    .build())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot have more than 4 clustering columns");
  }

  @Test
  public void testInvalidColumnName() {
    assertThatThrownBy(
            () -> ClusteringSpec.builderFor(TEST_SCHEMA).clusterBy("nonexistent_column").build())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find column");
  }

  @Test
  public void testNonPrimitiveColumnRejected() {
    Schema schemaWithStruct =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(
                2,
                "data",
                Types.StructType.of(Types.NestedField.required(3, "nested", Types.StringType.get()))));

    assertThatThrownBy(
            () -> ClusteringSpec.builderFor(schemaWithStruct).clusterBy("data").build())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("must be a primitive type");
  }

  @Test
  public void testMinSizeGreaterThanTargetRejected() {
    assertThatThrownBy(
            () ->
                ClusteringSpec.builderFor(TEST_SCHEMA)
                    .clusterBy("user_id")
                    .targetZCubeSize(100L * 1024 * 1024)
                    .minZCubeSize(200L * 1024 * 1024)
                    .build())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Minimum ZCube size");
  }

  @Test
  public void testAlgorithmFromName() {
    assertThat(ClusteringSpec.Algorithm.fromName("hilbert"))
        .isEqualTo(ClusteringSpec.Algorithm.HILBERT);
    assertThat(ClusteringSpec.Algorithm.fromName("HILBERT"))
        .isEqualTo(ClusteringSpec.Algorithm.HILBERT);
    assertThat(ClusteringSpec.Algorithm.fromName("zorder"))
        .isEqualTo(ClusteringSpec.Algorithm.ZORDER);

    assertThatThrownBy(() -> ClusteringSpec.Algorithm.fromName("unknown"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testSameColumns() {
    ClusteringSpec spec1 =
        ClusteringSpec.builderFor(TEST_SCHEMA).clusterBy("user_id", "event_type").build();
    ClusteringSpec spec2 =
        ClusteringSpec.builderFor(TEST_SCHEMA).clusterBy("user_id", "event_type").build();
    ClusteringSpec spec3 =
        ClusteringSpec.builderFor(TEST_SCHEMA).clusterBy("event_type", "user_id").build();

    assertThat(spec1.sameColumns(spec2)).isTrue();
    assertThat(spec1.sameColumns(spec3)).isFalse(); // Different order
  }

  @Test
  public void testEquality() {
    ClusteringSpec spec1 =
        ClusteringSpec.builderFor(TEST_SCHEMA)
            .withSpecId(1)
            .clusterBy("user_id")
            .algorithm(ClusteringSpec.Algorithm.HILBERT)
            .build();
    ClusteringSpec spec2 =
        ClusteringSpec.builderFor(TEST_SCHEMA)
            .withSpecId(1)
            .clusterBy("user_id")
            .algorithm(ClusteringSpec.Algorithm.HILBERT)
            .build();

    assertThat(spec1).isEqualTo(spec2);
    assertThat(spec1.hashCode()).isEqualTo(spec2.hashCode());
  }

  @Test
  public void testToString() {
    ClusteringSpec spec =
        ClusteringSpec.builderFor(TEST_SCHEMA).clusterBy("user_id", "event_type").build();

    String str = spec.toString();
    assertThat(str).contains("user_id");
    assertThat(str).contains("event_type");
    assertThat(str).contains("hilbert");
  }
}
