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

import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestClusteringSpecParser {

  private static final Schema TEST_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "user_id", Types.StringType.get()),
          Types.NestedField.required(3, "event_type", Types.StringType.get()));

  @Test
  public void testSerializeUnclustered() {
    ClusteringSpec spec = ClusteringSpec.unclustered();

    String json = ClusteringSpecParser.toJson(spec);
    ClusteringSpec parsed = ClusteringSpecParser.fromJson(json);

    assertThat(parsed.isUnclustered()).isTrue();
  }

  @Test
  public void testSerializeBasicSpec() {
    ClusteringSpec spec =
        ClusteringSpec.builderFor(TEST_SCHEMA)
            .withSpecId(1)
            .clusterBy("user_id", "event_type")
            .build();

    String json = ClusteringSpecParser.toJson(spec);
    assertThat(json).contains("\"spec-id\":1");
    assertThat(json).contains("user_id");
    assertThat(json).contains("event_type");
    assertThat(json).contains("hilbert");

    ClusteringSpec parsed = ClusteringSpecParser.fromJson(json);
    assertThat(parsed.specId()).isEqualTo(1);
    assertThat(parsed.columnNames()).containsExactly("user_id", "event_type");
    assertThat(parsed.algorithm()).isEqualTo(ClusteringSpec.Algorithm.HILBERT);
  }

  @Test
  public void testSerializeWithCustomSettings() {
    ClusteringSpec spec =
        ClusteringSpec.builderFor(TEST_SCHEMA)
            .withSpecId(2)
            .clusterBy("user_id")
            .algorithm(ClusteringSpec.Algorithm.ZORDER)
            .targetZCubeSize(1024L * 1024 * 1024)
            .minZCubeSize(256L * 1024 * 1024)
            .build();

    String json = ClusteringSpecParser.toJson(spec);
    ClusteringSpec parsed = ClusteringSpecParser.fromJson(json);

    assertThat(parsed.specId()).isEqualTo(2);
    assertThat(parsed.algorithm()).isEqualTo(ClusteringSpec.Algorithm.ZORDER);
    assertThat(parsed.targetZCubeSizeBytes()).isEqualTo(1024L * 1024 * 1024);
    assertThat(parsed.minZCubeSizeBytes()).isEqualTo(256L * 1024 * 1024);
  }

  @Test
  public void testPrettyPrint() {
    ClusteringSpec spec =
        ClusteringSpec.builderFor(TEST_SCHEMA).clusterBy("user_id").build();

    String prettyJson = ClusteringSpecParser.toJson(spec, true);
    assertThat(prettyJson).contains("\n"); // Pretty print should have newlines
    assertThat(prettyJson).contains("  "); // And indentation
  }

  @Test
  public void testRoundTrip() {
    ClusteringSpec original =
        ClusteringSpec.builderFor(TEST_SCHEMA)
            .withSpecId(5)
            .clusterBy("id", "user_id", "event_type")
            .algorithm(ClusteringSpec.Algorithm.HILBERT)
            .targetZCubeSize(512L * 1024 * 1024)
            .minZCubeSize(128L * 1024 * 1024)
            .build();

    String json = ClusteringSpecParser.toJson(original);
    ClusteringSpec parsed = ClusteringSpecParser.fromJson(json);

    assertThat(parsed.specId()).isEqualTo(original.specId());
    assertThat(parsed.columnNames()).isEqualTo(original.columnNames());
    assertThat(parsed.algorithm()).isEqualTo(original.algorithm());
    assertThat(parsed.targetZCubeSizeBytes()).isEqualTo(original.targetZCubeSizeBytes());
    assertThat(parsed.minZCubeSizeBytes()).isEqualTo(original.minZCubeSizeBytes());
  }
}
