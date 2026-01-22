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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.JsonUtil;

/** Utility class for parsing and serializing ClusteringSpec to/from JSON. */
public class ClusteringSpecParser {

  private static final String SPEC_ID = "spec-id";
  private static final String COLUMNS = "columns";
  private static final String SOURCE_ID = "source-id";
  private static final String NAME = "name";
  private static final String ALGORITHM = "algorithm";
  private static final String TARGET_ZCUBE_SIZE_BYTES = "target-zcube-size-bytes";
  private static final String MIN_ZCUBE_SIZE_BYTES = "min-zcube-size-bytes";

  private ClusteringSpecParser() {}

  /**
   * Serializes a ClusteringSpec to JSON string.
   *
   * @param spec the clustering spec to serialize
   * @return the JSON string representation
   */
  public static String toJson(ClusteringSpec spec) {
    return toJson(spec, false);
  }

  /**
   * Serializes a ClusteringSpec to JSON string.
   *
   * @param spec the clustering spec to serialize
   * @param pretty whether to format the output
   * @return the JSON string representation
   */
  public static String toJson(ClusteringSpec spec, boolean pretty) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      if (pretty) {
        generator.useDefaultPrettyPrinter();
      }
      toJson(spec, generator);
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write clustering spec to JSON", e);
    }
  }

  /**
   * Writes a ClusteringSpec to a JSON generator.
   *
   * @param spec the clustering spec to write
   * @param generator the JSON generator
   * @throws IOException if writing fails
   */
  public static void toJson(ClusteringSpec spec, JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    generator.writeNumberField(SPEC_ID, spec.specId());

    generator.writeArrayFieldStart(COLUMNS);
    for (ClusteringColumn column : spec.columns()) {
      generator.writeStartObject();
      generator.writeNumberField(SOURCE_ID, column.sourceId());
      generator.writeStringField(NAME, column.name());
      generator.writeEndObject();
    }
    generator.writeEndArray();

    generator.writeStringField(ALGORITHM, spec.algorithm().algorithmName());
    generator.writeNumberField(TARGET_ZCUBE_SIZE_BYTES, spec.targetZCubeSizeBytes());
    generator.writeNumberField(MIN_ZCUBE_SIZE_BYTES, spec.minZCubeSizeBytes());

    generator.writeEndObject();
  }

  /**
   * Parses a ClusteringSpec from JSON string.
   *
   * @param json the JSON string
   * @return the parsed clustering spec
   */
  public static ClusteringSpec fromJson(String json) {
    try {
      return fromJson(JsonUtil.mapper().readValue(json, JsonNode.class));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to parse clustering spec from JSON", e);
    }
  }

  /**
   * Parses a ClusteringSpec from a JSON node.
   *
   * @param node the JSON node
   * @return the parsed clustering spec
   */
  public static ClusteringSpec fromJson(JsonNode node) {
    Preconditions.checkArgument(
        node != null && node.isObject(), "Cannot parse clustering spec from non-object: %s", node);

    int specId = JsonUtil.getInt(SPEC_ID, node);

    List<ClusteringColumn> columns = Lists.newArrayList();
    if (node.has(COLUMNS)) {
      JsonNode columnsNode = node.get(COLUMNS);
      Preconditions.checkArgument(
          columnsNode.isArray(), "Cannot parse columns from non-array: %s", columnsNode);

      Iterator<JsonNode> elements = columnsNode.elements();
      while (elements.hasNext()) {
        JsonNode columnNode = elements.next();
        int sourceId = JsonUtil.getInt(SOURCE_ID, columnNode);
        String name = JsonUtil.getString(NAME, columnNode);
        columns.add(new ClusteringColumn(sourceId, name));
      }
    }

    if (columns.isEmpty()) {
      return ClusteringSpec.unclustered();
    }

    String algorithmName =
        JsonUtil.getStringOrDefault(ALGORITHM, node, ClusteringSpec.DEFAULT_ALGORITHM.algorithmName());
    ClusteringSpec.Algorithm algorithm = ClusteringSpec.Algorithm.fromName(algorithmName);

    long targetZCubeSizeBytes =
        JsonUtil.getLongOrDefault(
            TARGET_ZCUBE_SIZE_BYTES, node, ClusteringSpec.DEFAULT_TARGET_ZCUBE_SIZE_BYTES);
    long minZCubeSizeBytes =
        JsonUtil.getLongOrDefault(
            MIN_ZCUBE_SIZE_BYTES, node, ClusteringSpec.DEFAULT_MIN_ZCUBE_SIZE_BYTES);

    return new UnboundClusteringSpec(
            specId, columns, algorithm, targetZCubeSizeBytes, minZCubeSizeBytes)
        .toClusteringSpec();
  }

  /**
   * An unbound clustering spec used during parsing before schema binding.
   */
  private static class UnboundClusteringSpec {
    private final int specId;
    private final List<ClusteringColumn> columns;
    private final ClusteringSpec.Algorithm algorithm;
    private final long targetZCubeSizeBytes;
    private final long minZCubeSizeBytes;

    UnboundClusteringSpec(
        int specId,
        List<ClusteringColumn> columns,
        ClusteringSpec.Algorithm algorithm,
        long targetZCubeSizeBytes,
        long minZCubeSizeBytes) {
      this.specId = specId;
      this.columns = columns;
      this.algorithm = algorithm;
      this.targetZCubeSizeBytes = targetZCubeSizeBytes;
      this.minZCubeSizeBytes = minZCubeSizeBytes;
    }

    ClusteringSpec toClusteringSpec() {
      // Create a dummy schema just for building - the actual binding happens during table load
      Schema dummySchema = buildDummySchema();
      ClusteringSpec.Builder builder =
          ClusteringSpec.builderFor(dummySchema)
              .withSpecId(specId)
              .algorithm(algorithm)
              .targetZCubeSize(targetZCubeSizeBytes)
              .minZCubeSize(minZCubeSizeBytes);

      for (ClusteringColumn column : columns) {
        // Use the column directly without schema validation
        builder.addColumn(column);
      }

      return builder.buildUnchecked();
    }

    private Schema buildDummySchema() {
      List<Types.NestedField> fields = Lists.newArrayList();
      for (ClusteringColumn column : columns) {
        fields.add(Types.NestedField.optional(column.sourceId(), column.name(), Types.StringType.get()));
      }
      return new Schema(fields);
    }
  }
}
