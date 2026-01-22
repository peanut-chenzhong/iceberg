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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

/**
 * Implementation of {@link UpdateClusteringSpec} for updating a table's clustering specification.
 *
 * <p>This class manages the clustering configuration through table properties, allowing dynamic
 * modification of clustering columns without changing the underlying table metadata structure.
 */
class BaseUpdateClusteringSpec implements UpdateClusteringSpec {

  private final TableOperations ops;
  private final Schema schema;

  private List<String> clusteringColumns = null;
  private String algorithm = null;
  private Long targetZCubeSizeBytes = null;
  private Long minZCubeSizeBytes = null;
  private boolean disableClustering = false;

  BaseUpdateClusteringSpec(TableOperations ops) {
    this.ops = ops;
    this.schema = ops.current().schema();
  }

  @Override
  public UpdateClusteringSpec clusterBy(String... columns) {
    Preconditions.checkArgument(
        columns != null && columns.length > 0, "Clustering columns cannot be null or empty");
    Preconditions.checkArgument(
        columns.length <= ClusteringSpec.MAX_CLUSTERING_COLUMNS,
        "Cannot have more than %s clustering columns, got %s",
        ClusteringSpec.MAX_CLUSTERING_COLUMNS,
        columns.length);

    // Validate columns exist in schema
    for (String column : columns) {
      Types.NestedField field = schema.findField(column);
      ValidationException.check(field != null, "Column '%s' not found in schema", column);
      ValidationException.check(
          field.type().isPrimitiveType(),
          "Clustering column '%s' must be a primitive type, got %s",
          column,
          field.type());
    }

    this.clusteringColumns = Arrays.asList(columns);
    this.disableClustering = false;
    return this;
  }

  @Override
  public UpdateClusteringSpec clusterByNone() {
    this.disableClustering = true;
    this.clusteringColumns = null;
    return this;
  }

  @Override
  public UpdateClusteringSpec algorithm(String newAlgorithm) {
    Preconditions.checkArgument(
        newAlgorithm != null && !newAlgorithm.isEmpty(), "Algorithm cannot be null or empty");
    // Validate the algorithm is supported
    ClusteringSpec.Algorithm.fromName(newAlgorithm);
    this.algorithm = newAlgorithm;
    return this;
  }

  @Override
  public UpdateClusteringSpec targetZCubeSize(long bytes) {
    Preconditions.checkArgument(bytes > 0, "Target ZCube size must be positive: %s", bytes);
    this.targetZCubeSizeBytes = bytes;
    return this;
  }

  @Override
  public UpdateClusteringSpec minZCubeSize(long bytes) {
    Preconditions.checkArgument(bytes > 0, "Minimum ZCube size must be positive: %s", bytes);
    this.minZCubeSizeBytes = bytes;
    return this;
  }

  @Override
  public ClusteringSpec apply() {
    TableMetadata base = ops.current();

    // Build the new clustering spec based on the changes
    if (disableClustering) {
      return ClusteringSpec.unclustered();
    }

    if (clusteringColumns == null) {
      // No changes to columns, just return current spec
      return getClusteringSpecFromProperties(base);
    }

    ClusteringSpec.Builder builder = ClusteringSpec.builderFor(schema);

    for (String column : clusteringColumns) {
      builder.clusterBy(column);
    }

    if (algorithm != null) {
      builder.algorithm(algorithm);
    }

    if (targetZCubeSizeBytes != null) {
      builder.targetZCubeSize(targetZCubeSizeBytes);
    }

    if (minZCubeSizeBytes != null) {
      builder.minZCubeSize(minZCubeSizeBytes);
    }

    return builder.build();
  }

  @Override
  public void commit() {
    TableMetadata base = ops.refresh();

    // Build the updated properties
    UpdateProperties updateProperties = new PropertiesUpdate(ops);

    if (disableClustering) {
      updateProperties.set(TableProperties.CLUSTERING_ENABLED, "false");
      updateProperties.remove(TableProperties.CLUSTERING_COLUMNS);
    } else if (clusteringColumns != null) {
      updateProperties.set(TableProperties.CLUSTERING_ENABLED, "true");
      updateProperties.set(
          TableProperties.CLUSTERING_COLUMNS, String.join(",", clusteringColumns));

      if (algorithm != null) {
        updateProperties.set(TableProperties.CLUSTERING_ALGORITHM, algorithm);
      }

      if (targetZCubeSizeBytes != null) {
        updateProperties.set(
            TableProperties.CLUSTERING_TARGET_ZCUBE_SIZE_BYTES,
            String.valueOf(targetZCubeSizeBytes));
      }

      if (minZCubeSizeBytes != null) {
        updateProperties.set(
            TableProperties.CLUSTERING_MIN_ZCUBE_SIZE_BYTES, String.valueOf(minZCubeSizeBytes));
      }
    }

    updateProperties.commit();
  }

  private ClusteringSpec getClusteringSpecFromProperties(TableMetadata metadata) {
    String columnsStr = metadata.properties().get(TableProperties.CLUSTERING_COLUMNS);

    if (columnsStr == null || columnsStr.isEmpty()) {
      return ClusteringSpec.unclustered();
    }

    List<String> columns =
        Arrays.stream(columnsStr.split(",")).map(String::trim).collect(Collectors.toList());

    ClusteringSpec.Builder builder = ClusteringSpec.builderFor(schema);

    for (String column : columns) {
      builder.clusterBy(column);
    }

    String algo =
        metadata
            .properties()
            .getOrDefault(
                TableProperties.CLUSTERING_ALGORITHM,
                ClusteringSpec.DEFAULT_ALGORITHM.algorithmName());
    builder.algorithm(algo);

    String targetSize = metadata.properties().get(TableProperties.CLUSTERING_TARGET_ZCUBE_SIZE_BYTES);
    if (targetSize != null) {
      builder.targetZCubeSize(Long.parseLong(targetSize));
    }

    String minSize = metadata.properties().get(TableProperties.CLUSTERING_MIN_ZCUBE_SIZE_BYTES);
    if (minSize != null) {
      builder.minZCubeSize(Long.parseLong(minSize));
    }

    return builder.build();
  }
}
