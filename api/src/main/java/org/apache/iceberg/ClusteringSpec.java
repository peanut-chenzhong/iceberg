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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Represents a clustering specification for liquid clustering in Iceberg tables.
 *
 * <p>A clustering spec defines how data files should be organized within the table to optimize
 * query performance. It includes:
 *
 * <ul>
 *   <li>Clustering columns - the columns used for data organization
 *   <li>Algorithm - the clustering algorithm (hilbert or zorder)
 *   <li>Target ZCube size - the target size for each ZCube (cluster unit)
 * </ul>
 */
public class ClusteringSpec implements Serializable {

  /** The clustering algorithm to use. */
  public enum Algorithm {
    /** Hilbert space-filling curve, better for 2+ dimensions. */
    HILBERT("hilbert"),
    /** Z-Order curve, suitable for all dimensions. */
    ZORDER("zorder");

    private final String name;

    Algorithm(String name) {
      this.name = name;
    }

    public String algorithmName() {
      return name;
    }

    public static Algorithm fromName(String name) {
      for (Algorithm alg : values()) {
        if (alg.algorithmName().equalsIgnoreCase(name)) {
          return alg;
        }
      }
      throw new IllegalArgumentException("Unknown clustering algorithm: " + name);
    }
  }

  /** Maximum number of clustering columns allowed. */
  public static final int MAX_CLUSTERING_COLUMNS = 4;

  /** Default clustering algorithm. */
  public static final Algorithm DEFAULT_ALGORITHM = Algorithm.HILBERT;

  /** Default target ZCube size in bytes (512 MB). */
  public static final long DEFAULT_TARGET_ZCUBE_SIZE_BYTES = 512L * 1024 * 1024;

  /** Default minimum ZCube size in bytes (128 MB). */
  public static final long DEFAULT_MIN_ZCUBE_SIZE_BYTES = 128L * 1024 * 1024;

  /** Spec ID for unclustered tables. */
  public static final int UNCLUSTERED_SPEC_ID = 0;

  private static final ClusteringSpec UNCLUSTERED =
      new ClusteringSpec(UNCLUSTERED_SPEC_ID, Collections.emptyList(), Algorithm.HILBERT, 0, 0);

  private final int specId;
  private final ClusteringColumn[] columns;
  private final Algorithm algorithm;
  private final long targetZCubeSizeBytes;
  private final long minZCubeSizeBytes;

  private transient volatile List<ClusteringColumn> columnList;

  private ClusteringSpec(
      int specId,
      List<ClusteringColumn> columns,
      Algorithm algorithm,
      long targetZCubeSizeBytes,
      long minZCubeSizeBytes) {
    this.specId = specId;
    this.columns = columns.toArray(new ClusteringColumn[0]);
    this.algorithm = algorithm;
    this.targetZCubeSizeBytes = targetZCubeSizeBytes;
    this.minZCubeSizeBytes = minZCubeSizeBytes;
  }

  /**
   * Returns an unclustered spec.
   *
   * @return the unclustered clustering spec
   */
  public static ClusteringSpec unclustered() {
    return UNCLUSTERED;
  }

  /**
   * Creates a new builder for the given schema.
   *
   * @param schema the table schema
   * @return a new clustering spec builder
   */
  public static Builder builderFor(Schema schema) {
    return new Builder(schema);
  }

  /**
   * Creates a ClusteringSpec from table properties.
   *
   * @param schema the table schema
   * @param properties the table properties
   * @return the clustering spec, or unclustered if not configured
   */
  public static ClusteringSpec fromProperties(Schema schema, java.util.Map<String, String> properties) {
    String enabledStr = properties.get("clustering.enabled");
    if (enabledStr == null || !Boolean.parseBoolean(enabledStr)) {
      return unclustered();
    }

    String columnsStr = properties.get("clustering.columns");
    if (columnsStr == null || columnsStr.isEmpty()) {
      return unclustered();
    }

    Builder builder = builderFor(schema);

    String[] columns = columnsStr.split(",");
    for (String column : columns) {
      builder.clusterBy(column.trim());
    }

    String algorithm = properties.get("clustering.algorithm");
    if (algorithm != null && !algorithm.isEmpty()) {
      builder.algorithm(algorithm);
    }

    String targetSizeStr = properties.get("clustering.target-zcube-size-bytes");
    if (targetSizeStr != null) {
      builder.targetZCubeSize(Long.parseLong(targetSizeStr));
    }

    String minSizeStr = properties.get("clustering.min-zcube-size-bytes");
    if (minSizeStr != null) {
      builder.minZCubeSize(Long.parseLong(minSizeStr));
    }

    return builder.build();
  }

  /**
   * Returns the spec ID.
   *
   * @return the clustering spec ID
   */
  public int specId() {
    return specId;
  }

  /**
   * Returns the list of clustering columns.
   *
   * @return the clustering columns
   */
  public List<ClusteringColumn> columns() {
    return lazyColumnList();
  }

  /**
   * Returns the column names.
   *
   * @return list of column names
   */
  public List<String> columnNames() {
    return columns().stream().map(ClusteringColumn::name).collect(Collectors.toList());
  }

  /**
   * Returns the clustering algorithm.
   *
   * @return the clustering algorithm
   */
  public Algorithm algorithm() {
    return algorithm;
  }

  /**
   * Returns the target ZCube size in bytes.
   *
   * @return the target ZCube size
   */
  public long targetZCubeSizeBytes() {
    return targetZCubeSizeBytes;
  }

  /**
   * Returns the minimum ZCube size in bytes.
   *
   * @return the minimum ZCube size
   */
  public long minZCubeSizeBytes() {
    return minZCubeSizeBytes;
  }

  /**
   * Returns true if the spec has clustering enabled.
   *
   * @return true if clustering is enabled
   */
  public boolean isClustered() {
    return columns.length > 0;
  }

  /**
   * Returns true if the spec has no clustering.
   *
   * @return true if unclustered
   */
  public boolean isUnclustered() {
    return columns.length == 0;
  }

  /**
   * Checks if this spec has the same columns as another spec.
   *
   * @param other another clustering spec
   * @return true if the columns are the same
   */
  public boolean sameColumns(ClusteringSpec other) {
    return Arrays.equals(columns, other.columns);
  }

  private List<ClusteringColumn> lazyColumnList() {
    if (columnList == null) {
      synchronized (this) {
        if (columnList == null) {
          this.columnList = ImmutableList.copyOf(columns);
        }
      }
    }
    return columnList;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof ClusteringSpec)) {
      return false;
    }

    ClusteringSpec that = (ClusteringSpec) other;
    return specId == that.specId
        && Arrays.equals(columns, that.columns)
        && algorithm == that.algorithm
        && targetZCubeSizeBytes == that.targetZCubeSizeBytes
        && minZCubeSizeBytes == that.minZCubeSizeBytes;
  }

  @Override
  public int hashCode() {
    int result = Integer.hashCode(specId);
    result = 31 * result + Arrays.hashCode(columns);
    result = 31 * result + algorithm.hashCode();
    result = 31 * result + Long.hashCode(targetZCubeSizeBytes);
    result = 31 * result + Long.hashCode(minZCubeSizeBytes);
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ClusteringSpec[");
    sb.append("id=").append(specId);
    sb.append(", columns=[");
    for (int i = 0; i < columns.length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(columns[i].name());
    }
    sb.append("]");
    sb.append(", algorithm=").append(algorithm.algorithmName());
    sb.append("]");
    return sb.toString();
  }

  /** Builder for creating ClusteringSpec instances. */
  public static class Builder {
    private final Schema schema;
    private final List<ClusteringColumn> columns = Lists.newArrayList();
    private Integer specId = null;
    private Algorithm algorithm = DEFAULT_ALGORITHM;
    private long targetZCubeSizeBytes = DEFAULT_TARGET_ZCUBE_SIZE_BYTES;
    private long minZCubeSizeBytes = DEFAULT_MIN_ZCUBE_SIZE_BYTES;

    private Builder(Schema schema) {
      this.schema = schema;
    }

    /**
     * Sets the spec ID.
     *
     * @param newSpecId the spec ID
     * @return this builder
     */
    public Builder withSpecId(int newSpecId) {
      this.specId = newSpecId;
      return this;
    }

    /**
     * Adds a clustering column by name.
     *
     * @param columnName the column name
     * @return this builder
     */
    public Builder clusterBy(String columnName) {
      Preconditions.checkArgument(columnName != null, "Column name cannot be null");

      Types.NestedField field = schema.findField(columnName);
      ValidationException.check(
          field != null, "Cannot find column '%s' in schema: %s", columnName, schema);

      ValidationException.check(
          field.type().isPrimitiveType(),
          "Clustering column must be a primitive type: %s is %s",
          columnName,
          field.type());

      columns.add(new ClusteringColumn(field.fieldId(), columnName));
      return this;
    }

    /**
     * Adds multiple clustering columns by name.
     *
     * @param columnNames the column names
     * @return this builder
     */
    public Builder clusterBy(String... columnNames) {
      for (String name : columnNames) {
        clusterBy(name);
      }
      return this;
    }

    /**
     * Sets the clustering algorithm.
     *
     * @param newAlgorithm the algorithm
     * @return this builder
     */
    public Builder algorithm(Algorithm newAlgorithm) {
      this.algorithm = newAlgorithm;
      return this;
    }

    /**
     * Sets the clustering algorithm by name.
     *
     * @param algorithmName the algorithm name
     * @return this builder
     */
    public Builder algorithm(String algorithmName) {
      this.algorithm = Algorithm.fromName(algorithmName);
      return this;
    }

    /**
     * Sets the target ZCube size.
     *
     * @param sizeBytes the target size in bytes
     * @return this builder
     */
    public Builder targetZCubeSize(long sizeBytes) {
      Preconditions.checkArgument(sizeBytes > 0, "Target ZCube size must be positive: %s", sizeBytes);
      this.targetZCubeSizeBytes = sizeBytes;
      return this;
    }

    /**
     * Sets the minimum ZCube size.
     *
     * @param sizeBytes the minimum size in bytes
     * @return this builder
     */
    public Builder minZCubeSize(long sizeBytes) {
      Preconditions.checkArgument(sizeBytes > 0, "Minimum ZCube size must be positive: %s", sizeBytes);
      this.minZCubeSizeBytes = sizeBytes;
      return this;
    }

    /**
     * Adds a pre-constructed clustering column.
     *
     * <p>This is used during deserialization when schema validation is deferred.
     *
     * @param column the clustering column
     * @return this builder
     */
    Builder addColumn(ClusteringColumn column) {
      columns.add(column);
      return this;
    }

    /**
     * Builds the clustering spec.
     *
     * @return the built clustering spec
     */
    public ClusteringSpec build() {
      if (columns.isEmpty()) {
        return ClusteringSpec.unclustered();
      }

      ValidationException.check(
          columns.size() <= MAX_CLUSTERING_COLUMNS,
          "Cannot have more than %s clustering columns, got %s",
          MAX_CLUSTERING_COLUMNS,
          columns.size());

      ValidationException.check(
          minZCubeSizeBytes <= targetZCubeSizeBytes,
          "Minimum ZCube size (%s) must be <= target ZCube size (%s)",
          minZCubeSizeBytes,
          targetZCubeSizeBytes);

      int actualSpecId = specId != null ? specId : 1;
      return new ClusteringSpec(
          actualSpecId, columns, algorithm, targetZCubeSizeBytes, minZCubeSizeBytes);
    }

    /**
     * Builds the clustering spec without schema validation.
     *
     * <p>This is used during deserialization when the schema may have changed.
     *
     * @return the built clustering spec
     */
    ClusteringSpec buildUnchecked() {
      if (columns.isEmpty()) {
        return ClusteringSpec.unclustered();
      }

      int actualSpecId = specId != null ? specId : 1;
      return new ClusteringSpec(
          actualSpecId, columns, algorithm, targetZCubeSizeBytes, minZCubeSizeBytes);
    }
  }
}
