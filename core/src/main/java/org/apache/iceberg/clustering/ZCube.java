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
package org.apache.iceberg.clustering;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Represents a ZCube - a logical clustering unit in liquid clustering.
 *
 * <p>A ZCube groups together data files that were clustered using the same clustering columns and
 * algorithm. Files within a ZCube share:
 *
 * <ul>
 *   <li>The same clustering provider (e.g., "liquid")
 *   <li>The same clustering columns
 *   <li>The same clustering spec ID
 * </ul>
 *
 * <p>ZCubes are used to track:
 *
 * <ul>
 *   <li>Which files have been clustered together
 *   <li>The total size and record count of clustered data
 *   <li>Whether a ZCube is complete (reached target size) or needs more data
 * </ul>
 */
public class ZCube implements Serializable {
  private final String zCubeId;
  private final List<String> clusteringColumns;
  private final int clusteringSpecId;
  private final List<DataFile> files;
  private final long totalSizeBytes;
  private final long recordCount;

  private ZCube(
      String zCubeId,
      List<String> clusteringColumns,
      int clusteringSpecId,
      List<DataFile> files,
      long totalSizeBytes,
      long recordCount) {
    this.zCubeId = zCubeId;
    this.clusteringColumns = ImmutableList.copyOf(clusteringColumns);
    this.clusteringSpecId = clusteringSpecId;
    this.files = ImmutableList.copyOf(files);
    this.totalSizeBytes = totalSizeBytes;
    this.recordCount = recordCount;
  }

  /**
   * Generates a new unique ZCube ID.
   *
   * @return a new UUID string for a ZCube
   */
  public static String newZCubeId() {
    return UUID.randomUUID().toString();
  }

  /**
   * Returns the unique ID of this ZCube.
   *
   * @return the ZCube ID
   */
  public String zCubeId() {
    return zCubeId;
  }

  /**
   * Returns the list of column names used for clustering.
   *
   * @return the clustering column names
   */
  public List<String> clusteringColumns() {
    return clusteringColumns;
  }

  /**
   * Returns the clustering spec ID when these files were clustered.
   *
   * @return the clustering spec ID
   */
  public int clusteringSpecId() {
    return clusteringSpecId;
  }

  /**
   * Returns the list of files in this ZCube.
   *
   * @return the data files
   */
  public List<DataFile> files() {
    return files;
  }

  /**
   * Returns the total size of all files in this ZCube.
   *
   * @return the total size in bytes
   */
  public long totalSizeBytes() {
    return totalSizeBytes;
  }

  /**
   * Returns the total record count across all files in this ZCube.
   *
   * @return the total record count
   */
  public long recordCount() {
    return recordCount;
  }

  /**
   * Checks if this ZCube has reached its target size.
   *
   * @param targetSizeBytes the target ZCube size
   * @return true if the ZCube size >= target size
   */
  public boolean isComplete(long targetSizeBytes) {
    return totalSizeBytes >= targetSizeBytes;
  }

  /**
   * Checks if this ZCube needs re-clustering.
   *
   * <p>A ZCube needs re-clustering if:
   *
   * <ul>
   *   <li>The clustering columns don't match the current spec
   *   <li>The ZCube is smaller than the minimum size threshold
   * </ul>
   *
   * @param currentColumns the current clustering columns
   * @param minSizeBytes the minimum ZCube size threshold
   * @return true if re-clustering is needed
   */
  public boolean needsReclustering(List<String> currentColumns, long minSizeBytes) {
    return !clusteringColumns.equals(currentColumns) || totalSizeBytes < minSizeBytes;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof ZCube)) {
      return false;
    }

    ZCube that = (ZCube) other;
    return Objects.equal(zCubeId, that.zCubeId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(zCubeId);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("zCubeId", zCubeId)
        .add("columns", clusteringColumns)
        .add("specId", clusteringSpecId)
        .add("files", files.size())
        .add("totalSizeBytes", totalSizeBytes)
        .add("recordCount", recordCount)
        .toString();
  }

  /**
   * Creates a new builder for constructing ZCube instances.
   *
   * @return a new ZCube builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for constructing ZCube instances. */
  public static class Builder {
    private String zCubeId;
    private List<String> clusteringColumns;
    private int clusteringSpecId;
    private final List<DataFile> files = Lists.newArrayList();
    private long totalSizeBytes = 0;
    private long recordCount = 0;

    private Builder() {}

    /**
     * Sets the ZCube ID. If not set, a new ID will be generated.
     *
     * @param id the ZCube ID
     * @return this builder
     */
    public Builder zCubeId(String id) {
      this.zCubeId = id;
      return this;
    }

    /**
     * Sets the clustering columns.
     *
     * @param columns the column names
     * @return this builder
     */
    public Builder clusteringColumns(List<String> columns) {
      this.clusteringColumns = columns;
      return this;
    }

    /**
     * Sets the clustering spec ID.
     *
     * @param specId the spec ID
     * @return this builder
     */
    public Builder clusteringSpecId(int specId) {
      this.clusteringSpecId = specId;
      return this;
    }

    /**
     * Adds a file to the ZCube.
     *
     * @param file the data file to add
     * @return this builder
     */
    public Builder addFile(DataFile file) {
      files.add(file);
      totalSizeBytes += file.fileSizeInBytes();
      recordCount += file.recordCount();
      return this;
    }

    /**
     * Adds multiple files to the ZCube.
     *
     * @param filesToAdd the data files to add
     * @return this builder
     */
    public Builder addFiles(Iterable<DataFile> filesToAdd) {
      for (DataFile file : filesToAdd) {
        addFile(file);
      }
      return this;
    }

    /**
     * Builds the ZCube instance.
     *
     * @return the built ZCube
     */
    public ZCube build() {
      Preconditions.checkState(
          clusteringColumns != null && !clusteringColumns.isEmpty(),
          "Clustering columns must be set");

      String actualZCubeId = zCubeId != null ? zCubeId : newZCubeId();

      return new ZCube(
          actualZCubeId, clusteringColumns, clusteringSpecId, files, totalSizeBytes, recordCount);
    }
  }
}
