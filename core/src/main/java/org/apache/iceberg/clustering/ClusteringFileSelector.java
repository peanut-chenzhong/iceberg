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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.ClusteringSpec;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Selects files for clustering based on the clustering spec and file metadata.
 *
 * <p>The selection strategy considers:
 *
 * <ul>
 *   <li>Unclustered files (files without clustering metadata)
 *   <li>Files with different clustering columns than the current spec
 *   <li>Files in incomplete ZCubes (below minimum size threshold)
 *   <li>Files in complete ZCubes (excluded in incremental mode)
 * </ul>
 */
public class ClusteringFileSelector {
  private static final Logger LOG = LoggerFactory.getLogger(ClusteringFileSelector.class);

  /** The clustering provider identifier for liquid clustering. */
  public static final String LIQUID_PROVIDER = TableProperties.CLUSTERING_PROVIDER_LIQUID;

  private final ClusteringSpec spec;
  private final long minZCubeSizeBytes;

  /**
   * Creates a new file selector for the given clustering spec.
   *
   * @param spec the clustering spec
   */
  public ClusteringFileSelector(ClusteringSpec spec) {
    this(spec, spec.minZCubeSizeBytes());
  }

  /**
   * Creates a new file selector with custom min ZCube size.
   *
   * @param spec the clustering spec
   * @param minZCubeSizeBytes the minimum ZCube size threshold
   */
  public ClusteringFileSelector(ClusteringSpec spec, long minZCubeSizeBytes) {
    this.spec = spec;
    this.minZCubeSizeBytes = minZCubeSizeBytes;
  }

  /**
   * Selects files that need clustering from the given table scan.
   *
   * @param scan the table scan to select files from
   * @param isFull if true, select all files; if false, use incremental selection
   * @return list of files to cluster
   */
  public List<DataFile> selectFilesForClustering(TableScan scan, boolean isFull) {
    List<DataFile> candidates = Lists.newArrayList();
    Map<String, ZCube.Builder> zCubeBuilders = Maps.newHashMap();

    // Scan all files and group by ZCube ID
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        DataFile file = task.file();

        if (isFull) {
          // In FULL mode, select all files
          candidates.add(file);
        } else {
          // In incremental mode, use smart selection
          if (shouldIncludeFile(file)) {
            candidates.add(file);
          }
        }

        // Track ZCube membership for filtering
        String zCubeId = file.zCubeId();
        if (zCubeId != null) {
          ZCube.Builder builder =
              zCubeBuilders.computeIfAbsent(
                  zCubeId,
                  id ->
                      ZCube.builder()
                          .zCubeId(id)
                          .clusteringColumns(file.zCubeColumns())
                          .clusteringSpecId(
                              file.clusteringSpecId() != null ? file.clusteringSpecId() : 0));
          builder.addFile(file);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to scan files for clustering", e);
    }

    // In incremental mode, filter out files from complete ZCubes
    if (!isFull) {
      candidates = filterByZCubeCompleteness(candidates, zCubeBuilders);
    }

    LOG.info("Selected {} files for clustering (full={})", candidates.size(), isFull);
    return candidates;
  }

  /**
   * Determines if a file should be included in incremental clustering.
   *
   * @param file the data file to check
   * @return true if the file should be included
   */
  private boolean shouldIncludeFile(DataFile file) {
    // 1. Include unclustered files
    if (file.clusteringProvider() == null) {
      LOG.debug("Including unclustered file: {}", file.location());
      return true;
    }

    // 2. Exclude files from different providers (unless FULL mode)
    if (!LIQUID_PROVIDER.equals(file.clusteringProvider())) {
      LOG.debug(
          "Excluding file from different provider {}: {}", file.clusteringProvider(), file.location());
      return false;
    }

    // 3. Include files with different clustering columns
    List<String> currentColumns = spec.columnNames();
    List<String> fileColumns = file.zCubeColumns();
    if (fileColumns == null || !currentColumns.equals(fileColumns)) {
      LOG.debug(
          "Including file with different columns (current={}, file={}): {}",
          currentColumns,
          fileColumns,
          file.location());
      return true;
    }

    // 4. Include files from the same spec (they'll be filtered by ZCube completeness later)
    return true;
  }

  /**
   * Filters candidates by removing files from complete ZCubes.
   *
   * @param candidates the candidate files
   * @param zCubeBuilders the ZCube builders with accumulated file info
   * @return filtered list of files
   */
  private List<DataFile> filterByZCubeCompleteness(
      List<DataFile> candidates, Map<String, ZCube.Builder> zCubeBuilders) {

    // Build ZCubes and identify complete ones
    Set<String> completeZCubeIds = Sets.newHashSet();
    List<String> currentColumns = spec.columnNames();

    for (Map.Entry<String, ZCube.Builder> entry : zCubeBuilders.entrySet()) {
      ZCube zCube = entry.getValue().build();

      // A ZCube is complete if:
      // 1. It has the same columns as the current spec
      // 2. It has reached the minimum size threshold
      if (currentColumns.equals(zCube.clusteringColumns())
          && zCube.totalSizeBytes() >= minZCubeSizeBytes) {
        completeZCubeIds.add(zCube.zCubeId());
        LOG.debug(
            "ZCube {} is complete (size={}, min={})",
            zCube.zCubeId(),
            zCube.totalSizeBytes(),
            minZCubeSizeBytes);
      }
    }

    // Filter out files from complete ZCubes
    return candidates.stream()
        .filter(
            file -> {
              String zCubeId = file.zCubeId();
              if (zCubeId != null && completeZCubeIds.contains(zCubeId)) {
                LOG.debug(
                    "Excluding file from complete ZCube {}: {}", zCubeId, file.location());
                return false;
              }
              return true;
            })
        .collect(Collectors.toList());
  }

  /**
   * Groups files into bins for clustering jobs.
   *
   * <p>Each bin will become a separate clustering job. The bin size is approximately equal to the
   * target ZCube size.
   *
   * @param files the files to group
   * @param targetBinSizeBytes the target bin size
   * @return list of file groups (bins)
   */
  public List<List<DataFile>> groupFilesIntoBins(List<DataFile> files, long targetBinSizeBytes) {
    List<List<DataFile>> bins = Lists.newArrayList();
    List<DataFile> currentBin = Lists.newArrayList();
    long currentBinSize = 0;

    // Sort files by size for better bin packing
    List<DataFile> sortedFiles =
        files.stream()
            .sorted((f1, f2) -> Long.compare(f2.fileSizeInBytes(), f1.fileSizeInBytes()))
            .collect(Collectors.toList());

    for (DataFile file : sortedFiles) {
      long fileSize = file.fileSizeInBytes();

      // If adding this file would exceed target and bin is not empty, start a new bin
      if (currentBinSize + fileSize > targetBinSizeBytes && !currentBin.isEmpty()) {
        bins.add(currentBin);
        currentBin = Lists.newArrayList();
        currentBinSize = 0;
      }

      currentBin.add(file);
      currentBinSize += fileSize;
    }

    // Add the last bin if not empty
    if (!currentBin.isEmpty()) {
      bins.add(currentBin);
    }

    LOG.info("Grouped {} files into {} bins (target bin size={})", files.size(), bins.size(), targetBinSizeBytes);
    return bins;
  }
}
