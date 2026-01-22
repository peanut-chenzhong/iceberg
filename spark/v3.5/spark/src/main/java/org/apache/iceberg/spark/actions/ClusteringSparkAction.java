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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.ClusteringSpec;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.Action;
import org.apache.iceberg.clustering.ClusteringFileSelector;
import org.apache.iceberg.clustering.ZCube;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark action for liquid clustering.
 *
 * <p>This action reorganizes data files using a space-filling curve (Hilbert or Z-Order) to improve
 * query performance through better data locality and file pruning.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ClusteringSparkAction action = new ClusteringSparkAction(spark, table);
 * ClusteringResult result = action
 *     .filter(Expressions.greaterThan("event_time", "2024-01-01"))
 *     .full(false)
 *     .execute();
 * }</pre>
 */
public class ClusteringSparkAction implements Action<ClusteringSparkAction, ClusteringSparkAction.Result> {

  private static final Logger LOG = LoggerFactory.getLogger(ClusteringSparkAction.class);

  /** Default variable type size for string/binary columns in bytes. */
  private static final int DEFAULT_VAR_LENGTH_SIZE = 16;

  /** Maximum output size for clustering key in bytes. */
  private static final int MAX_OUTPUT_SIZE = 128;

  /** Default number of parallel clustering jobs. */
  private static final int DEFAULT_MAX_CONCURRENT_JOBS = 4;

  private final SparkSession spark;
  private final Table table;
  private final ClusteringSpec clusteringSpec;

  private boolean isFull = false;
  private Expression filter = Expressions.alwaysTrue();
  private int maxConcurrentJobs = DEFAULT_MAX_CONCURRENT_JOBS;
  private long targetFileSizeBytes = -1;

  /**
   * Creates a new clustering action.
   *
   * @param spark the Spark session
   * @param table the Iceberg table
   */
  public ClusteringSparkAction(SparkSession spark, Table table) {
    this.spark = spark;
    this.table = table;
    this.clusteringSpec = table.clusteringSpec();

    Preconditions.checkArgument(
        clusteringSpec != null && clusteringSpec.isClustered(),
        "Table %s does not have clustering enabled. Use ALTER TABLE ... CLUSTER BY to enable clustering.",
        table.name());
  }

  /**
   * Sets whether to perform full re-clustering.
   *
   * <p>In full mode, all files are re-clustered regardless of their current clustering state. In
   * incremental mode (default), only unclustered files and files in incomplete ZCubes are
   * processed.
   *
   * @return this action for chaining
   */
  public ClusteringSparkAction full() {
    return full(true);
  }

  /**
   * Sets whether to perform full re-clustering.
   *
   * @param fullMode true for full re-clustering, false for incremental
   * @return this action for chaining
   */
  public ClusteringSparkAction full(boolean fullMode) {
    this.isFull = fullMode;
    return this;
  }

  /**
   * Sets a filter expression to limit which files are considered for clustering.
   *
   * @param filterExpr the filter expression
   * @return this action for chaining
   */
  public ClusteringSparkAction filter(Expression filterExpr) {
    this.filter = filterExpr;
    return this;
  }

  /**
   * Sets the maximum number of concurrent clustering jobs.
   *
   * @param maxJobs the maximum number of concurrent jobs
   * @return this action for chaining
   */
  public ClusteringSparkAction maxConcurrentJobs(int maxJobs) {
    Preconditions.checkArgument(maxJobs > 0, "Max concurrent jobs must be positive");
    this.maxConcurrentJobs = maxJobs;
    return this;
  }

  /**
   * Sets the target file size for output files.
   *
   * @param sizeBytes the target file size in bytes
   * @return this action for chaining
   */
  public ClusteringSparkAction targetFileSizeBytes(long sizeBytes) {
    Preconditions.checkArgument(sizeBytes > 0, "Target file size must be positive");
    this.targetFileSizeBytes = sizeBytes;
    return this;
  }

  @Override
  public Result execute() {
    LOG.info("Starting liquid clustering for table {} (full={})", table.name(), isFull);

    // Get target file size from table properties if not set
    if (targetFileSizeBytes <= 0) {
      targetFileSizeBytes =
          table.properties().containsKey(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES)
              ? Long.parseLong(table.properties().get(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES))
              : TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;
    }

    // Select files for clustering
    ClusteringFileSelector selector = new ClusteringFileSelector(clusteringSpec);
    List<DataFile> candidates =
        selector.selectFilesForClustering(table.newScan().filter(filter), isFull);

    if (candidates.isEmpty()) {
      LOG.info("No files need clustering");
      return Result.empty();
    }

    LOG.info("Selected {} files for clustering", candidates.size());

    // Group files into bins
    List<List<DataFile>> bins =
        selector.groupFilesIntoBins(candidates, clusteringSpec.targetZCubeSizeBytes());

    LOG.info("Grouped files into {} bins", bins.size());

    // Execute clustering jobs
    List<JobResult> jobResults = Lists.newArrayList();
    for (int i = 0; i < bins.size(); i++) {
      List<DataFile> bin = bins.get(i);
      LOG.info("Processing bin {}/{} with {} files", i + 1, bins.size(), bin.size());

      try {
        JobResult result = executeClusteringJob(bin, i);
        jobResults.add(result);
      } catch (Exception e) {
        LOG.error("Failed to process bin {}", i, e);
        jobResults.add(JobResult.failed(bin, e));
      }
    }

    // Commit all changes
    commitChanges(jobResults);

    return buildResult(jobResults);
  }

  /**
   * Executes a single clustering job for a bin of files.
   *
   * @param files the files to cluster
   * @param binIndex the bin index for logging
   * @return the job result
   */
  private JobResult executeClusteringJob(List<DataFile> files, int binIndex) {
    String newZCubeId = ZCube.newZCubeId();
    List<String> clusteringColumns = clusteringSpec.columnNames();

    LOG.info("Clustering bin {} into ZCube {} using columns {}", binIndex, newZCubeId, clusteringColumns);

    // Read files into DataFrame
    Dataset<Row> df = readFiles(files);

    // Apply clustering
    Dataset<Row> clusteredDf = applyClustering(df, clusteringColumns);

    // Calculate target number of files
    long totalSize = files.stream().mapToLong(DataFile::fileSizeInBytes).sum();
    int targetNumFiles = Math.max(1, (int) Math.ceil((double) totalSize / targetFileSizeBytes));

    // Repartition and sort
    clusteredDf = clusteredDf.repartition(targetNumFiles);

    // Write new files
    List<DataFile> newFiles =
        writeFiles(clusteredDf, newZCubeId, clusteringSpec.specId(), clusteringColumns);

    return new JobResult(files, newFiles, newZCubeId);
  }

  /**
   * Reads files into a DataFrame.
   *
   * @param files the files to read
   * @return the DataFrame containing file data
   */
  private Dataset<Row> readFiles(List<DataFile> files) {
    List<String> paths = files.stream().map(DataFile::location).collect(Collectors.toList());

    String format = files.get(0).format().name().toLowerCase();
    return spark.read().format(format).load(paths.toArray(new String[0]));
  }

  /**
   * Applies clustering transformation to the DataFrame.
   *
   * @param df the input DataFrame
   * @param clusteringColumns the columns to cluster by
   * @return the clustered DataFrame
   */
  private Dataset<Row> applyClustering(Dataset<Row> df, List<String> clusteringColumns) {
    StructType schema = df.schema();
    int numCols = clusteringColumns.size();

    // Create UDF based on algorithm
    boolean useHilbert =
        clusteringSpec.algorithm() == ClusteringSpec.Algorithm.HILBERT && numCols >= 2;

    if (useHilbert) {
      return applyHilbertClustering(df, clusteringColumns, schema);
    } else {
      return applyZOrderClustering(df, clusteringColumns, schema);
    }
  }

  /**
   * Applies Hilbert curve clustering.
   */
  private Dataset<Row> applyHilbertClustering(
      Dataset<Row> df, List<String> clusteringColumns, StructType schema) {
    SparkHilbertUDF udf = new SparkHilbertUDF(clusteringColumns.size(), DEFAULT_VAR_LENGTH_SIZE, MAX_OUTPUT_SIZE);

    // Convert columns to ordered bytes
    List<Column> orderedByteCols = Lists.newArrayList();
    for (String colName : clusteringColumns) {
      StructField field = schema.apply(colName);
      DataType dataType = field.dataType();
      Column col = functions.col(colName);
      orderedByteCols.add(udf.sortedLexicographically(col, dataType));
    }

    // Create array of bytes and compute Hilbert index
    Column bytesArray = functions.array(orderedByteCols.toArray(new Column[0]));
    Column hilbertKey = udf.hilbertIndex(bytesArray);

    return df.withColumn("_clustering_key", hilbertKey)
        .sortWithinPartitions("_clustering_key")
        .drop("_clustering_key");
  }

  /**
   * Applies Z-Order clustering (fallback for single column or explicit Z-Order).
   */
  private Dataset<Row> applyZOrderClustering(
      Dataset<Row> df, List<String> clusteringColumns, StructType schema) {
    SparkZOrderUDF udf = new SparkZOrderUDF(clusteringColumns.size(), DEFAULT_VAR_LENGTH_SIZE, MAX_OUTPUT_SIZE);

    // Convert columns to ordered bytes
    List<Column> orderedByteCols = Lists.newArrayList();
    for (String colName : clusteringColumns) {
      StructField field = schema.apply(colName);
      DataType dataType = field.dataType();
      Column col = functions.col(colName);
      orderedByteCols.add(udf.sortedLexicographically(col, dataType));
    }

    // Create array of bytes and interleave
    Column bytesArray = functions.array(orderedByteCols.toArray(new Column[0]));
    Column zOrderKey = udf.interleaveBytes(bytesArray);

    return df.withColumn("_clustering_key", zOrderKey)
        .sortWithinPartitions("_clustering_key")
        .drop("_clustering_key");
  }

  /**
   * Writes clustered data to new files.
   */
  private List<DataFile> writeFiles(
      Dataset<Row> df, String zCubeId, int specId, List<String> clusteringColumns) {
    // TODO: Implement actual file writing with clustering metadata
    // This is a placeholder - actual implementation would use Iceberg's DataWriter
    // with clustering metadata attached to each file
    LOG.info("Writing clustered data to new files with ZCube ID: {}", zCubeId);

    // For now, return empty list - actual implementation needed
    return Lists.newArrayList();
  }

  /**
   * Commits all changes from clustering jobs.
   */
  private void commitChanges(List<JobResult> jobResults) {
    List<DataFile> filesToDelete = Lists.newArrayList();
    List<DataFile> filesToAdd = Lists.newArrayList();

    for (JobResult result : jobResults) {
      if (result.isSuccessful()) {
        filesToDelete.addAll(result.inputFiles);
        filesToAdd.addAll(result.outputFiles);
      }
    }

    if (filesToDelete.isEmpty() && filesToAdd.isEmpty()) {
      LOG.info("No changes to commit");
      return;
    }

    LOG.info("Committing: delete {} files, add {} files", filesToDelete.size(), filesToAdd.size());

    RewriteFiles rewrite = table.newRewrite();
    rewrite.rewriteFiles(
        Sets.newHashSet(filesToDelete),
        Sets.newHashSet(filesToAdd));
    rewrite.commit();

    LOG.info("Clustering commit successful");
  }

  /**
   * Builds the final result from job results.
   */
  private Result buildResult(List<JobResult> jobResults) {
    int rewrittenFiles = 0;
    int addedFiles = 0;
    long rewrittenBytes = 0;
    int failedJobs = 0;

    for (JobResult result : jobResults) {
      if (result.isSuccessful()) {
        rewrittenFiles += result.inputFiles.size();
        addedFiles += result.outputFiles.size();
        rewrittenBytes +=
            result.inputFiles.stream().mapToLong(DataFile::fileSizeInBytes).sum();
      } else {
        failedJobs++;
      }
    }

    return new Result(rewrittenFiles, addedFiles, rewrittenBytes, failedJobs);
  }

  /** Result of a single clustering job. */
  private static class JobResult {
    final List<DataFile> inputFiles;
    final List<DataFile> outputFiles;
    final String zCubeId;
    final Exception error;

    JobResult(List<DataFile> inputFiles, List<DataFile> outputFiles, String zCubeId) {
      this.inputFiles = inputFiles;
      this.outputFiles = outputFiles;
      this.zCubeId = zCubeId;
      this.error = null;
    }

    private JobResult(List<DataFile> inputFiles, Exception error) {
      this.inputFiles = inputFiles;
      this.outputFiles = Lists.newArrayList();
      this.zCubeId = null;
      this.error = error;
    }

    static JobResult failed(List<DataFile> inputFiles, Exception error) {
      return new JobResult(inputFiles, error);
    }

    boolean isSuccessful() {
      return error == null;
    }
  }

  /** Result of the clustering action. */
  public static class Result {
    private final int rewrittenDataFilesCount;
    private final int addedDataFilesCount;
    private final long rewrittenBytesCount;
    private final int failedJobsCount;

    Result(int rewrittenFiles, int addedFiles, long rewrittenBytes, int failedJobs) {
      this.rewrittenDataFilesCount = rewrittenFiles;
      this.addedDataFilesCount = addedFiles;
      this.rewrittenBytesCount = rewrittenBytes;
      this.failedJobsCount = failedJobs;
    }

    static Result empty() {
      return new Result(0, 0, 0, 0);
    }

    public int rewrittenDataFilesCount() {
      return rewrittenDataFilesCount;
    }

    public int addedDataFilesCount() {
      return addedDataFilesCount;
    }

    public long rewrittenBytesCount() {
      return rewrittenBytesCount;
    }

    public int failedJobsCount() {
      return failedJobsCount;
    }

    @Override
    public String toString() {
      return String.format(
          "ClusteringResult{rewritten=%d, added=%d, bytes=%d, failed=%d}",
          rewrittenDataFilesCount, addedDataFilesCount, rewrittenBytesCount, failedJobsCount);
    }
  }
}
