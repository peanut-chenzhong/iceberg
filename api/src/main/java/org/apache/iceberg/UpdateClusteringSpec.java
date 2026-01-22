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

/**
 * API for updating a table's clustering specification.
 *
 * <p>Apply returns the pending update with all changes applied. Commit makes the pending changes
 * permanent.
 */
public interface UpdateClusteringSpec extends PendingUpdate<ClusteringSpec> {

  /**
   * Sets the clustering columns.
   *
   * <p>This replaces any existing clustering columns with the specified columns.
   *
   * @param columns the column names to cluster by
   * @return this for method chaining
   */
  UpdateClusteringSpec clusterBy(String... columns);

  /**
   * Disables clustering by removing all clustering columns.
   *
   * @return this for method chaining
   */
  UpdateClusteringSpec clusterByNone();

  /**
   * Sets the clustering algorithm.
   *
   * @param algorithm the algorithm name (hilbert or zorder)
   * @return this for method chaining
   */
  UpdateClusteringSpec algorithm(String algorithm);

  /**
   * Sets the target ZCube size in bytes.
   *
   * @param sizeBytes the target ZCube size
   * @return this for method chaining
   */
  UpdateClusteringSpec targetZCubeSize(long sizeBytes);

  /**
   * Sets the minimum ZCube size in bytes.
   *
   * @param sizeBytes the minimum ZCube size
   * @return this for method chaining
   */
  UpdateClusteringSpec minZCubeSize(long sizeBytes);
}
