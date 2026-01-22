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
package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.Attribute

/**
 * Logical plan for ALTER TABLE ... CLUSTER BY command.
 *
 * This command sets or modifies the clustering columns for a table's Liquid Clustering
 * configuration. The clustering columns determine how data is organized during OPTIMIZE
 * operations.
 *
 * @param table The multi-part identifier of the table
 * @param columns The column names to use for clustering, or empty for CLUSTER BY NONE
 * @param options Optional clustering configuration (algorithm, target size, etc.)
 */
case class SetClusteringColumns(
    table: Seq[String],
    columns: Seq[String],
    options: ClusteringOptions) extends LeafCommand {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override lazy val output: Seq[Attribute] = Nil

  override def simpleString(maxFields: Int): String = {
    if (columns.isEmpty) {
      s"SetClusteringColumns ${table.quoted} NONE"
    } else {
      s"SetClusteringColumns ${table.quoted} (${columns.quoted})"
    }
  }
}

/**
 * Options for clustering configuration.
 *
 * @param algorithm The clustering algorithm (e.g., "hilbert", "zorder")
 * @param targetSizeBytes Target ZCube size in bytes
 * @param minSizeBytes Minimum ZCube size in bytes
 */
case class ClusteringOptions(
    algorithm: Option[String] = None,
    targetSizeBytes: Option[Long] = None,
    minSizeBytes: Option[Long] = None)

object ClusteringOptions {
  val empty: ClusteringOptions = ClusteringOptions()
}
