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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.iceberg.TableProperties
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import scala.jdk.CollectionConverters._

/**
 * Physical plan node for executing OPTIMIZE TABLE command.
 *
 * This execution node triggers Liquid Clustering optimization on a table,
 * reorganizing data files using the table's configured clustering columns
 * and algorithm (default: Hilbert curve).
 */
case class OptimizeTableExec(
    catalog: TableCatalog,
    ident: Identifier,
    isFull: Boolean)
    extends LeafV2CommandExec {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override lazy val output: Seq[Attribute] = {
    val schema = StructType(Seq(
      StructField("rewritten_data_files_count", IntegerType, nullable = false),
      StructField("added_data_files_count", IntegerType, nullable = false),
      StructField("rewritten_bytes", LongType, nullable = false),
      StructField("failed_data_files_count", IntegerType, nullable = false)
    ))
    schema.toAttributes
  }

  override protected def run(): Seq[InternalRow] = {
    catalog.loadTable(ident) match {
      case iceberg: SparkTable =>
        val table = iceberg.table
        val props = table.properties()

        // Check if clustering is enabled
        val clusteringEnabled = Option(props.get(TableProperties.CLUSTERING_ENABLED))
          .map(_.toBoolean)
          .getOrElse(false)

        if (!clusteringEnabled) {
          throw new UnsupportedOperationException(
            s"Table ${ident.quoted} does not have clustering enabled. " +
            "Use ALTER TABLE ... CLUSTER BY to enable clustering first.")
        }

        // Get clustering columns
        val clusteringColumns = Option(props.get(TableProperties.CLUSTERING_COLUMNS))
          .map(_.split(",").map(_.trim).toSeq)
          .getOrElse(Seq.empty)

        if (clusteringColumns.isEmpty) {
          throw new UnsupportedOperationException(
            s"Table ${ident.quoted} has no clustering columns defined. " +
            "Use ALTER TABLE ... CLUSTER BY (col1, col2, ...) to set clustering columns.")
        }

        // Get clustering algorithm (default: hilbert)
        val algorithm = Option(props.get(TableProperties.CLUSTERING_ALGORITHM))
          .getOrElse("hilbert")

        // Execute the clustering operation using SparkActions
        val actions = SparkActions.get(session)
        val rewriteAction = actions.rewriteDataFiles(table)

        // Apply clustering based on algorithm
        algorithm.toLowerCase match {
          case "hilbert" =>
            rewriteAction.hilbert(clusteringColumns: _*)
          case "zorder" | "z-order" =>
            rewriteAction.zOrder(clusteringColumns: _*)
          case _ =>
            // Default to hilbert for unknown algorithms
            rewriteAction.hilbert(clusteringColumns: _*)
        }

        // Execute and get results
        val result = rewriteAction.execute()

        // Aggregate results
        var rewrittenCount = 0
        var addedCount = 0
        var rewrittenBytes = 0L
        var failedCount = 0

        result.rewriteResults().asScala.foreach { groupResult =>
          rewrittenCount += groupResult.rewrittenDataFilesCount()
          addedCount += groupResult.addedDataFilesCount()
          rewrittenBytes += groupResult.rewrittenBytesCount()
        }

        failedCount = result.failedGroupCount()

        Seq(new GenericInternalRow(Array[Any](rewrittenCount, addedCount, rewrittenBytes, failedCount)))

      case table =>
        throw new UnsupportedOperationException(
          s"Cannot optimize non-Iceberg table: $table")
    }
  }

  override def simpleString(maxFields: Int): String = {
    val fullStr = if (isFull) " FULL" else ""
    s"OptimizeTable${fullStr} ${catalog.name}.${ident.quoted}"
  }
}
