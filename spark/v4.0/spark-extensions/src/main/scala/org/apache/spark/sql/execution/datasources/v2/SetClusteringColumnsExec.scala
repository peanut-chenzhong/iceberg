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
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.ClusteringOptions
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCatalog

/**
 * Physical plan node for executing ALTER TABLE ... CLUSTER BY command.
 *
 * This execution node sets or modifies the clustering columns for a table's
 * Liquid Clustering configuration.
 */
case class SetClusteringColumnsExec(
    catalog: TableCatalog,
    ident: Identifier,
    columns: Seq[String],
    options: ClusteringOptions)
    extends LeafV2CommandExec {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    catalog.loadTable(ident) match {
      case iceberg: SparkTable =>
        val table = iceberg.table
        val updates = table.updateProperties()

        if (columns.isEmpty) {
          // CLUSTER BY NONE - disable clustering
          updates.set(TableProperties.CLUSTERING_ENABLED, "false")
          updates.remove(TableProperties.CLUSTERING_COLUMNS)
        } else {
          // Set clustering columns
          updates.set(TableProperties.CLUSTERING_ENABLED, "true")
          updates.set(TableProperties.CLUSTERING_COLUMNS, columns.mkString(","))

          // Apply optional settings
          options.algorithm.foreach { algo =>
            updates.set(TableProperties.CLUSTERING_ALGORITHM, algo)
          }
          options.targetSizeBytes.foreach { size =>
            updates.set(TableProperties.CLUSTERING_TARGET_ZCUBE_SIZE_BYTES, size.toString)
          }
          options.minSizeBytes.foreach { size =>
            updates.set(TableProperties.CLUSTERING_MIN_ZCUBE_SIZE_BYTES, size.toString)
          }
        }

        updates.commit()

      case table =>
        throw new UnsupportedOperationException(
          s"Cannot set clustering columns in non-Iceberg table: $table")
    }

    Nil
  }

  override def simpleString(maxFields: Int): String = {
    if (columns.isEmpty) {
      s"SetClusteringColumns ${catalog.name}.${ident.quoted} NONE"
    } else {
      s"SetClusteringColumns ${catalog.name}.${ident.quoted} (${columns.quoted})"
    }
  }
}
