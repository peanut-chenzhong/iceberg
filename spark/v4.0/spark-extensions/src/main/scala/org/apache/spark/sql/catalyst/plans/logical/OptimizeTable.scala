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
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
 * Logical plan for OPTIMIZE TABLE command.
 *
 * This command triggers Liquid Clustering optimization on a table. It reorganizes
 * data files using the table's configured clustering columns and algorithm.
 *
 * @param table The multi-part identifier of the table
 * @param isFull If true, performs full re-clustering; if false, incremental clustering
 */
case class OptimizeTable(
    table: Seq[String],
    isFull: Boolean) extends LeafCommand {
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

  override def simpleString(maxFields: Int): String = {
    val fullStr = if (isFull) " FULL" else ""
    s"OptimizeTable${fullStr} ${table.quoted}"
  }
}
