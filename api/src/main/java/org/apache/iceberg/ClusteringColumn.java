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
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Represents a column used for liquid clustering.
 *
 * <p>A clustering column references a source column in the table schema by its field ID and name.
 */
public class ClusteringColumn implements Serializable {
  private final int sourceId;
  private final String name;

  /**
   * Creates a new clustering column.
   *
   * @param sourceId the field ID of the source column in the schema
   * @param name the name of the source column
   */
  public ClusteringColumn(int sourceId, String name) {
    Preconditions.checkArgument(sourceId > 0, "Source ID must be positive: %s", sourceId);
    Preconditions.checkArgument(
        name != null && !name.isEmpty(), "Column name cannot be null or empty");
    this.sourceId = sourceId;
    this.name = name;
  }

  /**
   * Returns the field ID of the source column in the schema.
   *
   * @return the source column field ID
   */
  public int sourceId() {
    return sourceId;
  }

  /**
   * Returns the name of the source column.
   *
   * @return the source column name
   */
  public String name() {
    return name;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof ClusteringColumn)) {
      return false;
    }

    ClusteringColumn that = (ClusteringColumn) other;
    return sourceId == that.sourceId && Objects.equal(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(sourceId, name);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("sourceId", sourceId)
        .add("name", name)
        .toString();
  }
}
