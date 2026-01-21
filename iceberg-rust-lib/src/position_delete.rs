//! Position Delete File Handler
//!
//! Position delete文件格式：
//! - file_path: string - 数据文件的完整URI
//! - pos: long - 被删除行在数据文件中的位置（从0开始）
//! - row: struct (可选) - 被删除行的值

use crate::error::{IcebergError, Result};
use arrow::array::{Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Position delete information for a specific data file
#[derive(Debug, Clone)]
pub struct PositionDeletes {
    /// Set of deleted positions
    pub deleted_positions: HashSet<i64>,
}

impl PositionDeletes {
    /// Create a new empty PositionDeletes
    pub fn new() -> Self {
        Self {
            deleted_positions: HashSet::new(),
        }
    }

    /// Check if a position is deleted
    pub fn is_deleted(&self, pos: i64) -> bool {
        self.deleted_positions.contains(&pos)
    }

    /// Add a deleted position
    pub fn add_deleted(&mut self, pos: i64) {
        self.deleted_positions.insert(pos);
    }

    /// Get the number of deleted positions
    pub fn len(&self) -> usize {
        self.deleted_positions.len()
    }

    /// Check if there are no deletes
    pub fn is_empty(&self) -> bool {
        self.deleted_positions.is_empty()
    }
}

impl Default for PositionDeletes {
    fn default() -> Self {
        Self::new()
    }
}

/// Reader for position delete files
pub struct PositionDeleteReader {
    ctx: SessionContext,
}

impl Default for PositionDeleteReader {
    fn default() -> Self {
        Self::new()
    }
}

impl PositionDeleteReader {
    /// Create a new position delete reader
    pub fn new() -> Self {
        let config = SessionConfig::new()
            .with_batch_size(8192)
            .with_parquet_pruning(true);

        Self {
            ctx: SessionContext::new_with_config(config),
        }
    }

    /// Read position delete file and return a map of file_path -> deleted positions
    ///
    /// # Arguments
    /// * `delete_file_path` - Path to the position delete parquet file
    ///
    /// # Returns
    /// HashMap mapping data file paths to their deleted positions
    pub async fn read_delete_file(
        &self,
        delete_file_path: &str,
    ) -> Result<HashMap<String, PositionDeletes>> {
        let mut result: HashMap<String, PositionDeletes> = HashMap::new();

        let normalized_path = Self::normalize_path(delete_file_path)?;
        let table_url = ListingTableUrl::parse(&normalized_path)?;

        let file_format = ParquetFormat::default();
        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(".parquet");

        let schema = listing_options
            .infer_schema(&self.ctx.state(), &table_url)
            .await?;

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .with_schema(schema);

        let table = ListingTable::try_new(config)?;

        self.ctx.register_table("position_delete_temp", Arc::new(table))?;

        // 只读取file_path和pos列
        let df = self.ctx
            .table("position_delete_temp")
            .await?
            .select(vec![col("file_path"), col("pos")])?;

        let batches = df.collect().await?;

        for batch in batches {
            self.process_delete_batch(&batch, &mut result)?;
        }

        self.ctx.deregister_table("position_delete_temp")?;

        Ok(result)
    }

    /// Read position delete file for a specific data file
    ///
    /// Uses filter pushdown to only read relevant delete records
    pub async fn read_delete_file_for_data_file(
        &self,
        delete_file_path: &str,
        data_file_path: &str,
    ) -> Result<PositionDeletes> {
        let normalized_path = Self::normalize_path(delete_file_path)?;
        let table_url = ListingTableUrl::parse(&normalized_path)?;

        let file_format = ParquetFormat::default()
            .with_enable_pruning(true);

        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(".parquet");

        let schema = listing_options
            .infer_schema(&self.ctx.state(), &table_url)
            .await?;

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .with_schema(schema);

        let table = ListingTable::try_new(config)?;

        let table_name = format!("pos_del_{}", uuid::Uuid::new_v4().simple());
        self.ctx.register_table(&table_name, Arc::new(table))?;

        // 使用谓词下推过滤特定数据文件的删除记录
        let df = self.ctx
            .table(&table_name)
            .await?
            .filter(col("file_path").eq(lit(data_file_path)))?
            .select(vec![col("pos")])?;

        let batches = df.collect().await?;

        let mut deletes = PositionDeletes::new();
        for batch in batches {
            if let Some(pos_array) = batch.column_by_name("pos") {
                let pos_array = pos_array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| IcebergError::position_delete_error("pos column is not Int64"))?;

                for i in 0..pos_array.len() {
                    if !pos_array.is_null(i) {
                        deletes.add_deleted(pos_array.value(i));
                    }
                }
            }
        }

        self.ctx.deregister_table(&table_name)?;

        Ok(deletes)
    }

    /// Read multiple delete files and merge results
    pub async fn read_delete_files(
        &self,
        delete_file_paths: &[&str],
    ) -> Result<HashMap<String, PositionDeletes>> {
        let mut result: HashMap<String, PositionDeletes> = HashMap::new();

        for path in delete_file_paths {
            let file_deletes = self.read_delete_file(path).await?;
            for (file_path, deletes) in file_deletes {
                result
                    .entry(file_path)
                    .or_default()
                    .deleted_positions
                    .extend(deletes.deleted_positions);
            }
        }

        Ok(result)
    }

    /// Process a batch of position delete records
    fn process_delete_batch(
        &self,
        batch: &RecordBatch,
        result: &mut HashMap<String, PositionDeletes>,
    ) -> Result<()> {
        let file_path_col = batch
            .column_by_name("file_path")
            .ok_or_else(|| IcebergError::position_delete_error("file_path column not found"))?;

        let pos_col = batch
            .column_by_name("pos")
            .ok_or_else(|| IcebergError::position_delete_error("pos column not found"))?;

        let file_path_array = file_path_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                IcebergError::position_delete_error("file_path column is not String")
            })?;

        let pos_array = pos_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| IcebergError::position_delete_error("pos column is not Int64"))?;

        for i in 0..batch.num_rows() {
            if !file_path_array.is_null(i) && !pos_array.is_null(i) {
                let file_path = file_path_array.value(i).to_string();
                let pos = pos_array.value(i);

                result.entry(file_path).or_default().add_deleted(pos);
            }
        }

        Ok(())
    }

    /// Normalize file path
    fn normalize_path(path: &str) -> Result<String> {
        if path.starts_with("s3://")
            || path.starts_with("gs://")
            || path.starts_with("az://")
            || path.starts_with("abfs://")
            || path.starts_with("hdfs://")
            || path.starts_with("file://")
        {
            Ok(path.to_string())
        } else {
            let abs_path = if std::path::Path::new(path).is_absolute() {
                path.to_string()
            } else {
                std::env::current_dir()?
                    .join(path)
                    .to_string_lossy()
                    .to_string()
            };
            Ok(format!("file://{}", abs_path.replace('\\', "/")))
        }
    }
}

/// Filter RecordBatches using position deletes
pub fn filter_batch_with_position_deletes(
    batch: &RecordBatch,
    deletes: &PositionDeletes,
    start_position: i64,
) -> Result<RecordBatch> {
    use arrow::array::BooleanArray;
    use arrow::compute::filter_record_batch;

    let num_rows = batch.num_rows();
    let mut filter_array = Vec::with_capacity(num_rows);

    for i in 0..num_rows {
        let position = start_position + i as i64;
        filter_array.push(!deletes.is_deleted(position));
    }

    let filter = BooleanArray::from(filter_array);
    let filtered_batch = filter_record_batch(batch, &filter)?;

    Ok(filtered_batch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_position_deletes() {
        let mut deletes = PositionDeletes::new();
        deletes.add_deleted(1);
        deletes.add_deleted(5);
        deletes.add_deleted(10);

        assert!(deletes.is_deleted(1));
        assert!(deletes.is_deleted(5));
        assert!(deletes.is_deleted(10));
        assert!(!deletes.is_deleted(2));
        assert!(!deletes.is_deleted(100));
        assert_eq!(deletes.len(), 3);
    }
}
