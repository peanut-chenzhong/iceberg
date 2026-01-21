//! Parquet Small File Merger
//!
//! 使用DataFusion实现高性能的小文件合并
//! 性能优先，同时控制内存使用

use crate::error::{IcebergError, Result};
use crate::writer::{ParquetWriter, WriteOptions};

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::execution::context::SessionContext;
use datafusion::execution::memory_pool::{FairSpillPool, MemoryPool};
use datafusion::logical_expr::SortExpr;
use datafusion::prelude::*;
use futures::StreamExt;
use std::path::Path;
use std::sync::Arc;

/// Merge options for small file merging
#[derive(Debug, Clone)]
pub struct MergeOptions {
    /// Target file size in bytes (default: 128MB)
    pub target_file_size: usize,
    /// Maximum memory usage in bytes (default: 1GB)
    pub max_memory: usize,
    /// Output file compression
    pub compression: crate::writer::CompressionType,
    /// Row group size for output files
    pub row_group_size: usize,
    /// Whether to sort the output by certain columns
    pub sort_columns: Option<Vec<String>>,
    /// Batch size for reading
    pub batch_size: usize,
    /// Number of partitions for parallel processing
    pub partitions: usize,
    /// Enable statistics for output files
    pub enable_statistics: bool,
    /// Enable bloom filter for output files
    pub enable_bloom_filter: bool,
}

impl Default for MergeOptions {
    fn default() -> Self {
        Self {
            target_file_size: 128 * 1024 * 1024, // 128MB
            max_memory: 1024 * 1024 * 1024,      // 1GB
            compression: crate::writer::CompressionType::Zstd,
            row_group_size: 1024 * 1024, // 1M rows
            sort_columns: None,
            batch_size: 8192,
            partitions: num_cpus::get(),
            enable_statistics: true,
            enable_bloom_filter: false,
        }
    }
}

impl MergeOptions {
    /// Create new merge options with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set target file size
    pub fn with_target_file_size(mut self, size: usize) -> Self {
        self.target_file_size = size;
        self
    }

    /// Set maximum memory usage
    pub fn with_max_memory(mut self, size: usize) -> Self {
        self.max_memory = size;
        self
    }

    /// Set compression type
    pub fn with_compression(mut self, compression: crate::writer::CompressionType) -> Self {
        self.compression = compression;
        self
    }

    /// Set row group size
    pub fn with_row_group_size(mut self, size: usize) -> Self {
        self.row_group_size = size;
        self
    }

    /// Set sort columns
    pub fn with_sort_columns(mut self, columns: Vec<String>) -> Self {
        self.sort_columns = Some(columns);
        self
    }

    /// Set batch size
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set number of partitions
    pub fn with_partitions(mut self, partitions: usize) -> Self {
        self.partitions = partitions;
        self
    }

    /// Enable bloom filter
    pub fn with_bloom_filter(mut self, enabled: bool) -> Self {
        self.enable_bloom_filter = enabled;
        self
    }
}

/// Small file merger using DataFusion
pub struct FileMerger {
    ctx: SessionContext,
    options: MergeOptions,
}

impl FileMerger {
    /// Create a new FileMerger
    pub fn new(options: MergeOptions) -> Self {
        // 创建带内存限制的session context
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(options.max_memory));
        
        let runtime_env = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
            .with_memory_pool(memory_pool)
            .build()
            .expect("Failed to create runtime environment");

        let session_config = SessionConfig::new()
            .with_batch_size(options.batch_size)
            .with_target_partitions(options.partitions)
            .with_parquet_pruning(true);

        let state = datafusion::execution::session_state::SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(Arc::new(runtime_env))
            .build();

        Self {
            ctx: SessionContext::new_with_state(state),
            options,
        }
    }

    /// Merge multiple parquet files into a single file
    ///
    /// # Arguments
    /// * `input_files` - List of input file paths
    /// * `output_path` - Output file path
    ///
    /// # Returns
    /// The output file path
    ///
    /// # Example
    /// ```ignore
    /// use iceberg_rust_lib::{FileMerger, MergeOptions};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let options = MergeOptions::new()
    ///         .with_target_file_size(256 * 1024 * 1024)
    ///         .with_max_memory(2 * 1024 * 1024 * 1024);
    ///     
    ///     let merger = FileMerger::new(options);
    ///     let output = merger.merge_to_single_file(
    ///         &["file1.parquet", "file2.parquet", "file3.parquet"],
    ///         "merged.parquet"
    ///     ).await.unwrap();
    /// }
    /// ```
    pub async fn merge_to_single_file(
        &self,
        input_files: &[&str],
        output_path: &str,
    ) -> Result<String> {
        if input_files.is_empty() {
            return Err(IcebergError::InvalidArgument("No input files provided".to_string()));
        }

        // 创建union DataFrame from all input files
        let df = self.create_union_dataframe(input_files).await?;

        // 如果需要排序
        let df = if let Some(ref sort_cols) = self.options.sort_columns {
            let sort_exprs: Vec<SortExpr> = sort_cols
                .iter()
                .map(|c| col(c).sort(true, true))
                .collect();
            df.sort(sort_exprs)?
        } else {
            df
        };

        // 写入输出文件
        let write_options = self.create_write_options();
        let writer = ParquetWriter::with_context(self.ctx.clone());
        
        let normalized_output = Self::normalize_path(output_path)?;
        writer.write_dataframe(df, &normalized_output, write_options).await?;

        Ok(output_path.to_string())
    }

    /// Merge multiple parquet files with automatic file splitting
    ///
    /// 根据target_file_size自动分割输出文件
    ///
    /// # Arguments
    /// * `input_files` - List of input file paths
    /// * `output_dir` - Output directory path
    /// * `file_prefix` - Prefix for output file names
    ///
    /// # Returns
    /// Vector of output file paths
    pub async fn merge_with_splitting(
        &self,
        input_files: &[&str],
        output_dir: &str,
        file_prefix: &str,
    ) -> Result<Vec<String>> {
        if input_files.is_empty() {
            return Err(IcebergError::InvalidArgument("No input files provided".to_string()));
        }

        // 确保输出目录存在
        std::fs::create_dir_all(output_dir)?;

        // 创建union DataFrame
        let df = self.create_union_dataframe(input_files).await?;

        // 如果需要排序
        let df = if let Some(ref sort_cols) = self.options.sort_columns {
            let sort_exprs: Vec<SortExpr> = sort_cols
                .iter()
                .map(|c| col(c).sort(true, true))
                .collect();
            df.sort(sort_exprs)?
        } else {
            df
        };

        // 流式处理，按目标文件大小分割
        let mut stream = df.execute_stream().await?;
        let mut current_batches: Vec<RecordBatch> = Vec::new();
        let mut current_size: usize = 0;
        let mut file_index = 0;
        let mut output_files = Vec::new();
        let write_options = self.create_write_options();
        let writer = ParquetWriter::with_context(self.ctx.clone());

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            let batch_size = Self::estimate_batch_size(&batch);

            current_batches.push(batch);
            current_size += batch_size;

            // 如果当前大小超过目标，写入文件
            if current_size >= self.options.target_file_size {
                let file_name = format!("{}-{:05}.parquet", file_prefix, file_index);
                let file_path = Path::new(output_dir).join(&file_name);
                let file_path_str = file_path.to_string_lossy().to_string();

                writer.write(&current_batches, &file_path_str, write_options.clone()).await?;
                output_files.push(file_path_str);

                current_batches.clear();
                current_size = 0;
                file_index += 1;
            }
        }

        // 写入剩余数据
        if !current_batches.is_empty() {
            let file_name = format!("{}-{:05}.parquet", file_prefix, file_index);
            let file_path = Path::new(output_dir).join(&file_name);
            let file_path_str = file_path.to_string_lossy().to_string();

            writer.write(&current_batches, &file_path_str, write_options).await?;
            output_files.push(file_path_str);
        }

        Ok(output_files)
    }

    /// Merge files with memory-efficient streaming
    ///
    /// 使用流式处理最小化内存使用
    pub async fn merge_streaming(
        &self,
        input_files: &[&str],
        output_path: &str,
    ) -> Result<String> {
        if input_files.is_empty() {
            return Err(IcebergError::InvalidArgument("No input files provided".to_string()));
        }

        let df = self.create_union_dataframe(input_files).await?;
        let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());

        let df = if let Some(ref sort_cols) = self.options.sort_columns {
            let sort_exprs: Vec<SortExpr> = sort_cols
                .iter()
                .map(|c| col(c).sort(true, true))
                .collect();
            df.sort(sort_exprs)?
        } else {
            df
        };

        let stream = df.execute_stream().await?;
        let mapped_stream = stream.map(|r| r.map_err(IcebergError::DataFusion));

        let writer = ParquetWriter::with_context(self.ctx.clone());
        let write_options = self.create_write_options();

        writer.write_stream(mapped_stream, schema, output_path, write_options).await?;

        Ok(output_path.to_string())
    }

    /// Get statistics about input files before merging
    pub async fn get_merge_stats(&self, input_files: &[&str]) -> Result<MergeStats> {
        let mut total_rows = 0u64;
        let mut total_size = 0u64;

        for file_path in input_files {
            let normalized = Self::normalize_path(file_path)?;
            let table_url = ListingTableUrl::parse(&normalized)?;

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
            let table_name = format!("stats_{}", uuid::Uuid::new_v4().simple());
            self.ctx.register_table(&table_name, Arc::new(table))?;

            let df = self.ctx.table(&table_name).await?;
            let count = df.count().await?;
            total_rows += count as u64;

            // 获取文件大小
            let path = if file_path.starts_with("file://") {
                file_path.strip_prefix("file://").unwrap_or(file_path)
            } else {
                file_path
            };
            if let Ok(metadata) = std::fs::metadata(path) {
                total_size += metadata.len();
            }

            self.ctx.deregister_table(&table_name)?;
        }

        let estimated_output_files = (total_size as f64 / self.options.target_file_size as f64).ceil() as u64;

        Ok(MergeStats {
            input_file_count: input_files.len() as u64,
            total_rows,
            total_size,
            estimated_output_files,
        })
    }

    /// Create a union DataFrame from multiple input files
    async fn create_union_dataframe(&self, input_files: &[&str]) -> Result<DataFrame> {
        let mut dfs: Vec<DataFrame> = Vec::with_capacity(input_files.len());

        for (idx, file_path) in input_files.iter().enumerate() {
            let normalized = Self::normalize_path(file_path)?;
            let table_url = ListingTableUrl::parse(&normalized)?;

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
            let table_name = format!("merge_input_{}_{}", idx, uuid::Uuid::new_v4().simple());
            self.ctx.register_table(&table_name, Arc::new(table))?;

            let df = self.ctx.table(&table_name).await?;
            dfs.push(df);
        }

        // Union all DataFrames
        if dfs.len() == 1 {
            return Ok(dfs.into_iter().next().unwrap());
        }

        let mut iter = dfs.into_iter();
        let mut result = iter.next().unwrap();
        for df in iter {
            result = result.union(df)?;
        }

        Ok(result)
    }

    /// Estimate the size of a RecordBatch in bytes
    fn estimate_batch_size(batch: &RecordBatch) -> usize {
        batch.get_array_memory_size()
    }

    /// Create write options from merge options
    fn create_write_options(&self) -> WriteOptions {
        WriteOptions::new()
            .with_compression(self.options.compression)
            .with_row_group_size(self.options.row_group_size)
            .with_statistics(self.options.enable_statistics)
            .with_bloom_filter(self.options.enable_bloom_filter, 0.05)
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

/// Statistics about the merge operation
#[derive(Debug, Clone)]
pub struct MergeStats {
    /// Number of input files
    pub input_file_count: u64,
    /// Total number of rows across all input files
    pub total_rows: u64,
    /// Total size of input files in bytes
    pub total_size: u64,
    /// Estimated number of output files based on target file size
    pub estimated_output_files: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_options_default() {
        let options = MergeOptions::default();
        assert_eq!(options.target_file_size, 128 * 1024 * 1024);
        assert_eq!(options.max_memory, 1024 * 1024 * 1024);
    }

    #[test]
    fn test_merge_options_builder() {
        let options = MergeOptions::new()
            .with_target_file_size(256 * 1024 * 1024)
            .with_max_memory(2 * 1024 * 1024 * 1024)
            .with_sort_columns(vec!["id".to_string(), "ts".to_string()]);

        assert_eq!(options.target_file_size, 256 * 1024 * 1024);
        assert_eq!(options.max_memory, 2 * 1024 * 1024 * 1024);
        assert_eq!(options.sort_columns, Some(vec!["id".to_string(), "ts".to_string()]));
    }
}
