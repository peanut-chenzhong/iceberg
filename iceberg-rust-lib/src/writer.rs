//! Parquet File Writer
//!
//! 使用DataFusion实现高性能的异步Parquet文件写入
//! 支持各种写入优化参数

use crate::error::{IcebergError, Result};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::config::TableParquetOptions;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use parquet::basic::Compression;
use parquet::file::properties::{WriterProperties, WriterVersion};
use std::path::Path;
use std::sync::Arc;

/// Parquet write options
#[derive(Debug, Clone)]
pub struct WriteOptions {
    /// Compression codec
    pub compression: CompressionType,
    /// Row group size (number of rows per row group)
    pub row_group_size: usize,
    /// Data page size in bytes
    pub data_page_size: usize,
    /// Dictionary page size in bytes
    pub dictionary_page_size: usize,
    /// Whether to enable dictionary encoding
    pub enable_dictionary: bool,
    /// Whether to enable statistics
    pub enable_statistics: bool,
    /// Max statistics size
    pub max_statistics_size: usize,
    /// Bloom filter enabled
    pub bloom_filter_enabled: bool,
    /// Bloom filter false positive probability
    pub bloom_filter_fpp: f64,
    /// Bloom filter number of distinct values hint
    pub bloom_filter_ndv: Option<u64>,
    /// Writer version
    pub writer_version: ParquetWriterVersion,
    /// Whether to write column index
    pub write_column_index: bool,
    /// Whether to write offset index
    pub write_offset_index: bool,
    /// Maximum number of rows per file (for splitting)
    pub max_rows_per_file: Option<usize>,
    /// Target file size in bytes (for splitting)
    pub target_file_size: Option<usize>,
}

/// Compression type for Parquet files
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    Uncompressed,
    Snappy,
    Gzip,
    Lz4,
    Zstd,
    Lz4Raw,
    Brotli,
}

impl Default for CompressionType {
    fn default() -> Self {
        CompressionType::Zstd
    }
}

impl From<CompressionType> for Compression {
    fn from(ct: CompressionType) -> Compression {
        match ct {
            CompressionType::Uncompressed => Compression::UNCOMPRESSED,
            CompressionType::Snappy => Compression::SNAPPY,
            CompressionType::Gzip => Compression::GZIP(Default::default()),
            CompressionType::Lz4 => Compression::LZ4,
            CompressionType::Zstd => Compression::ZSTD(Default::default()),
            CompressionType::Lz4Raw => Compression::LZ4_RAW,
            CompressionType::Brotli => Compression::BROTLI(Default::default()),
        }
    }
}

/// Parquet writer version
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParquetWriterVersion {
    V1,
    V2,
}

impl Default for ParquetWriterVersion {
    fn default() -> Self {
        ParquetWriterVersion::V2
    }
}

impl From<ParquetWriterVersion> for WriterVersion {
    fn from(v: ParquetWriterVersion) -> WriterVersion {
        match v {
            ParquetWriterVersion::V1 => WriterVersion::PARQUET_1_0,
            ParquetWriterVersion::V2 => WriterVersion::PARQUET_2_0,
        }
    }
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            compression: CompressionType::Zstd,
            row_group_size: 1024 * 1024, // 1M rows
            data_page_size: 1024 * 1024, // 1MB
            dictionary_page_size: 1024 * 1024, // 1MB
            enable_dictionary: true,
            enable_statistics: true,
            max_statistics_size: 4096,
            bloom_filter_enabled: false,
            bloom_filter_fpp: 0.05,
            bloom_filter_ndv: None,
            writer_version: ParquetWriterVersion::V2,
            write_column_index: true,
            write_offset_index: true,
            max_rows_per_file: None,
            target_file_size: None,
        }
    }
}

impl WriteOptions {
    /// Create a new WriteOptions with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set compression type
    pub fn with_compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }

    /// Set row group size
    pub fn with_row_group_size(mut self, size: usize) -> Self {
        self.row_group_size = size;
        self
    }

    /// Set data page size
    pub fn with_data_page_size(mut self, size: usize) -> Self {
        self.data_page_size = size;
        self
    }

    /// Set dictionary page size
    pub fn with_dictionary_page_size(mut self, size: usize) -> Self {
        self.dictionary_page_size = size;
        self
    }

    /// Enable or disable dictionary encoding
    pub fn with_dictionary(mut self, enable: bool) -> Self {
        self.enable_dictionary = enable;
        self
    }

    /// Enable or disable statistics
    pub fn with_statistics(mut self, enable: bool) -> Self {
        self.enable_statistics = enable;
        self
    }

    /// Enable bloom filter
    pub fn with_bloom_filter(mut self, enabled: bool, fpp: f64) -> Self {
        self.bloom_filter_enabled = enabled;
        self.bloom_filter_fpp = fpp;
        self
    }

    /// Set writer version
    pub fn with_writer_version(mut self, version: ParquetWriterVersion) -> Self {
        self.writer_version = version;
        self
    }

    /// Set max rows per file
    pub fn with_max_rows_per_file(mut self, max_rows: usize) -> Self {
        self.max_rows_per_file = Some(max_rows);
        self
    }

    /// Set target file size
    pub fn with_target_file_size(mut self, size: usize) -> Self {
        self.target_file_size = Some(size);
        self
    }

    /// Convert to ParquetOptions for DataFusion
    pub(crate) fn to_table_parquet_options(&self) -> TableParquetOptions {
        let mut options = TableParquetOptions::default();
        
        options.global.compression = Some(match self.compression {
            CompressionType::Uncompressed => "uncompressed".to_string(),
            CompressionType::Snappy => "snappy".to_string(),
            CompressionType::Gzip => "gzip".to_string(),
            CompressionType::Lz4 => "lz4".to_string(),
            CompressionType::Zstd => "zstd".to_string(),
            CompressionType::Lz4Raw => "lz4_raw".to_string(),
            CompressionType::Brotli => "brotli".to_string(),
        });

        options.global.dictionary_enabled = Some(self.enable_dictionary);
        // statistics_enabled expects a string: "none", "chunk", "page"
        if self.enable_statistics {
            options.global.statistics_enabled = Some("page".to_string());
        } else {
            options.global.statistics_enabled = Some("none".to_string());
        }
        options.global.max_statistics_size = Some(self.max_statistics_size);
        options.global.max_row_group_size = self.row_group_size;
        options.global.write_batch_size = 1024;
        options.global.bloom_filter_on_write = self.bloom_filter_enabled;
        options.global.bloom_filter_fpp = Some(self.bloom_filter_fpp);
        options.global.bloom_filter_ndv = self.bloom_filter_ndv;

        options
    }

    /// Convert to WriterProperties for direct parquet writing
    pub fn to_writer_properties(&self) -> WriterProperties {
        let mut builder = WriterProperties::builder()
            .set_compression(self.compression.into())
            .set_writer_version(self.writer_version.into())
            .set_max_row_group_size(self.row_group_size)
            .set_data_page_size_limit(self.data_page_size)
            .set_dictionary_page_size_limit(self.dictionary_page_size)
            .set_max_statistics_size(self.max_statistics_size)
            .set_column_index_truncate_length(Some(64))
            .set_statistics_truncate_length(Some(64));

        if self.enable_dictionary {
            builder = builder.set_dictionary_enabled(true);
        } else {
            builder = builder.set_dictionary_enabled(false);
        }

        if self.bloom_filter_enabled {
            builder = builder.set_bloom_filter_enabled(true);
            builder = builder.set_bloom_filter_fpp(self.bloom_filter_fpp);
            if let Some(ndv) = self.bloom_filter_ndv {
                builder = builder.set_bloom_filter_ndv(ndv);
            }
        }

        builder.build()
    }
}

/// Parquet file writer using DataFusion
pub struct ParquetWriter {
    ctx: SessionContext,
}

impl Default for ParquetWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl ParquetWriter {
    /// Create a new ParquetWriter
    pub fn new() -> Self {
        let config = SessionConfig::new()
            .with_batch_size(8192)
            .with_target_partitions(num_cpus::get());

        Self {
            ctx: SessionContext::new_with_config(config),
        }
    }

    /// Create a new ParquetWriter with a custom session context
    pub fn with_context(ctx: SessionContext) -> Self {
        Self { ctx }
    }

    /// Write RecordBatches to a Parquet file
    ///
    /// # Arguments
    /// * `batches` - The RecordBatches to write
    /// * `output_path` - The output file path
    /// * `options` - Write options
    ///
    /// # Example
    /// ```ignore
    /// use iceberg_rust_lib::{ParquetWriter, WriteOptions, CompressionType};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let writer = ParquetWriter::new();
    ///     let options = WriteOptions::new()
    ///         .with_compression(CompressionType::Zstd)
    ///         .with_row_group_size(100000);
    ///     
    ///     writer.write(&batches, "output.parquet", options).await.unwrap();
    /// }
    /// ```
    pub async fn write(
        &self,
        batches: &[RecordBatch],
        output_path: &str,
        options: WriteOptions,
    ) -> Result<()> {
        if batches.is_empty() {
            return Err(IcebergError::InvalidArgument("No batches to write".to_string()));
        }

        let schema = batches[0].schema();

        // 创建内存表
        let mem_table = MemTable::try_new(schema.clone(), vec![batches.to_vec()])?;

        // 注册表
        let table_name = format!("write_temp_{}", uuid::Uuid::new_v4().simple());
        self.ctx.register_table(&table_name, Arc::new(mem_table))?;

        // 创建DataFrame
        let df = self.ctx.table(&table_name).await?;

        // 准备输出路径
        let output_path = Self::normalize_output_path(output_path)?;

        // 配置写入选项
        let parquet_options = options.to_table_parquet_options();
        let write_options = DataFrameWriteOptions::new()
            .with_single_file_output(true);

        // 执行写入
        df.write_parquet(&output_path, write_options, Some(parquet_options)).await?;

        // 清理临时表
        self.ctx.deregister_table(&table_name)?;

        Ok(())
    }

    /// Write RecordBatches to multiple Parquet files (partitioned by rows)
    ///
    /// 将数据分成多个文件写入，适用于大数据量
    pub async fn write_partitioned(
        &self,
        batches: &[RecordBatch],
        output_dir: &str,
        file_prefix: &str,
        options: WriteOptions,
    ) -> Result<Vec<String>> {
        if batches.is_empty() {
            return Err(IcebergError::InvalidArgument("No batches to write".to_string()));
        }

        let _schema = batches[0].schema(); // Keep for potential future validation
        let max_rows = options.max_rows_per_file.unwrap_or(1_000_000);
        
        // 将batches按max_rows分组
        let mut current_rows = 0;
        let mut current_batches: Vec<RecordBatch> = Vec::new();
        let mut file_index = 0;
        let mut output_files = Vec::new();

        // 确保输出目录存在
        std::fs::create_dir_all(output_dir)?;

        for batch in batches {
            current_batches.push(batch.clone());
            current_rows += batch.num_rows();

            if current_rows >= max_rows {
                // 写入当前分区
                let file_name = format!("{}-{:05}.parquet", file_prefix, file_index);
                let file_path = Path::new(output_dir).join(&file_name);
                let file_path_str = file_path.to_string_lossy().to_string();

                self.write(&current_batches, &file_path_str, options.clone()).await?;
                output_files.push(file_path_str);

                current_batches.clear();
                current_rows = 0;
                file_index += 1;
            }
        }

        // 写入剩余数据
        if !current_batches.is_empty() {
            let file_name = format!("{}-{:05}.parquet", file_prefix, file_index);
            let file_path = Path::new(output_dir).join(&file_name);
            let file_path_str = file_path.to_string_lossy().to_string();

            self.write(&current_batches, &file_path_str, options).await?;
            output_files.push(file_path_str);
        }

        Ok(output_files)
    }

    /// Write a DataFrame to Parquet
    pub async fn write_dataframe(
        &self,
        df: DataFrame,
        output_path: &str,
        options: WriteOptions,
    ) -> Result<()> {
        let output_path = Self::normalize_output_path(output_path)?;
        let parquet_options = options.to_table_parquet_options();
        let write_options = DataFrameWriteOptions::new()
            .with_single_file_output(true);

        df.write_parquet(&output_path, write_options, Some(parquet_options)).await?;

        Ok(())
    }

    /// Write with streaming support for memory efficiency
    ///
    /// 使用流式写入，减少内存使用，适合大数据量
    pub async fn write_stream<S>(
        &self,
        mut stream: S,
        schema: SchemaRef,
        output_path: &str,
        options: WriteOptions,
    ) -> Result<()>
    where
        S: futures::Stream<Item = Result<RecordBatch>> + Unpin,
    {
        use futures::StreamExt;
        use parquet::arrow::AsyncArrowWriter;
        use tokio::fs::File;

        let output_path = Self::normalize_output_path(output_path)?;
        let file_path = output_path.strip_prefix("file://").unwrap_or(&output_path);
        
        // 确保父目录存在
        if let Some(parent) = Path::new(file_path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = File::create(file_path).await?;
        let props = options.to_writer_properties();

        let mut writer = AsyncArrowWriter::try_new(file, schema, Some(props))?;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            writer.write(&batch).await?;
        }

        writer.close().await?;

        Ok(())
    }

    /// Normalize output path
    fn normalize_output_path(path: &str) -> Result<String> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_options_default() {
        let options = WriteOptions::default();
        assert_eq!(options.compression, CompressionType::Zstd);
        assert!(options.enable_dictionary);
        assert!(options.enable_statistics);
    }

    #[test]
    fn test_write_options_builder() {
        let options = WriteOptions::new()
            .with_compression(CompressionType::Snappy)
            .with_row_group_size(50000)
            .with_dictionary(false);

        assert_eq!(options.compression, CompressionType::Snappy);
        assert_eq!(options.row_group_size, 50000);
        assert!(!options.enable_dictionary);
    }
}
