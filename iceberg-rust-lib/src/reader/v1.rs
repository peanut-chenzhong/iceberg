//! Iceberg V1 Table Reader
//!
//! V1版本读取器：直接读取Parquet数据文件
//! 支持DataFusion的所有优化：谓词下推、列裁剪、行组过滤等

use crate::error::{IcebergError, Result};
use crate::reader::{create_optimized_context, ReadOptions};

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use futures::TryStreamExt;
use std::sync::Arc;

/// Iceberg V1 table reader
///
/// V1版本的Iceberg表只包含数据文件，不包含删除文件
/// 使用DataFusion直接读取Parquet文件，完全支持谓词下推和列裁剪
pub struct IcebergV1Reader {
    ctx: SessionContext,
}

impl Default for IcebergV1Reader {
    fn default() -> Self {
        Self::new()
    }
}

impl IcebergV1Reader {
    /// Create a new V1 reader
    pub fn new() -> Self {
        Self {
            ctx: create_optimized_context(),
        }
    }

    /// Create a new V1 reader with a custom session context
    pub fn with_context(ctx: SessionContext) -> Self {
        Self { ctx }
    }

    /// Read a parquet file and return RecordBatches
    ///
    /// # Arguments
    /// * `parquet_path` - Path to the parquet file (local or object store URL)
    /// * `options` - Read options including projection, filter, etc.
    ///
    /// # Returns
    /// Vector of RecordBatch containing the data
    ///
    /// # Example
    /// ```ignore
    /// use iceberg_rust_lib::{IcebergV1Reader, ReadOptions};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let reader = IcebergV1Reader::new();
    ///     let options = ReadOptions::new()
    ///         .with_projection(vec!["id".to_string(), "name".to_string()])
    ///         .with_filter("id > 100");
    ///     
    ///     let batches = reader.read("path/to/file.parquet", options).await.unwrap();
    /// }
    /// ```
    pub async fn read(&self, parquet_path: &str, options: ReadOptions) -> Result<Vec<RecordBatch>> {
        // 注册parquet文件为临时表
        let table_path = Self::normalize_path(parquet_path)?;
        
        // 使用ListingTable支持更好的优化
        let table_url = ListingTableUrl::parse(&table_path)?;
        
        let file_format = ParquetFormat::default()
            .with_enable_pruning(true);
        
        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(".parquet");

        // 获取文件schema
        let resolved_schema = listing_options
            .infer_schema(&self.ctx.state(), &table_url)
            .await?;

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);

        let table = ListingTable::try_new(config)?;

        // 注册表
        self.ctx.register_table("iceberg_v1_temp", Arc::new(table))?;

        // 构建查询
        let df = self.build_query("iceberg_v1_temp", &options).await?;

        // 执行查询并收集结果
        let batches = df.collect().await?;

        // 清理临时表
        self.ctx.deregister_table("iceberg_v1_temp")?;

        Ok(batches)
    }

    /// Read parquet file and return a DataFrame for further processing
    ///
    /// This method allows more flexible data processing using DataFusion's DataFrame API
    pub async fn read_as_dataframe(
        &self,
        parquet_path: &str,
        options: ReadOptions,
    ) -> Result<DataFrame> {
        let table_path = Self::normalize_path(parquet_path)?;
        let table_url = ListingTableUrl::parse(&table_path)?;

        let file_format = ParquetFormat::default()
            .with_enable_pruning(true);

        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(".parquet");

        let resolved_schema = listing_options
            .infer_schema(&self.ctx.state(), &table_url)
            .await?;

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);

        let table = ListingTable::try_new(config)?;

        let table_name = format!("iceberg_v1_df_{}", uuid::Uuid::new_v4().simple());
        self.ctx.register_table(&table_name, Arc::new(table))?;

        let df = self.build_query(&table_name, &options).await?;

        Ok(df)
    }

    /// Read parquet file with streaming support
    ///
    /// Returns batches one at a time using async iteration
    pub async fn read_stream(
        &self,
        parquet_path: &str,
        options: ReadOptions,
    ) -> Result<impl futures::Stream<Item = Result<RecordBatch>>> {
        let df = self.read_as_dataframe(parquet_path, options).await?;
        let stream = df.execute_stream().await?;
        
        Ok(stream.map_err(|e| IcebergError::DataFusion(e)))
    }

    /// Get the schema of a parquet file
    pub async fn get_schema(&self, parquet_path: &str) -> Result<SchemaRef> {
        let table_path = Self::normalize_path(parquet_path)?;
        let table_url = ListingTableUrl::parse(&table_path)?;

        let file_format = ParquetFormat::default();
        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(".parquet");

        let schema = listing_options
            .infer_schema(&self.ctx.state(), &table_url)
            .await?;

        Ok(schema)
    }

    /// Build a query with projection and filter
    async fn build_query(&self, table_name: &str, options: &ReadOptions) -> Result<DataFrame> {
        let mut df = self.ctx.table(table_name).await?;

        // Apply column projection
        if let Some(ref columns) = options.projection {
            let cols: Vec<Expr> = columns.iter().map(|c| col(c)).collect();
            df = df.select(cols)?;
        }

        // Apply filter if provided using SQL
        if let Some(ref filter_expr) = options.filter {
            // 使用SQL语法解析过滤表达式
            let df_schema = df.schema().clone();
            let expr = self.ctx.parse_sql_expr(filter_expr, &df_schema)?;
            df = df.filter(expr)?;
        }

        Ok(df)
    }

    /// Normalize file path for different storage backends
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
            // 本地文件路径
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

    #[tokio::test]
    async fn test_normalize_path() {
        assert_eq!(
            IcebergV1Reader::normalize_path("s3://bucket/path/file.parquet").unwrap(),
            "s3://bucket/path/file.parquet"
        );

        let local_path = IcebergV1Reader::normalize_path("test.parquet").unwrap();
        assert!(local_path.starts_with("file://"));
    }
}
