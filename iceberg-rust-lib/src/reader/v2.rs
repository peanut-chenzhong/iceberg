//! Iceberg V2 Table Reader
//!
//! V2版本读取器：支持Position Delete文件
//! 完全支持DataFusion的所有优化：谓词下推、列裁剪等

use crate::error::{IcebergError, Result};
use crate::position_delete::{filter_batch_with_position_deletes, PositionDeleteReader, PositionDeletes};
use crate::reader::{create_optimized_context, ReadOptions};

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;

/// Iceberg V2 table reader
///
/// V2版本的Iceberg表支持Position Delete文件
/// 使用DataFusion读取数据文件，然后应用position delete过滤
pub struct IcebergV2Reader {
    ctx: SessionContext,
    position_delete_reader: PositionDeleteReader,
}

impl Default for IcebergV2Reader {
    fn default() -> Self {
        Self::new()
    }
}

impl IcebergV2Reader {
    /// Create a new V2 reader
    pub fn new() -> Self {
        Self {
            ctx: create_optimized_context(),
            position_delete_reader: PositionDeleteReader::new(),
        }
    }

    /// Create a new V2 reader with a custom session context
    pub fn with_context(ctx: SessionContext) -> Self {
        Self {
            ctx,
            position_delete_reader: PositionDeleteReader::new(),
        }
    }

    /// Read a parquet file with position deletes applied
    ///
    /// # Arguments
    /// * `parquet_path` - Path to the parquet data file
    /// * `position_delete_paths` - Paths to position delete files
    /// * `options` - Read options including projection, filter, etc.
    ///
    /// # Returns
    /// Vector of RecordBatch with deleted rows filtered out
    ///
    /// # Example
    /// ```ignore
    /// use iceberg_rust_lib::{IcebergV2Reader, ReadOptions};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let reader = IcebergV2Reader::new();
    ///     let options = ReadOptions::new()
    ///         .with_projection(vec!["id".to_string(), "name".to_string()])
    ///         .with_filter("id > 100");
    ///     
    ///     let batches = reader.read(
    ///         "path/to/data.parquet",
    ///         &["path/to/delete.parquet"],
    ///         options
    ///     ).await.unwrap();
    /// }
    /// ```
    pub async fn read(
        &self,
        parquet_path: &str,
        position_delete_paths: &[&str],
        options: ReadOptions,
    ) -> Result<Vec<RecordBatch>> {
        // 首先读取position delete文件获取删除信息
        let all_deletes = self
            .position_delete_reader
            .read_delete_files(position_delete_paths)
            .await?;

        // 获取当前数据文件的删除信息
        let normalized_data_path = Self::normalize_path(parquet_path)?;
        let deletes = all_deletes
            .get(&normalized_data_path)
            .cloned()
            .or_else(|| {
                // 也尝试用原始路径匹配
                all_deletes.get(parquet_path).cloned()
            })
            .unwrap_or_default();

        // 如果没有删除记录，直接读取数据文件
        if deletes.is_empty() {
            return self.read_data_file_direct(parquet_path, &options).await;
        }

        // 读取数据文件并应用删除过滤
        self.read_with_deletes(parquet_path, &deletes, &options).await
    }

    /// Read data file with position deletes for a specific data file
    ///
    /// 使用谓词下推优化position delete文件读取
    pub async fn read_optimized(
        &self,
        parquet_path: &str,
        position_delete_path: &str,
        options: ReadOptions,
    ) -> Result<Vec<RecordBatch>> {
        // 使用优化的方式读取特定数据文件的删除记录
        let normalized_path = Self::normalize_path(parquet_path)?;
        let deletes = self
            .position_delete_reader
            .read_delete_file_for_data_file(position_delete_path, &normalized_path)
            .await?;

        // 如果也需要尝试原始路径
        let deletes = if deletes.is_empty() {
            self.position_delete_reader
                .read_delete_file_for_data_file(position_delete_path, parquet_path)
                .await?
        } else {
            deletes
        };

        if deletes.is_empty() {
            return self.read_data_file_direct(parquet_path, &options).await;
        }

        self.read_with_deletes(parquet_path, &deletes, &options).await
    }

    /// Read data file directly without position deletes
    async fn read_data_file_direct(
        &self,
        parquet_path: &str,
        options: &ReadOptions,
    ) -> Result<Vec<RecordBatch>> {
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

        let table_name = format!("iceberg_v2_data_{}", uuid::Uuid::new_v4().simple());
        self.ctx.register_table(&table_name, Arc::new(table))?;

        let df = self.build_query(&table_name, options).await?;
        let batches = df.collect().await?;

        self.ctx.deregister_table(&table_name)?;

        Ok(batches)
    }

    /// Read data file with position delete filtering
    async fn read_with_deletes(
        &self,
        parquet_path: &str,
        deletes: &PositionDeletes,
        options: &ReadOptions,
    ) -> Result<Vec<RecordBatch>> {
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

        let table_name = format!("iceberg_v2_del_{}", uuid::Uuid::new_v4().simple());
        self.ctx.register_table(&table_name, Arc::new(table))?;

        let df = self.build_query(&table_name, options).await?;

        // 执行流式读取并应用删除过滤
        let stream = df.execute_stream().await?;
        let mut result = Vec::new();
        let mut current_position: i64 = 0;

        let mut stream = Box::pin(stream);
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            let batch_rows = batch.num_rows() as i64;

            // 检查这个batch是否有任何删除
            let has_deletes = (current_position..current_position + batch_rows)
                .any(|pos| deletes.is_deleted(pos));

            if has_deletes {
                // 应用删除过滤
                let filtered = filter_batch_with_position_deletes(&batch, deletes, current_position)?;
                if filtered.num_rows() > 0 {
                    result.push(filtered);
                }
            } else {
                // 没有删除，直接使用原始batch
                result.push(batch);
            }

            current_position += batch_rows;
        }

        self.ctx.deregister_table(&table_name)?;

        Ok(result)
    }

    /// Read as stream with position deletes applied
    pub async fn read_stream(
        &self,
        parquet_path: &str,
        position_delete_paths: &[&str],
        options: ReadOptions,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>> {
        // 首先读取所有position deletes
        let all_deletes = self
            .position_delete_reader
            .read_delete_files(position_delete_paths)
            .await?;

        let normalized_data_path = Self::normalize_path(parquet_path)?;
        let deletes = all_deletes
            .get(&normalized_data_path)
            .cloned()
            .or_else(|| all_deletes.get(parquet_path).cloned())
            .unwrap_or_default();

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

        let table_name = format!("iceberg_v2_stream_{}", uuid::Uuid::new_v4().simple());
        self.ctx.register_table(&table_name, Arc::new(table))?;

        let df = self.build_query(&table_name, &options).await?;
        let inner_stream = df.execute_stream().await?;

        // 创建一个应用删除过滤的流
        let filtered_stream = PositionDeleteFilterStream::new(inner_stream, deletes);

        Ok(Box::pin(filtered_stream))
    }

    /// Get the schema of the data file
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

    /// Build query with projection and filter
    async fn build_query(&self, table_name: &str, options: &ReadOptions) -> Result<DataFrame> {
        let mut df = self.ctx.table(table_name).await?;

        if let Some(ref columns) = options.projection {
            let cols: Vec<Expr> = columns.iter().map(|c| col(c)).collect();
            df = df.select(cols)?;
        }

        if let Some(ref filter_expr) = options.filter {
            let df_schema = df.schema().clone();
            let expr = self.ctx.parse_sql_expr(filter_expr, &df_schema)?;
            df = df.filter(expr)?;
        }

        Ok(df)
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

/// A stream that applies position delete filtering
struct PositionDeleteFilterStream<S> {
    inner: S,
    deletes: PositionDeletes,
    current_position: i64,
}

impl<S> PositionDeleteFilterStream<S> {
    fn new(inner: S, deletes: PositionDeletes) -> Self {
        Self {
            inner,
            deletes,
            current_position: 0,
        }
    }
}

impl<S> Stream for PositionDeleteFilterStream<S>
where
    S: Stream<Item = std::result::Result<RecordBatch, datafusion::error::DataFusionError>> + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;

        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let batch_rows = batch.num_rows() as i64;
                let start_pos = self.current_position;
                self.current_position += batch_rows;

                // 检查是否有删除
                let has_deletes = (start_pos..start_pos + batch_rows)
                    .any(|pos| self.deletes.is_deleted(pos));

                if has_deletes {
                    match filter_batch_with_position_deletes(&batch, &self.deletes, start_pos) {
                        Ok(filtered) if filtered.num_rows() > 0 => {
                            Poll::Ready(Some(Ok(filtered)))
                        }
                        Ok(_) => {
                            // 所有行都被删除，继续下一个batch
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        }
                        Err(e) => Poll::Ready(Some(Err(e))),
                    }
                } else {
                    Poll::Ready(Some(Ok(batch)))
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(IcebergError::DataFusion(e)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_path() {
        assert_eq!(
            IcebergV2Reader::normalize_path("s3://bucket/path/file.parquet").unwrap(),
            "s3://bucket/path/file.parquet"
        );
    }
}
