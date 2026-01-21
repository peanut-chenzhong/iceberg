//! Iceberg V3 Table Reader
//!
//! V3版本读取器：支持Deletion Vector（二进制删除向量）
//! 完全支持DataFusion的所有优化：谓词下推、列裁剪等

use crate::deletion_vector::{filter_batch_with_deletion_vector, DeletionVector, DeletionVectorReader};
use crate::error::{IcebergError, Result};
use crate::reader::{create_optimized_context, ReadOptions};

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use futures::{Stream, StreamExt};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

/// Iceberg V3 table reader
///
/// V3版本的Iceberg表使用Deletion Vector（存储在Puffin文件中）
/// 而不是Position Delete文件
pub struct IcebergV3Reader {
    ctx: SessionContext,
}

impl Default for IcebergV3Reader {
    fn default() -> Self {
        Self::new()
    }
}

impl IcebergV3Reader {
    /// Create a new V3 reader
    pub fn new() -> Self {
        Self {
            ctx: create_optimized_context(),
        }
    }

    /// Create a new V3 reader with a custom session context
    pub fn with_context(ctx: SessionContext) -> Self {
        Self { ctx }
    }

    /// Read a parquet file with deletion vector applied
    ///
    /// # Arguments
    /// * `parquet_path` - Path to the parquet data file
    /// * `deletion_vector_path` - Path to the Puffin file containing deletion vectors
    /// * `options` - Read options including projection, filter, etc.
    ///
    /// # Returns
    /// Vector of RecordBatch with deleted rows filtered out
    ///
    /// # Example
    /// ```ignore
    /// use iceberg_rust_lib::{IcebergV3Reader, ReadOptions};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let reader = IcebergV3Reader::new();
    ///     let options = ReadOptions::new()
    ///         .with_projection(vec!["id".to_string(), "name".to_string()])
    ///         .with_filter("id > 100");
    ///     
    ///     let batches = reader.read(
    ///         "path/to/data.parquet",
    ///         "path/to/delete.puffin",
    ///         options
    ///     ).await.unwrap();
    /// }
    /// ```
    pub async fn read(
        &self,
        parquet_path: &str,
        deletion_vector_path: &str,
        options: ReadOptions,
    ) -> Result<Vec<RecordBatch>> {
        // 读取deletion vector文件
        let all_dvs = DeletionVectorReader::read_from_file(deletion_vector_path)?;

        // 查找当前数据文件的deletion vector
        let normalized_path = Self::normalize_path(parquet_path)?;
        let dv = self.find_deletion_vector(&all_dvs, parquet_path, &normalized_path);

        // 如果没有deletion vector，直接读取数据文件
        if dv.is_none() || dv.as_ref().map(|d| d.is_empty()).unwrap_or(true) {
            return self.read_data_file_direct(parquet_path, &options).await;
        }

        // 读取数据文件并应用deletion vector过滤
        self.read_with_deletion_vector(parquet_path, dv.unwrap(), &options).await
    }

    /// Read data file with a pre-loaded deletion vector
    ///
    /// 如果已经有deletion vector对象，可以直接使用这个方法避免重复读取
    pub async fn read_with_dv(
        &self,
        parquet_path: &str,
        dv: &DeletionVector,
        options: ReadOptions,
    ) -> Result<Vec<RecordBatch>> {
        if dv.is_empty() {
            return self.read_data_file_direct(parquet_path, &options).await;
        }
        self.read_with_deletion_vector(parquet_path, dv.clone(), &options).await
    }

    /// Read multiple parquet files with their deletion vectors
    ///
    /// 批量读取多个数据文件，每个文件可能有对应的deletion vector
    pub async fn read_batch(
        &self,
        file_dv_pairs: &[(&str, Option<&str>)],
        options: ReadOptions,
    ) -> Result<Vec<RecordBatch>> {
        let mut all_batches = Vec::new();

        for (parquet_path, dv_path) in file_dv_pairs {
            let batches = if let Some(dv_path) = dv_path {
                self.read(parquet_path, dv_path, options.clone()).await?
            } else {
                self.read_data_file_direct(parquet_path, &options).await?
            };
            all_batches.extend(batches);
        }

        Ok(all_batches)
    }

    /// Read data file directly without deletion vector
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

        let table_name = format!("iceberg_v3_data_{}", uuid::Uuid::new_v4().simple());
        self.ctx.register_table(&table_name, Arc::new(table))?;

        let df = self.build_query(&table_name, options).await?;
        let batches = df.collect().await?;

        self.ctx.deregister_table(&table_name)?;

        Ok(batches)
    }

    /// Read data file with deletion vector filtering
    async fn read_with_deletion_vector(
        &self,
        parquet_path: &str,
        dv: DeletionVector,
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

        let table_name = format!("iceberg_v3_dv_{}", uuid::Uuid::new_v4().simple());
        self.ctx.register_table(&table_name, Arc::new(table))?;

        let df = self.build_query(&table_name, options).await?;

        // 流式读取并应用deletion vector过滤
        let stream = df.execute_stream().await?;
        let mut result = Vec::new();
        let mut current_position: u64 = 0;

        let mut stream = Box::pin(stream);
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            let batch_rows = batch.num_rows() as u64;

            // 检查这个batch是否有任何删除
            // 使用range检查比单独检查每个位置更高效
            let has_deletes = self.batch_has_deletes(&dv, current_position, batch_rows);

            if has_deletes {
                // 应用deletion vector过滤
                let filtered = filter_batch_with_deletion_vector(&batch, &dv, current_position)?;
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

    /// Check if a batch has any deletes
    fn batch_has_deletes(&self, dv: &DeletionVector, start_pos: u64, num_rows: u64) -> bool {
        // 使用roaring bitmap的高效范围查询
        // 如果deletion vector包含start_pos到start_pos+num_rows范围内的任何位置
        for pos in start_pos..start_pos + num_rows {
            if dv.is_deleted(pos) {
                return true;
            }
        }
        false
    }

    /// Read as stream with deletion vector applied
    pub async fn read_stream(
        &self,
        parquet_path: &str,
        deletion_vector_path: &str,
        options: ReadOptions,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>> {
        let all_dvs = DeletionVectorReader::read_from_file(deletion_vector_path)?;
        let normalized_path = Self::normalize_path(parquet_path)?;
        let dv = self.find_deletion_vector(&all_dvs, parquet_path, &normalized_path)
            .unwrap_or_else(|| DeletionVector::new(parquet_path));

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

        let table_name = format!("iceberg_v3_stream_{}", uuid::Uuid::new_v4().simple());
        self.ctx.register_table(&table_name, Arc::new(table))?;

        let df = self.build_query(&table_name, &options).await?;
        let inner_stream = df.execute_stream().await?;

        // 创建一个应用deletion vector过滤的流
        let filtered_stream = DeletionVectorFilterStream::new(inner_stream, dv);

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

    /// Find deletion vector for a data file
    fn find_deletion_vector(
        &self,
        all_dvs: &HashMap<String, DeletionVector>,
        original_path: &str,
        normalized_path: &str,
    ) -> Option<DeletionVector> {
        // 尝试多种路径匹配方式
        all_dvs.get(original_path).cloned()
            .or_else(|| all_dvs.get(normalized_path).cloned())
            .or_else(|| {
                // 尝试匹配文件名
                let file_name = std::path::Path::new(original_path)
                    .file_name()
                    .and_then(|n| n.to_str())?;
                all_dvs.iter()
                    .find(|(k, _)| k.ends_with(file_name))
                    .map(|(_, v)| v.clone())
            })
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

/// A stream that applies deletion vector filtering
struct DeletionVectorFilterStream<S> {
    inner: S,
    dv: DeletionVector,
    current_position: u64,
}

impl<S> DeletionVectorFilterStream<S> {
    fn new(inner: S, dv: DeletionVector) -> Self {
        Self {
            inner,
            dv,
            current_position: 0,
        }
    }
}

impl<S> Stream for DeletionVectorFilterStream<S>
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
                let batch_rows = batch.num_rows() as u64;
                let start_pos = self.current_position;
                self.current_position += batch_rows;

                // 检查是否有删除
                let has_deletes = (start_pos..start_pos + batch_rows)
                    .any(|pos| self.dv.is_deleted(pos));

                if has_deletes {
                    match filter_batch_with_deletion_vector(&batch, &self.dv, start_pos) {
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
            IcebergV3Reader::normalize_path("s3://bucket/path/file.parquet").unwrap(),
            "s3://bucket/path/file.parquet"
        );
    }
}
