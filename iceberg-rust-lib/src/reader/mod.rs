//! Iceberg table readers
//!
//! 提供V1、V2、V3版本的Iceberg表读取功能
//! 支持DataFusion的所有优化特性：谓词下推、列裁剪等

pub mod v1;
pub mod v2;
pub mod v3;

pub use v1::IcebergV1Reader;
pub use v2::IcebergV2Reader;
pub use v3::IcebergV3Reader;

use arrow_schema::SchemaRef;
use datafusion::prelude::*;

/// Read options for Iceberg table reading
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    /// Column projection - list of column names to read
    /// If empty, all columns are read
    pub projection: Option<Vec<String>>,

    /// Filter expression in SQL format
    /// e.g., "id > 100 AND name = 'test'"
    pub filter: Option<String>,

    /// Batch size for reading
    pub batch_size: Option<usize>,

    /// Whether to enable parallel reading
    pub parallel: bool,

    /// Target schema for the output
    pub schema: Option<SchemaRef>,
}

impl ReadOptions {
    /// Create a new ReadOptions with default values
    pub fn new() -> Self {
        Self {
            projection: None,
            filter: None,
            batch_size: Some(8192),
            parallel: true,
            schema: None,
        }
    }

    /// Set column projection
    pub fn with_projection(mut self, columns: Vec<String>) -> Self {
        self.projection = Some(columns);
        self
    }

    /// Set filter expression
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Set batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Set parallel reading
    pub fn with_parallel(mut self, parallel: bool) -> Self {
        self.parallel = parallel;
        self
    }

    /// Set target schema
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }
}

/// Internal helper to create DataFusion session context with optimizations
pub(crate) fn create_optimized_context() -> SessionContext {
    let config = SessionConfig::new()
        .with_batch_size(8192)
        .with_parquet_pruning(true)
        .with_parquet_bloom_filter_pruning(true)
        .set_bool("datafusion.execution.parquet.pushdown_filters", true)
        .set_bool("datafusion.execution.parquet.reorder_filters", true)
        .set_bool("datafusion.optimizer.enable_round_robin_repartition", true);

    SessionContext::new_with_config(config)
}
