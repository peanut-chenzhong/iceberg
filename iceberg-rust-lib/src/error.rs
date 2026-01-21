//! Error types for the Iceberg Rust library

use thiserror::Error;

/// Result type alias for this library
pub type Result<T> = std::result::Result<T, IcebergError>;

/// Main error type for the Iceberg library
#[derive(Error, Debug)]
pub enum IcebergError {
    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Parquet error
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// Arrow error
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// DataFusion error
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    /// Object store error
    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    /// Invalid file format
    #[error("Invalid file format: {0}")]
    InvalidFormat(String),

    /// Invalid deletion vector
    #[error("Invalid deletion vector: {0}")]
    InvalidDeletionVector(String),

    /// Position delete error
    #[error("Position delete error: {0}")]
    PositionDeleteError(String),

    /// Schema mismatch
    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),

    /// File not found
    #[error("File not found: {0}")]
    FileNotFound(String),

    /// Invalid argument
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// JSON parsing error
    #[error("JSON parsing error: {0}")]
    JsonError(#[from] serde_json::Error),

    /// General error
    #[error("{0}")]
    General(String),
}

impl IcebergError {
    /// Create a general error with a message
    pub fn general<S: Into<String>>(msg: S) -> Self {
        IcebergError::General(msg.into())
    }

    /// Create an invalid format error
    pub fn invalid_format<S: Into<String>>(msg: S) -> Self {
        IcebergError::InvalidFormat(msg.into())
    }

    /// Create an invalid deletion vector error
    pub fn invalid_deletion_vector<S: Into<String>>(msg: S) -> Self {
        IcebergError::InvalidDeletionVector(msg.into())
    }

    /// Create a position delete error
    pub fn position_delete_error<S: Into<String>>(msg: S) -> Self {
        IcebergError::PositionDeleteError(msg.into())
    }
}
