//! Error types for iceberg-rust-native
//!
//! This module provides a unified error type for all operations.

use thiserror::Error;

/// Result type alias using our Error type
pub type Result<T> = std::result::Result<T, Error>;

/// Unified error type for iceberg-rust-native
#[derive(Error, Debug)]
pub enum Error {
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Avro decoding error
    #[error("Avro error: {0}")]
    Avro(#[from] apache_avro::Error),

    /// JSON parsing error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Invalid data format
    #[error("Invalid data: {message}")]
    InvalidData { message: String },

    /// Schema mismatch
    #[error("Schema mismatch: expected {expected}, got {actual}")]
    SchemaMismatch { expected: String, actual: String },

    /// Missing required field
    #[error("Missing required field: {field}")]
    MissingField { field: String },

    /// Unsupported format version
    #[error("Unsupported format version: {version}")]
    UnsupportedVersion { version: i32 },

    /// Invalid file path
    #[error("Invalid file path: {path}")]
    InvalidPath { path: String },

    /// Invalid partition data
    #[error("Invalid partition: {message}")]
    InvalidPartition { message: String },

    /// Generic error with context
    #[error("{context}: {source}")]
    WithContext {
        context: String,
        #[source]
        source: Box<Error>,
    },

    /// Other/generic error
    #[error("{message}")]
    Other { message: String },
}

impl Error {
    /// Create an Other error
    pub fn other(message: impl Into<String>) -> Self {
        Error::Other {
            message: message.into(),
        }
    }

    /// Create an InvalidData error
    pub fn invalid_data(message: impl Into<String>) -> Self {
        Error::InvalidData {
            message: message.into(),
        }
    }

    /// Create a MissingField error
    pub fn missing_field(field: impl Into<String>) -> Self {
        Error::MissingField {
            field: field.into(),
        }
    }

    /// Add context to an error
    pub fn with_context(self, context: impl Into<String>) -> Self {
        Error::WithContext {
            context: context.into(),
            source: Box::new(self),
        }
    }
}

/// Extension trait for adding context to Results
pub trait ResultExt<T> {
    /// Add context to an error
    fn context(self, context: impl Into<String>) -> Result<T>;
}

impl<T> ResultExt<T> for Result<T> {
    fn context(self, context: impl Into<String>) -> Result<T> {
        self.map_err(|e| e.with_context(context))
    }
}
