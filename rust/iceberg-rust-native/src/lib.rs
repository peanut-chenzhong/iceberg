//! # Iceberg Rust Native
//!
//! High-performance native Rust implementation of Apache Iceberg core components.
//!
//! This crate provides optimized implementations of performance-critical paths
//! in Apache Iceberg, designed for maximum throughput and minimal memory allocation.
//!
//! ## Features
//!
//! - Zero-copy manifest file reading
//! - Efficient expression evaluation
//! - Memory-efficient data structures
//! - Full V3 spec support including Row Lineage
//!
//! ## Example
//!
//! ```rust,ignore
//! use iceberg_rust_native::manifest::{ManifestReader, ManifestEntry};
//!
//! let reader = ManifestReader::open("path/to/manifest.avro")?;
//! for entry in reader.entries() {
//!     println!("File: {}", entry.file().path());
//! }
//! ```

pub mod delete;
pub mod error;
pub mod expr;
pub mod manifest;
pub mod metadata;
pub mod types;

#[cfg(feature = "jni")]
pub mod jni;

pub use error::{Error, Result};
pub use expr::{
    Expression, BoundExpression, Expressions,
    Operation, Datum, Literal,
    Reference, BoundReference,
    Predicate, UnboundPredicate, BoundPredicate,
    And, Or, Not,
    Evaluator, Binder,
};
pub use delete::{DeleteFileIndex, DeleteFileIndexBuilder, EqualityDeletes, PositionDeletes};
pub use metadata::{
    TableMetadata, TableMetadataParser, Schema, SchemaField, NestedField, Type,
    Snapshot, SnapshotRef, SnapshotLogEntry, PartitionSpec, PartitionField, SortOrder, SortField,
};