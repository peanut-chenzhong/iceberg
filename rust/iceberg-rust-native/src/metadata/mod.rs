//! Table Metadata Parsing
//!
//! This module provides types and functions for parsing Iceberg table metadata JSON files.
//!
//! ## Example
//!
//! ```rust,ignore
//! use iceberg_rust_native::metadata::TableMetadata;
//!
//! let json = std::fs::read_to_string("metadata.json")?;
//! let metadata = TableMetadata::from_json(&json)?;
//! println!("Table location: {}", metadata.location);
//! println!("Format version: {}", metadata.format_version);
//! ```

pub mod schema;
pub mod snapshot;
pub mod spec;
pub mod table;

pub use schema::{Schema, SchemaField, NestedField, Type};
pub use snapshot::{Snapshot, SnapshotRef, SnapshotLogEntry};
pub use spec::{PartitionSpec, PartitionField, SortOrder, SortField};
pub use table::{TableMetadata, TableMetadataParser, MetadataLogEntry};
