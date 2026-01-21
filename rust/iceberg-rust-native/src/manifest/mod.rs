//! Manifest file reading and writing
//!
//! This module provides high-performance manifest file operations,
//! including zero-copy reading and efficient filtering.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                   ManifestReader                         │
//! │  ┌──────────────────────────────────────────────────┐   │
//! │  │               Avro Reader                         │   │
//! │  │  (Zero-copy deserialization)                      │   │
//! │  └──────────────────────────────────────────────────┘   │
//! │                         │                               │
//! │                         ▼                               │
//! │  ┌──────────────────────────────────────────────────┐   │
//! │  │             ManifestEntry<F>                      │   │
//! │  │  ├── status: Status                               │   │
//! │  │  ├── snapshot_id: Option<i64>                     │   │
//! │  │  ├── data_sequence_number: Option<i64>            │   │
//! │  │  ├── file_sequence_number: Option<i64>            │   │
//! │  │  └── file: F (DataFile or DeleteFile)             │   │
//! │  └──────────────────────────────────────────────────┘   │
//! │                         │                               │
//! │                         ▼                               │
//! │  ┌──────────────────────────────────────────────────┐   │
//! │  │               ContentFile                         │   │
//! │  │  ├── content: FileContent                         │   │
//! │  │  ├── file_path: String                            │   │
//! │  │  ├── file_format: FileFormat                      │   │
//! │  │  ├── partition: PartitionData                     │   │
//! │  │  ├── record_count: i64                            │   │
//! │  │  ├── file_size_in_bytes: i64                      │   │
//! │  │  ├── column_sizes: HashMap<i32, i64>              │   │
//! │  │  ├── ... (statistics)                             │   │
//! │  │  └── first_row_id: Option<i64>  (V3 Row Lineage)  │   │
//! │  └──────────────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────┘
//! ```

mod content_file;
mod entry;
mod filter;
mod group;
mod reader;

pub use content_file::{ContentFile, ContentFileData, DataFile, DeleteFile};
pub use entry::{ManifestEntry, Status};
pub use filter::{
    FileMetrics, InclusiveMetricsEvaluator, MetricsFilter, MetricsOperation,
    PartitionEvaluator, PartitionSet,
};
pub use group::{
    ManifestFile, ManifestGroup, ManifestGroupBuilder,
    ParallelManifestIter, ParallelReadResult, ParallelReadStats,
};
pub use reader::{ManifestContent, ManifestMetadata, ManifestReadStats, ManifestReader, ManifestReaderBuilder};
