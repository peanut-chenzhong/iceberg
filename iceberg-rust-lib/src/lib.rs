//! Iceberg Rust Library
//!
//! 使用DataFusion实现Iceberg表文件的读写操作
//!
//! # 功能特性
//! - Iceberg V1表读取（支持谓词下推、列裁剪）
//! - Iceberg V2表读取（支持position delete文件）
//! - Iceberg V3表读取（支持deletion vector）
//! - Parquet文件异步写入
//! - Parquet小文件合并

pub mod error;
pub mod reader;
pub mod writer;
pub mod deletion_vector;
pub mod position_delete;
pub mod merge;

pub use error::{IcebergError, Result};
pub use reader::{IcebergV1Reader, IcebergV2Reader, IcebergV3Reader, ReadOptions};
pub use writer::{ParquetWriter, WriteOptions};
pub use merge::{FileMerger, MergeOptions};
