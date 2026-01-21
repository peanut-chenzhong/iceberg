//! Content file types (DataFile and DeleteFile)
//!
//! This module defines the core file metadata structures that represent
//! data and delete files in Iceberg tables.

use crate::types::{FileContent, FileFormat, PartitionData};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

/// Field IDs for DataFile schema (matching Java implementation)
pub mod field_ids {
    pub const CONTENT: i32 = 134;
    pub const FILE_PATH: i32 = 100;
    pub const FILE_FORMAT: i32 = 101;
    pub const PARTITION: i32 = 102;
    pub const RECORD_COUNT: i32 = 103;
    pub const FILE_SIZE: i32 = 104;
    pub const COLUMN_SIZES: i32 = 108;
    pub const VALUE_COUNTS: i32 = 109;
    pub const NULL_VALUE_COUNTS: i32 = 110;
    pub const NAN_VALUE_COUNTS: i32 = 137;
    pub const LOWER_BOUNDS: i32 = 125;
    pub const UPPER_BOUNDS: i32 = 128;
    pub const KEY_METADATA: i32 = 131;
    pub const SPLIT_OFFSETS: i32 = 132;
    pub const EQUALITY_IDS: i32 = 135;
    pub const SORT_ORDER_ID: i32 = 140;
    pub const SPEC_ID: i32 = 141;
    pub const FIRST_ROW_ID: i32 = 142;
    pub const REFERENCED_DATA_FILE: i32 = 143;
    pub const CONTENT_OFFSET: i32 = 144;
    pub const CONTENT_SIZE: i32 = 145;
}

/// Common trait for content files (data files and delete files)
pub trait ContentFile: Clone + Send + Sync {
    /// File content type (DATA, POSITION_DELETES, EQUALITY_DELETES)
    fn content(&self) -> FileContent;

    /// File path (URI with FS scheme)
    fn file_path(&self) -> &str;

    /// File format
    fn file_format(&self) -> FileFormat;

    /// Partition spec ID
    fn spec_id(&self) -> i32;

    /// Partition data tuple
    fn partition(&self) -> &PartitionData;

    /// Number of records in the file
    fn record_count(&self) -> i64;

    /// File size in bytes
    fn file_size_in_bytes(&self) -> i64;

    /// Map of column ID to size in bytes
    fn column_sizes(&self) -> Option<&FxHashMap<i32, i64>>;

    /// Map of column ID to value count
    fn value_counts(&self) -> Option<&FxHashMap<i32, i64>>;

    /// Map of column ID to null value count
    fn null_value_counts(&self) -> Option<&FxHashMap<i32, i64>>;

    /// Map of column ID to NaN value count
    fn nan_value_counts(&self) -> Option<&FxHashMap<i32, i64>>;

    /// Map of column ID to lower bound
    fn lower_bounds(&self) -> Option<&FxHashMap<i32, Vec<u8>>>;

    /// Map of column ID to upper bound
    fn upper_bounds(&self) -> Option<&FxHashMap<i32, Vec<u8>>>;

    /// Encryption key metadata
    fn key_metadata(&self) -> Option<&[u8]>;

    /// Split offsets for file splitting
    fn split_offsets(&self) -> Option<&[i64]>;

    /// Sort order ID
    fn sort_order_id(&self) -> Option<i32>;

    /// First row ID for Row Lineage (V3+)
    fn first_row_id(&self) -> Option<i64>;

    /// Data sequence number (when the file was added)
    fn data_sequence_number(&self) -> Option<i64>;

    /// File sequence number
    fn file_sequence_number(&self) -> Option<i64>;

    /// Create a copy without statistics (for memory efficiency)
    fn copy_without_stats(&self) -> Self;
}

/// Internal storage for content file data
///
/// This struct is optimized for minimal memory allocation:
/// - Uses FxHashMap for faster hashing
/// - Uses SmallVec for split_offsets (usually small)
/// - Optional fields are stored as Option to avoid allocation when empty
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentFileData {
    /// File content type
    pub content: FileContent,

    /// File path (URI)
    pub file_path: String,

    /// File format
    pub file_format: FileFormat,

    /// Partition spec ID
    pub spec_id: i32,

    /// Partition data
    pub partition: PartitionData,

    /// Number of records
    pub record_count: i64,

    /// File size in bytes
    pub file_size_in_bytes: i64,

    // === Statistics (optional) ===
    /// Column sizes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column_sizes: Option<FxHashMap<i32, i64>>,

    /// Value counts
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_counts: Option<FxHashMap<i32, i64>>,

    /// Null value counts
    #[serde(skip_serializing_if = "Option::is_none")]
    pub null_value_counts: Option<FxHashMap<i32, i64>>,

    /// NaN value counts
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nan_value_counts: Option<FxHashMap<i32, i64>>,

    /// Lower bounds (column_id -> serialized value)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lower_bounds: Option<FxHashMap<i32, Vec<u8>>>,

    /// Upper bounds (column_id -> serialized value)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upper_bounds: Option<FxHashMap<i32, Vec<u8>>>,

    // === Other optional fields ===
    /// Encryption key metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_metadata: Option<Vec<u8>>,

    /// Split offsets (uses SmallVec for common case of few offsets)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub split_offsets: Option<SmallVec<[i64; 8]>>,

    /// Sort order ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_order_id: Option<i32>,

    /// First row ID (V3 Row Lineage)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_row_id: Option<i64>,

    /// Data sequence number
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_sequence_number: Option<i64>,

    /// File sequence number
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_sequence_number: Option<i64>,

    // === Delete file specific fields ===
    /// Equality field IDs (for equality deletes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub equality_field_ids: Option<SmallVec<[i32; 4]>>,

    /// Referenced data file path (for position deletes / DVs)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub referenced_data_file: Option<String>,

    /// Content offset within file (for DVs)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_offset: Option<i64>,

    /// Content size in bytes (for DVs)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_size_in_bytes: Option<i64>,
}

impl Default for ContentFileData {
    fn default() -> Self {
        Self {
            content: FileContent::Data,
            file_path: String::new(),
            file_format: FileFormat::Parquet,
            spec_id: 0,
            partition: PartitionData::empty(),
            record_count: 0,
            file_size_in_bytes: 0,
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            key_metadata: None,
            split_offsets: None,
            sort_order_id: None,
            first_row_id: None,
            data_sequence_number: None,
            file_sequence_number: None,
            equality_field_ids: None,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }
}

impl ContentFileData {
    /// Create a new data file
    pub fn new_data_file(
        file_path: String,
        file_format: FileFormat,
        spec_id: i32,
        partition: PartitionData,
        record_count: i64,
        file_size_in_bytes: i64,
    ) -> Self {
        Self {
            content: FileContent::Data,
            file_path,
            file_format,
            spec_id,
            partition,
            record_count,
            file_size_in_bytes,
            ..Default::default()
        }
    }

    /// Create a new position delete file
    pub fn new_position_delete_file(
        file_path: String,
        file_format: FileFormat,
        spec_id: i32,
        partition: PartitionData,
        record_count: i64,
        file_size_in_bytes: i64,
        referenced_data_file: Option<String>,
    ) -> Self {
        Self {
            content: FileContent::PositionDeletes,
            file_path,
            file_format,
            spec_id,
            partition,
            record_count,
            file_size_in_bytes,
            referenced_data_file,
            ..Default::default()
        }
    }

    /// Create a new equality delete file
    pub fn new_equality_delete_file(
        file_path: String,
        file_format: FileFormat,
        spec_id: i32,
        partition: PartitionData,
        record_count: i64,
        file_size_in_bytes: i64,
        equality_field_ids: Vec<i32>,
    ) -> Self {
        Self {
            content: FileContent::EqualityDeletes,
            file_path,
            file_format,
            spec_id,
            partition,
            record_count,
            file_size_in_bytes,
            equality_field_ids: Some(SmallVec::from_vec(equality_field_ids)),
            ..Default::default()
        }
    }

    /// Copy without statistics (for memory efficiency when stats aren't needed)
    pub fn copy_without_stats(&self) -> Self {
        Self {
            content: self.content,
            file_path: self.file_path.clone(),
            file_format: self.file_format,
            spec_id: self.spec_id,
            partition: self.partition.clone(),
            record_count: self.record_count,
            file_size_in_bytes: self.file_size_in_bytes,
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            key_metadata: self.key_metadata.clone(),
            split_offsets: self.split_offsets.clone(),
            sort_order_id: self.sort_order_id,
            first_row_id: self.first_row_id,
            data_sequence_number: self.data_sequence_number,
            file_sequence_number: self.file_sequence_number,
            equality_field_ids: self.equality_field_ids.clone(),
            referenced_data_file: self.referenced_data_file.clone(),
            content_offset: self.content_offset,
            content_size_in_bytes: self.content_size_in_bytes,
        }
    }
}

/// Data file - represents a file containing table data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFile {
    data: ContentFileData,
}

impl DataFile {
    /// Create a new data file
    pub fn new(
        file_path: String,
        file_format: FileFormat,
        spec_id: i32,
        partition: PartitionData,
        record_count: i64,
        file_size_in_bytes: i64,
    ) -> Self {
        Self {
            data: ContentFileData::new_data_file(
                file_path,
                file_format,
                spec_id,
                partition,
                record_count,
                file_size_in_bytes,
            ),
        }
    }

    /// Create from raw data (used by reader)
    pub(crate) fn from_data(data: ContentFileData) -> Self {
        debug_assert!(data.content == FileContent::Data);
        Self { data }
    }

    /// Get mutable access to underlying data (for builder pattern)
    pub fn data_mut(&mut self) -> &mut ContentFileData {
        &mut self.data
    }

    /// Set first row ID (for Row Lineage)
    pub fn set_first_row_id(&mut self, first_row_id: Option<i64>) {
        self.data.first_row_id = first_row_id;
    }
}

impl From<ContentFileData> for DataFile {
    fn from(data: ContentFileData) -> Self {
        Self { data }
    }
}

impl ContentFile for DataFile {
    fn content(&self) -> FileContent {
        FileContent::Data
    }

    fn file_path(&self) -> &str {
        &self.data.file_path
    }

    fn file_format(&self) -> FileFormat {
        self.data.file_format
    }

    fn spec_id(&self) -> i32 {
        self.data.spec_id
    }

    fn partition(&self) -> &PartitionData {
        &self.data.partition
    }

    fn record_count(&self) -> i64 {
        self.data.record_count
    }

    fn file_size_in_bytes(&self) -> i64 {
        self.data.file_size_in_bytes
    }

    fn column_sizes(&self) -> Option<&FxHashMap<i32, i64>> {
        self.data.column_sizes.as_ref()
    }

    fn value_counts(&self) -> Option<&FxHashMap<i32, i64>> {
        self.data.value_counts.as_ref()
    }

    fn null_value_counts(&self) -> Option<&FxHashMap<i32, i64>> {
        self.data.null_value_counts.as_ref()
    }

    fn nan_value_counts(&self) -> Option<&FxHashMap<i32, i64>> {
        self.data.nan_value_counts.as_ref()
    }

    fn lower_bounds(&self) -> Option<&FxHashMap<i32, Vec<u8>>> {
        self.data.lower_bounds.as_ref()
    }

    fn upper_bounds(&self) -> Option<&FxHashMap<i32, Vec<u8>>> {
        self.data.upper_bounds.as_ref()
    }

    fn key_metadata(&self) -> Option<&[u8]> {
        self.data.key_metadata.as_deref()
    }

    fn split_offsets(&self) -> Option<&[i64]> {
        self.data.split_offsets.as_deref()
    }

    fn sort_order_id(&self) -> Option<i32> {
        self.data.sort_order_id
    }

    fn first_row_id(&self) -> Option<i64> {
        self.data.first_row_id
    }

    fn data_sequence_number(&self) -> Option<i64> {
        self.data.data_sequence_number
    }

    fn file_sequence_number(&self) -> Option<i64> {
        self.data.file_sequence_number
    }

    fn copy_without_stats(&self) -> Self {
        Self {
            data: self.data.copy_without_stats(),
        }
    }
}

/// Delete file - represents a file containing delete records
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteFile {
    data: ContentFileData,
}

impl DeleteFile {
    /// Create a new position delete file
    pub fn new_position_deletes(
        file_path: String,
        file_format: FileFormat,
        spec_id: i32,
        partition: PartitionData,
        record_count: i64,
        file_size_in_bytes: i64,
        referenced_data_file: Option<String>,
    ) -> Self {
        Self {
            data: ContentFileData::new_position_delete_file(
                file_path,
                file_format,
                spec_id,
                partition,
                record_count,
                file_size_in_bytes,
                referenced_data_file,
            ),
        }
    }

    /// Create a new equality delete file
    pub fn new_equality_deletes(
        file_path: String,
        file_format: FileFormat,
        spec_id: i32,
        partition: PartitionData,
        record_count: i64,
        file_size_in_bytes: i64,
        equality_field_ids: Vec<i32>,
    ) -> Self {
        Self {
            data: ContentFileData::new_equality_delete_file(
                file_path,
                file_format,
                spec_id,
                partition,
                record_count,
                file_size_in_bytes,
                equality_field_ids,
            ),
        }
    }

    /// Create from raw data (used by reader)
    pub(crate) fn from_data(data: ContentFileData) -> Self {
        debug_assert!(
            data.content == FileContent::PositionDeletes
                || data.content == FileContent::EqualityDeletes
        );
        Self { data }
    }

    /// Get equality field IDs (for equality deletes)
    pub fn equality_field_ids(&self) -> Option<&[i32]> {
        self.data.equality_field_ids.as_deref()
    }

    /// Get referenced data file (for position deletes / DVs)
    pub fn referenced_data_file(&self) -> Option<&str> {
        self.data.referenced_data_file.as_deref()
    }

    /// Get content offset (for DVs)
    pub fn content_offset(&self) -> Option<i64> {
        self.data.content_offset
    }

    /// Get content size in bytes (for DVs)
    pub fn content_size_in_bytes(&self) -> Option<i64> {
        self.data.content_size_in_bytes
    }

    /// Check if this is a deletion vector
    pub fn is_deletion_vector(&self) -> bool {
        self.data.content == FileContent::PositionDeletes
            && self.data.file_format == FileFormat::Puffin
            && self.data.referenced_data_file.is_some()
    }
}

impl From<ContentFileData> for DeleteFile {
    fn from(data: ContentFileData) -> Self {
        Self { data }
    }
}

impl ContentFile for DeleteFile {
    fn content(&self) -> FileContent {
        self.data.content
    }

    fn file_path(&self) -> &str {
        &self.data.file_path
    }

    fn file_format(&self) -> FileFormat {
        self.data.file_format
    }

    fn spec_id(&self) -> i32 {
        self.data.spec_id
    }

    fn partition(&self) -> &PartitionData {
        &self.data.partition
    }

    fn record_count(&self) -> i64 {
        self.data.record_count
    }

    fn file_size_in_bytes(&self) -> i64 {
        self.data.file_size_in_bytes
    }

    fn column_sizes(&self) -> Option<&FxHashMap<i32, i64>> {
        self.data.column_sizes.as_ref()
    }

    fn value_counts(&self) -> Option<&FxHashMap<i32, i64>> {
        self.data.value_counts.as_ref()
    }

    fn null_value_counts(&self) -> Option<&FxHashMap<i32, i64>> {
        self.data.null_value_counts.as_ref()
    }

    fn nan_value_counts(&self) -> Option<&FxHashMap<i32, i64>> {
        self.data.nan_value_counts.as_ref()
    }

    fn lower_bounds(&self) -> Option<&FxHashMap<i32, Vec<u8>>> {
        self.data.lower_bounds.as_ref()
    }

    fn upper_bounds(&self) -> Option<&FxHashMap<i32, Vec<u8>>> {
        self.data.upper_bounds.as_ref()
    }

    fn key_metadata(&self) -> Option<&[u8]> {
        self.data.key_metadata.as_deref()
    }

    fn split_offsets(&self) -> Option<&[i64]> {
        self.data.split_offsets.as_deref()
    }

    fn sort_order_id(&self) -> Option<i32> {
        self.data.sort_order_id
    }

    fn first_row_id(&self) -> Option<i64> {
        self.data.first_row_id
    }

    fn data_sequence_number(&self) -> Option<i64> {
        self.data.data_sequence_number
    }

    fn file_sequence_number(&self) -> Option<i64> {
        self.data.file_sequence_number
    }

    fn copy_without_stats(&self) -> Self {
        Self {
            data: self.data.copy_without_stats(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_file_creation() {
        let file = DataFile::new(
            "s3://bucket/data/file.parquet".to_string(),
            FileFormat::Parquet,
            0,
            PartitionData::empty(),
            1000,
            1024 * 1024,
        );

        assert_eq!(file.content(), FileContent::Data);
        assert_eq!(file.file_path(), "s3://bucket/data/file.parquet");
        assert_eq!(file.file_format(), FileFormat::Parquet);
        assert_eq!(file.record_count(), 1000);
        assert_eq!(file.file_size_in_bytes(), 1024 * 1024);
        assert!(file.first_row_id().is_none());
    }

    #[test]
    fn test_delete_file_creation() {
        let file = DeleteFile::new_position_deletes(
            "s3://bucket/data/deletes.puffin".to_string(),
            FileFormat::Puffin,
            0,
            PartitionData::empty(),
            10,
            1024,
            Some("s3://bucket/data/file.parquet".to_string()),
        );

        assert_eq!(file.content(), FileContent::PositionDeletes);
        assert!(file.is_deletion_vector());
        assert_eq!(
            file.referenced_data_file(),
            Some("s3://bucket/data/file.parquet")
        );
    }

    #[test]
    fn test_copy_without_stats() {
        let mut file = DataFile::new(
            "s3://bucket/data/file.parquet".to_string(),
            FileFormat::Parquet,
            0,
            PartitionData::empty(),
            1000,
            1024 * 1024,
        );

        // Add some stats
        file.data_mut().column_sizes = Some(FxHashMap::from_iter([(1, 100), (2, 200)]));
        file.data_mut().value_counts = Some(FxHashMap::from_iter([(1, 1000), (2, 1000)]));

        // Copy without stats
        let copy = file.copy_without_stats();

        // Verify stats are removed
        assert!(copy.column_sizes().is_none());
        assert!(copy.value_counts().is_none());

        // Verify other fields are preserved
        assert_eq!(copy.file_path(), file.file_path());
        assert_eq!(copy.record_count(), file.record_count());
    }
}
