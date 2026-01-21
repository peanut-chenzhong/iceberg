//! Core Iceberg types
//!
//! This module defines the fundamental types used throughout Iceberg,
//! following the Iceberg specification.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// File format enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileFormat {
    Avro,
    Parquet,
    Orc,
    Puffin,
}

impl FileFormat {
    /// Parse file format from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "avro" => Some(FileFormat::Avro),
            "parquet" => Some(FileFormat::Parquet),
            "orc" => Some(FileFormat::Orc),
            "puffin" => Some(FileFormat::Puffin),
            _ => None,
        }
    }

    /// Get file format from file extension
    pub fn from_extension(path: &str) -> Option<Self> {
        if path.ends_with(".avro") {
            Some(FileFormat::Avro)
        } else if path.ends_with(".parquet") {
            Some(FileFormat::Parquet)
        } else if path.ends_with(".orc") {
            Some(FileFormat::Orc)
        } else if path.ends_with(".puffin") {
            Some(FileFormat::Puffin)
        } else {
            None
        }
    }
}

impl fmt::Display for FileFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileFormat::Avro => write!(f, "avro"),
            FileFormat::Parquet => write!(f, "parquet"),
            FileFormat::Orc => write!(f, "orc"),
            FileFormat::Puffin => write!(f, "puffin"),
        }
    }
}

/// Content type of a file
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i32)]
pub enum FileContent {
    /// Data file containing table rows
    Data = 0,
    /// Position delete file
    PositionDeletes = 1,
    /// Equality delete file
    EqualityDeletes = 2,
}

impl FileContent {
    /// Create from integer ID (as stored in Avro)
    pub fn from_id(id: i32) -> Option<Self> {
        match id {
            0 => Some(FileContent::Data),
            1 => Some(FileContent::PositionDeletes),
            2 => Some(FileContent::EqualityDeletes),
            _ => None,
        }
    }

    /// Get integer ID
    pub fn id(&self) -> i32 {
        *self as i32
    }
}

impl Default for FileContent {
    fn default() -> Self {
        FileContent::Data
    }
}

/// Partition field value
///
/// Represents a single value in a partition tuple.
/// Uses an enum to avoid boxing for common types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PartitionValue {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(String),
    Binary(Vec<u8>),
    Date(i32),      // Days since epoch
    Time(i64),      // Microseconds since midnight
    Timestamp(i64), // Microseconds since epoch
    Uuid(uuid::Uuid),
}

impl std::hash::Hash for PartitionValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            PartitionValue::Null => {}
            PartitionValue::Boolean(b) => b.hash(state),
            PartitionValue::Int(i) => i.hash(state),
            PartitionValue::Long(l) => l.hash(state),
            PartitionValue::Float(f) => f.to_bits().hash(state),
            PartitionValue::Double(d) => d.to_bits().hash(state),
            PartitionValue::String(s) => s.hash(state),
            PartitionValue::Binary(b) => b.hash(state),
            PartitionValue::Date(d) => d.hash(state),
            PartitionValue::Time(t) => t.hash(state),
            PartitionValue::Timestamp(t) => t.hash(state),
            PartitionValue::Uuid(u) => u.hash(state),
        }
    }
}

impl Eq for PartitionValue {}

impl PartitionValue {
    /// Check if value is null
    pub fn is_null(&self) -> bool {
        matches!(self, PartitionValue::Null)
    }
}

/// Partition data - tuple of partition field values
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartitionData {
    /// Partition field values in spec order
    values: Vec<PartitionValue>,
}

impl PartitionData {
    /// Create empty partition data (for unpartitioned tables)
    pub fn empty() -> Self {
        Self { values: Vec::new() }
    }

    /// Create partition data with given values
    pub fn new(values: Vec<PartitionValue>) -> Self {
        Self { values }
    }

    /// Get partition value at index
    pub fn get(&self, index: usize) -> Option<&PartitionValue> {
        self.values.get(index)
    }

    /// Number of partition fields
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if partition is empty (unpartitioned)
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get all values as a slice
    pub fn values(&self) -> &[PartitionValue] {
        &self.values
    }
}

/// Column statistics bounds
///
/// Uses bytes to store bounds in their serialized form,
/// avoiding expensive type conversions during scanning.
#[derive(Debug, Clone, Default)]
pub struct ColumnBounds {
    /// Lower bound value (serialized)
    pub lower: Option<Vec<u8>>,
    /// Upper bound value (serialized)
    pub upper: Option<Vec<u8>>,
}

/// Column-level statistics
#[derive(Debug, Clone, Default)]
pub struct ColumnStats {
    /// Column ID
    pub column_id: i32,
    /// Size in bytes (optional)
    pub size_in_bytes: Option<i64>,
    /// Total value count including nulls
    pub value_count: Option<i64>,
    /// Null value count
    pub null_value_count: Option<i64>,
    /// NaN value count (for floating point)
    pub nan_value_count: Option<i64>,
    /// Bounds
    pub bounds: ColumnBounds,
}

/// File metrics/statistics
#[derive(Debug, Clone, Default)]
pub struct FileMetrics {
    /// Record count
    pub record_count: i64,
    /// Per-column statistics (column_id -> stats)
    pub column_stats: HashMap<i32, ColumnStats>,
}

impl FileMetrics {
    /// Create new metrics with record count
    pub fn new(record_count: i64) -> Self {
        Self {
            record_count,
            column_stats: HashMap::new(),
        }
    }

    /// Get column sizes map
    pub fn column_sizes(&self) -> HashMap<i32, i64> {
        self.column_stats
            .iter()
            .filter_map(|(id, stats)| stats.size_in_bytes.map(|size| (*id, size)))
            .collect()
    }

    /// Get value counts map
    pub fn value_counts(&self) -> HashMap<i32, i64> {
        self.column_stats
            .iter()
            .filter_map(|(id, stats)| stats.value_count.map(|count| (*id, count)))
            .collect()
    }

    /// Get null value counts map
    pub fn null_value_counts(&self) -> HashMap<i32, i64> {
        self.column_stats
            .iter()
            .filter_map(|(id, stats)| stats.null_value_count.map(|count| (*id, count)))
            .collect()
    }

    /// Get lower bounds map
    pub fn lower_bounds(&self) -> HashMap<i32, Vec<u8>> {
        self.column_stats
            .iter()
            .filter_map(|(id, stats)| stats.bounds.lower.clone().map(|b| (*id, b)))
            .collect()
    }

    /// Get upper bounds map
    pub fn upper_bounds(&self) -> HashMap<i32, Vec<u8>> {
        self.column_stats
            .iter()
            .filter_map(|(id, stats)| stats.bounds.upper.clone().map(|b| (*id, b)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_format_from_str() {
        assert_eq!(FileFormat::from_str("parquet"), Some(FileFormat::Parquet));
        assert_eq!(FileFormat::from_str("PARQUET"), Some(FileFormat::Parquet));
        assert_eq!(FileFormat::from_str("avro"), Some(FileFormat::Avro));
        assert_eq!(FileFormat::from_str("orc"), Some(FileFormat::Orc));
        assert_eq!(FileFormat::from_str("unknown"), None);
    }

    #[test]
    fn test_file_content_from_id() {
        assert_eq!(FileContent::from_id(0), Some(FileContent::Data));
        assert_eq!(FileContent::from_id(1), Some(FileContent::PositionDeletes));
        assert_eq!(FileContent::from_id(2), Some(FileContent::EqualityDeletes));
        assert_eq!(FileContent::from_id(3), None);
    }

    #[test]
    fn test_partition_data() {
        let partition = PartitionData::new(vec![
            PartitionValue::String("2024-01-15".to_string()),
            PartitionValue::Int(42),
        ]);
        
        assert_eq!(partition.len(), 2);
        assert!(!partition.is_empty());
        
        match partition.get(0) {
            Some(PartitionValue::String(s)) => assert_eq!(s, "2024-01-15"),
            _ => panic!("Expected string partition value"),
        }
    }
}
