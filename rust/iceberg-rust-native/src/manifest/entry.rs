//! Manifest entry types
//!
//! A manifest entry represents a single file entry in a manifest,
//! including its status (EXISTING, ADDED, DELETED) and metadata.

use super::content_file::{ContentFile, ContentFileData, DataFile, DeleteFile};
use crate::types::FileContent;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Entry status - whether the file is existing, added, or deleted
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i32)]
pub enum Status {
    /// File exists in the table
    Existing = 0,
    /// File was added in this snapshot
    Added = 1,
    /// File was deleted in this snapshot
    Deleted = 2,
}

impl Status {
    /// Create from integer ID (as stored in Avro)
    pub fn from_id(id: i32) -> Option<Self> {
        match id {
            0 => Some(Status::Existing),
            1 => Some(Status::Added),
            2 => Some(Status::Deleted),
            _ => None,
        }
    }

    /// Get integer ID
    pub fn id(&self) -> i32 {
        *self as i32
    }

    /// Check if this is a live entry (not deleted)
    pub fn is_live(&self) -> bool {
        matches!(self, Status::Existing | Status::Added)
    }
}

impl Default for Status {
    fn default() -> Self {
        Status::Existing
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Status::Existing => write!(f, "EXISTING"),
            Status::Added => write!(f, "ADDED"),
            Status::Deleted => write!(f, "DELETED"),
        }
    }
}

/// Field IDs for ManifestEntry schema
pub mod field_ids {
    pub const STATUS: i32 = 0;
    pub const SNAPSHOT_ID: i32 = 1;
    pub const DATA_FILE: i32 = 2;
    pub const SEQUENCE_NUMBER: i32 = 3;
    pub const FILE_SEQUENCE_NUMBER: i32 = 4;
}

/// Manifest entry - represents a file entry in a manifest
///
/// Generic over the file type F (DataFile or DeleteFile)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry<F: ContentFile> {
    /// Entry status
    status: Status,

    /// Snapshot ID when the file was added
    snapshot_id: Option<i64>,

    /// Data sequence number (for ordering)
    data_sequence_number: Option<i64>,

    /// File sequence number
    file_sequence_number: Option<i64>,

    /// The file metadata
    file: F,
}

impl<F: ContentFile> ManifestEntry<F> {
    /// Create a new manifest entry
    pub fn new(
        status: Status,
        snapshot_id: Option<i64>,
        data_sequence_number: Option<i64>,
        file_sequence_number: Option<i64>,
        file: F,
    ) -> Self {
        Self {
            status,
            snapshot_id,
            data_sequence_number,
            file_sequence_number,
            file,
        }
    }

    /// Get the entry status
    pub fn status(&self) -> Status {
        self.status
    }

    /// Get the snapshot ID
    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }

    /// Get the data sequence number
    pub fn data_sequence_number(&self) -> Option<i64> {
        self.data_sequence_number
    }

    /// Get the file sequence number
    pub fn file_sequence_number(&self) -> Option<i64> {
        self.file_sequence_number
    }

    /// Get reference to the file
    pub fn file(&self) -> &F {
        &self.file
    }

    /// Get mutable reference to the file
    pub fn file_mut(&mut self) -> &mut F {
        &mut self.file
    }

    /// Take ownership of the file
    pub fn into_file(self) -> F {
        self.file
    }

    /// Check if this is a live entry (not deleted)
    pub fn is_live(&self) -> bool {
        self.status.is_live()
    }

    /// Create a copy without statistics
    pub fn copy_without_stats(&self) -> Self {
        Self {
            status: self.status,
            snapshot_id: self.snapshot_id,
            data_sequence_number: self.data_sequence_number,
            file_sequence_number: self.file_sequence_number,
            file: self.file.copy_without_stats(),
        }
    }
}

/// Data manifest entry - entry containing a DataFile
pub type DataManifestEntry = ManifestEntry<DataFile>;

/// Delete manifest entry - entry containing a DeleteFile
pub type DeleteManifestEntry = ManifestEntry<DeleteFile>;

/// Raw manifest entry data used during Avro deserialization
///
/// This is an intermediate representation that can be converted
/// to either DataManifestEntry or DeleteManifestEntry
#[derive(Debug, Clone, Default)]
pub struct RawManifestEntry {
    pub status: i32,
    pub snapshot_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
    pub file_sequence_number: Option<i64>,
    pub file: ContentFileData,
}

impl RawManifestEntry {
    /// Convert to DataManifestEntry
    pub fn into_data_entry(self) -> DataManifestEntry {
        ManifestEntry {
            status: Status::from_id(self.status).unwrap_or_default(),
            snapshot_id: self.snapshot_id,
            data_sequence_number: self.data_sequence_number,
            file_sequence_number: self.file_sequence_number,
            file: DataFile::from_data(self.file),
        }
    }

    /// Convert to DeleteManifestEntry
    pub fn into_delete_entry(self) -> DeleteManifestEntry {
        ManifestEntry {
            status: Status::from_id(self.status).unwrap_or_default(),
            snapshot_id: self.snapshot_id,
            data_sequence_number: self.data_sequence_number,
            file_sequence_number: self.file_sequence_number,
            file: DeleteFile::from_data(self.file),
        }
    }

    /// Convert based on file content type
    pub fn into_typed_entry(self) -> TypedManifestEntry {
        match self.file.content {
            FileContent::Data => TypedManifestEntry::Data(self.into_data_entry()),
            FileContent::PositionDeletes | FileContent::EqualityDeletes => {
                TypedManifestEntry::Delete(self.into_delete_entry())
            }
        }
    }
}

/// Typed manifest entry - either data or delete
#[derive(Debug, Clone)]
pub enum TypedManifestEntry {
    Data(DataManifestEntry),
    Delete(DeleteManifestEntry),
}

impl TypedManifestEntry {
    /// Get the entry status
    pub fn status(&self) -> Status {
        match self {
            TypedManifestEntry::Data(e) => e.status(),
            TypedManifestEntry::Delete(e) => e.status(),
        }
    }

    /// Check if this is a live entry
    pub fn is_live(&self) -> bool {
        match self {
            TypedManifestEntry::Data(e) => e.is_live(),
            TypedManifestEntry::Delete(e) => e.is_live(),
        }
    }

    /// Get the file path
    pub fn file_path(&self) -> &str {
        match self {
            TypedManifestEntry::Data(e) => e.file().file_path(),
            TypedManifestEntry::Delete(e) => e.file().file_path(),
        }
    }

    /// Get record count
    pub fn record_count(&self) -> i64 {
        match self {
            TypedManifestEntry::Data(e) => e.file().record_count(),
            TypedManifestEntry::Delete(e) => e.file().record_count(),
        }
    }

    /// Check if this is a data entry
    pub fn is_data(&self) -> bool {
        matches!(self, TypedManifestEntry::Data(_))
    }

    /// Check if this is a delete entry
    pub fn is_delete(&self) -> bool {
        matches!(self, TypedManifestEntry::Delete(_))
    }

    /// Get as data entry if applicable
    pub fn as_data(&self) -> Option<&DataManifestEntry> {
        match self {
            TypedManifestEntry::Data(e) => Some(e),
            _ => None,
        }
    }

    /// Get as delete entry if applicable
    pub fn as_delete(&self) -> Option<&DeleteManifestEntry> {
        match self {
            TypedManifestEntry::Delete(e) => Some(e),
            _ => None,
        }
    }
}

/// Builder for creating manifest entries
pub struct ManifestEntryBuilder<F: ContentFile> {
    status: Status,
    snapshot_id: Option<i64>,
    data_sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
    file: Option<F>,
}

impl<F: ContentFile> Default for ManifestEntryBuilder<F> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F: ContentFile> ManifestEntryBuilder<F> {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            status: Status::Added,
            snapshot_id: None,
            data_sequence_number: None,
            file_sequence_number: None,
            file: None,
        }
    }

    /// Set the status
    pub fn status(mut self, status: Status) -> Self {
        self.status = status;
        self
    }

    /// Set the snapshot ID
    pub fn snapshot_id(mut self, id: i64) -> Self {
        self.snapshot_id = Some(id);
        self
    }

    /// Set the data sequence number
    pub fn data_sequence_number(mut self, seq: i64) -> Self {
        self.data_sequence_number = Some(seq);
        self
    }

    /// Set the file sequence number
    pub fn file_sequence_number(mut self, seq: i64) -> Self {
        self.file_sequence_number = Some(seq);
        self
    }

    /// Set the file
    pub fn file(mut self, file: F) -> Self {
        self.file = Some(file);
        self
    }

    /// Build the entry
    pub fn build(self) -> Option<ManifestEntry<F>> {
        Some(ManifestEntry {
            status: self.status,
            snapshot_id: self.snapshot_id,
            data_sequence_number: self.data_sequence_number,
            file_sequence_number: self.file_sequence_number,
            file: self.file?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{FileFormat, PartitionData};

    #[test]
    fn test_status_from_id() {
        assert_eq!(Status::from_id(0), Some(Status::Existing));
        assert_eq!(Status::from_id(1), Some(Status::Added));
        assert_eq!(Status::from_id(2), Some(Status::Deleted));
        assert_eq!(Status::from_id(3), None);
    }

    #[test]
    fn test_status_is_live() {
        assert!(Status::Existing.is_live());
        assert!(Status::Added.is_live());
        assert!(!Status::Deleted.is_live());
    }

    #[test]
    fn test_manifest_entry() {
        let file = DataFile::new(
            "s3://bucket/file.parquet".to_string(),
            FileFormat::Parquet,
            0,
            PartitionData::empty(),
            1000,
            1024,
        );

        let entry = ManifestEntry::new(Status::Added, Some(123), Some(1), Some(1), file);

        assert_eq!(entry.status(), Status::Added);
        assert_eq!(entry.snapshot_id(), Some(123));
        assert!(entry.is_live());
        assert_eq!(entry.file().file_path(), "s3://bucket/file.parquet");
    }

    #[test]
    fn test_entry_builder() {
        let file = DataFile::new(
            "s3://bucket/file.parquet".to_string(),
            FileFormat::Parquet,
            0,
            PartitionData::empty(),
            1000,
            1024,
        );

        let entry = ManifestEntryBuilder::new()
            .status(Status::Added)
            .snapshot_id(123)
            .data_sequence_number(1)
            .file(file)
            .build()
            .unwrap();

        assert_eq!(entry.status(), Status::Added);
        assert_eq!(entry.snapshot_id(), Some(123));
    }
}
