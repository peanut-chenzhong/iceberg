//! Iceberg Snapshot types
//!
//! This module defines snapshot and snapshot reference types.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Snapshot operation type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Append,
    Replace,
    Overwrite,
    Delete,
}

/// Snapshot summary
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotSummary {
    /// Operation type
    pub operation: Operation,
    /// Additional summary properties
    #[serde(flatten)]
    pub other: HashMap<String, String>,
}

/// A table snapshot
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    /// Unique snapshot ID
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    /// ID of the parent snapshot
    #[serde(rename = "parent-snapshot-id", skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<i64>,
    /// Sequence number (V2+)
    #[serde(rename = "sequence-number", skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<i64>,
    /// Timestamp when the snapshot was created
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    /// Location of the manifest list file
    #[serde(rename = "manifest-list", skip_serializing_if = "Option::is_none")]
    pub manifest_list: Option<String>,
    /// Manifests (deprecated in V2, use manifest-list instead)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifests: Option<Vec<String>>,
    /// Snapshot summary
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<SnapshotSummary>,
    /// Schema ID used for this snapshot
    #[serde(rename = "schema-id", skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
    /// First row ID for this snapshot (V3+)
    #[serde(rename = "first-row-id", skip_serializing_if = "Option::is_none")]
    pub first_row_id: Option<i64>,
}

impl Snapshot {
    /// Get the manifest list location
    pub fn manifest_list_location(&self) -> Option<&str> {
        self.manifest_list.as_deref()
    }

    /// Check if this is the initial snapshot (no parent)
    pub fn is_initial(&self) -> bool {
        self.parent_snapshot_id.is_none()
    }

    /// Get summary operation
    pub fn operation(&self) -> Option<&Operation> {
        self.summary.as_ref().map(|s| &s.operation)
    }
}

/// Snapshot reference type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotRefType {
    Branch,
    Tag,
}

/// A reference to a snapshot (branch or tag)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotRef {
    /// The snapshot ID this reference points to
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    /// Reference type (branch or tag)
    #[serde(rename = "type")]
    pub ref_type: SnapshotRefType,
    /// Maximum age of the reference in milliseconds (for GC)
    #[serde(rename = "max-ref-age-ms", skip_serializing_if = "Option::is_none")]
    pub max_ref_age_ms: Option<i64>,
    /// Maximum snapshot age in milliseconds (for branches)
    #[serde(rename = "max-snapshot-age-ms", skip_serializing_if = "Option::is_none")]
    pub max_snapshot_age_ms: Option<i64>,
    /// Minimum number of snapshots to keep (for branches)
    #[serde(rename = "min-snapshots-to-keep", skip_serializing_if = "Option::is_none")]
    pub min_snapshots_to_keep: Option<i32>,
}

impl SnapshotRef {
    /// Create a new branch reference
    pub fn branch(snapshot_id: i64) -> Self {
        Self {
            snapshot_id,
            ref_type: SnapshotRefType::Branch,
            max_ref_age_ms: None,
            max_snapshot_age_ms: None,
            min_snapshots_to_keep: None,
        }
    }

    /// Create a new tag reference
    pub fn tag(snapshot_id: i64) -> Self {
        Self {
            snapshot_id,
            ref_type: SnapshotRefType::Tag,
            max_ref_age_ms: None,
            max_snapshot_age_ms: None,
            min_snapshots_to_keep: None,
        }
    }

    /// Check if this is a branch
    pub fn is_branch(&self) -> bool {
        self.ref_type == SnapshotRefType::Branch
    }

    /// Check if this is a tag
    pub fn is_tag(&self) -> bool {
        self.ref_type == SnapshotRefType::Tag
    }
}

/// Snapshot log entry (history)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotLogEntry {
    /// Timestamp when the snapshot became current
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    /// Snapshot ID
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_snapshot() {
        let json = r#"
        {
            "snapshot-id": 3051729675574597004,
            "parent-snapshot-id": 1234567890,
            "sequence-number": 10,
            "timestamp-ms": 1515100955770,
            "manifest-list": "s3://bucket/path/snap-3051729675574597004-1-9b1a0d47-6315-4c10-8d9a-7d2b7c3f4f3a.avro",
            "summary": {
                "operation": "append"
            }
        }
        "#;

        let snapshot: Snapshot = serde_json::from_str(json).unwrap();
        assert_eq!(snapshot.snapshot_id, 3051729675574597004);
        assert_eq!(snapshot.parent_snapshot_id, Some(1234567890));
        assert_eq!(snapshot.sequence_number, Some(10));
        assert!(snapshot.manifest_list.is_some());
        assert!(!snapshot.is_initial());
    }

    #[test]
    fn test_parse_initial_snapshot() {
        let json = r#"
        {
            "snapshot-id": 1,
            "timestamp-ms": 1515100955770,
            "manifest-list": "s3://bucket/path/snap-1.avro"
        }
        "#;

        let snapshot: Snapshot = serde_json::from_str(json).unwrap();
        assert!(snapshot.is_initial());
        assert!(snapshot.parent_snapshot_id.is_none());
    }

    #[test]
    fn test_parse_snapshot_ref_branch() {
        let json = r#"
        {
            "snapshot-id": 123456,
            "type": "branch",
            "max-snapshot-age-ms": 86400000,
            "min-snapshots-to-keep": 10
        }
        "#;

        let ref_: SnapshotRef = serde_json::from_str(json).unwrap();
        assert_eq!(ref_.snapshot_id, 123456);
        assert!(ref_.is_branch());
        assert_eq!(ref_.max_snapshot_age_ms, Some(86400000));
        assert_eq!(ref_.min_snapshots_to_keep, Some(10));
    }

    #[test]
    fn test_parse_snapshot_ref_tag() {
        let json = r#"
        {
            "snapshot-id": 789,
            "type": "tag"
        }
        "#;

        let ref_: SnapshotRef = serde_json::from_str(json).unwrap();
        assert!(ref_.is_tag());
    }

    #[test]
    fn test_parse_snapshot_log() {
        let json = r#"
        [
            {"timestamp-ms": 1515100955770, "snapshot-id": 1},
            {"timestamp-ms": 1515100956000, "snapshot-id": 2}
        ]
        "#;

        let log: Vec<SnapshotLogEntry> = serde_json::from_str(json).unwrap();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].snapshot_id, 1);
        assert_eq!(log[1].snapshot_id, 2);
    }
}
