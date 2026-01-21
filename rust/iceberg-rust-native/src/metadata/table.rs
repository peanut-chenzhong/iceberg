//! Table Metadata
//!
//! This module defines the main TableMetadata structure and parser.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use crate::error::{Error, Result};
use super::schema::Schema;
use super::snapshot::{Snapshot, SnapshotLogEntry, SnapshotRef};
use super::spec::{PartitionSpec, SortOrder};

/// Metadata log entry
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataLogEntry {
    /// Timestamp when the metadata file was created
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    /// Path to the metadata file
    #[serde(rename = "metadata-file")]
    pub metadata_file: String,
}

/// Statistics file reference
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatisticsFile {
    /// Snapshot ID the statistics are for
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    /// Path to the statistics file
    #[serde(rename = "statistics-path")]
    pub statistics_path: String,
    /// File size in bytes
    #[serde(rename = "file-size-in-bytes")]
    pub file_size_in_bytes: i64,
    /// Footer size in bytes
    #[serde(rename = "file-footer-size-in-bytes")]
    pub file_footer_size_in_bytes: i64,
    /// Blob metadata
    #[serde(rename = "blob-metadata", default)]
    pub blob_metadata: Vec<BlobMetadata>,
}

/// Blob metadata in statistics file
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BlobMetadata {
    /// Blob type
    #[serde(rename = "type")]
    pub blob_type: String,
    /// Snapshot ID
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    /// Sequence number
    #[serde(rename = "sequence-number")]
    pub sequence_number: i64,
    /// Field IDs
    pub fields: Vec<i32>,
    /// Other properties
    #[serde(flatten)]
    pub properties: HashMap<String, serde_json::Value>,
}

/// Partition statistics file reference
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionStatisticsFile {
    /// Snapshot ID
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    /// Path to the partition statistics file
    #[serde(rename = "statistics-path")]
    pub statistics_path: String,
    /// File size in bytes
    #[serde(rename = "file-size-in-bytes")]
    pub file_size_in_bytes: i64,
}

/// Table metadata
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableMetadata {
    /// Format version (1, 2, or 3)
    #[serde(rename = "format-version")]
    pub format_version: i32,
    
    /// Table UUID
    #[serde(rename = "table-uuid")]
    pub table_uuid: String,
    
    /// Table location
    pub location: String,
    
    /// Last sequence number (V2+)
    #[serde(rename = "last-sequence-number", default)]
    pub last_sequence_number: i64,
    
    /// Last updated timestamp in milliseconds
    #[serde(rename = "last-updated-ms")]
    pub last_updated_ms: i64,
    
    /// Last assigned column ID
    #[serde(rename = "last-column-id")]
    pub last_column_id: i32,
    
    /// Current schema ID
    #[serde(rename = "current-schema-id", default)]
    pub current_schema_id: i32,
    
    /// All schemas (V2+)
    #[serde(default)]
    pub schemas: Vec<Schema>,
    
    /// Single schema (V1, deprecated)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<Schema>,
    
    /// Default partition spec ID
    #[serde(rename = "default-spec-id", default)]
    pub default_spec_id: i32,
    
    /// All partition specs (V2+)
    #[serde(rename = "partition-specs", default)]
    pub partition_specs: Vec<PartitionSpec>,
    
    /// Single partition spec (V1, deprecated)
    #[serde(rename = "partition-spec", skip_serializing_if = "Option::is_none")]
    pub partition_spec: Option<Vec<serde_json::Value>>,
    
    /// Last assigned partition field ID
    #[serde(rename = "last-partition-id", default)]
    pub last_partition_id: i32,
    
    /// Default sort order ID
    #[serde(rename = "default-sort-order-id", default)]
    pub default_sort_order_id: i32,
    
    /// All sort orders
    #[serde(rename = "sort-orders", default)]
    pub sort_orders: Vec<SortOrder>,
    
    /// Table properties
    #[serde(default)]
    pub properties: HashMap<String, String>,
    
    /// Current snapshot ID (-1 if no current snapshot)
    #[serde(rename = "current-snapshot-id", default = "default_snapshot_id")]
    pub current_snapshot_id: Option<i64>,
    
    /// All snapshots
    #[serde(default)]
    pub snapshots: Vec<Snapshot>,
    
    /// Snapshot log (history of current snapshot changes)
    #[serde(rename = "snapshot-log", default)]
    pub snapshot_log: Vec<SnapshotLogEntry>,
    
    /// Metadata log (history of metadata file changes)
    #[serde(rename = "metadata-log", default)]
    pub metadata_log: Vec<MetadataLogEntry>,
    
    /// Snapshot references (branches and tags)
    #[serde(default)]
    pub refs: HashMap<String, SnapshotRef>,
    
    /// Statistics files
    #[serde(default)]
    pub statistics: Vec<StatisticsFile>,
    
    /// Partition statistics files
    #[serde(rename = "partition-statistics", default)]
    pub partition_statistics: Vec<PartitionStatisticsFile>,
    
    /// Next row ID (V3+, for row lineage)
    #[serde(rename = "next-row-id", skip_serializing_if = "Option::is_none")]
    pub next_row_id: Option<i64>,

    // Internal: the location this metadata was read from
    #[serde(skip)]
    metadata_location: Option<String>,
}

fn default_snapshot_id() -> Option<i64> {
    None
}

impl TableMetadata {
    /// Get the current schema
    pub fn current_schema(&self) -> Option<&Schema> {
        // First try V2+ schemas list
        if !self.schemas.is_empty() {
            return self.schemas.iter().find(|s| s.schema_id == self.current_schema_id);
        }
        // Fall back to V1 schema field
        self.schema.as_ref()
    }

    /// Get the current partition spec
    pub fn current_spec(&self) -> Option<&PartitionSpec> {
        self.partition_specs.iter().find(|s| s.spec_id == self.default_spec_id)
    }

    /// Get the current sort order
    pub fn current_sort_order(&self) -> Option<&SortOrder> {
        self.sort_orders.iter().find(|s| s.order_id == self.default_sort_order_id)
    }

    /// Get the current snapshot
    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        self.current_snapshot_id
            .and_then(|id| self.snapshots.iter().find(|s| s.snapshot_id == id))
    }

    /// Get a snapshot by ID
    pub fn snapshot(&self, id: i64) -> Option<&Snapshot> {
        self.snapshots.iter().find(|s| s.snapshot_id == id)
    }

    /// Get a schema by ID
    pub fn schema(&self, id: i32) -> Option<&Schema> {
        self.schemas.iter().find(|s| s.schema_id == id)
    }

    /// Get a partition spec by ID
    pub fn spec(&self, id: i32) -> Option<&PartitionSpec> {
        self.partition_specs.iter().find(|s| s.spec_id == id)
    }

    /// Get a sort order by ID
    pub fn sort_order(&self, id: i32) -> Option<&SortOrder> {
        self.sort_orders.iter().find(|s| s.order_id == id)
    }

    /// Get a snapshot ref by name
    pub fn snapshot_ref(&self, name: &str) -> Option<&SnapshotRef> {
        self.refs.get(name)
    }

    /// Get the main branch reference
    pub fn main_branch(&self) -> Option<&SnapshotRef> {
        self.snapshot_ref("main")
    }

    /// Get a property by key
    pub fn property(&self, key: &str) -> Option<&str> {
        self.properties.get(key).map(|s| s.as_str())
    }

    /// Get the location this metadata was read from
    pub fn metadata_location(&self) -> Option<&str> {
        self.metadata_location.as_deref()
    }

    /// Check if this is a V1 format table
    pub fn is_v1(&self) -> bool {
        self.format_version == 1
    }

    /// Check if this is a V2 format table
    pub fn is_v2(&self) -> bool {
        self.format_version == 2
    }

    /// Check if this is a V3 format table
    pub fn is_v3(&self) -> bool {
        self.format_version == 3
    }

    /// Check if row lineage is enabled (V3+)
    pub fn has_row_lineage(&self) -> bool {
        self.format_version >= 3 && self.next_row_id.is_some()
    }
}

/// Parser for table metadata
pub struct TableMetadataParser;

impl TableMetadataParser {
    /// Parse table metadata from a JSON string
    pub fn from_json(json: &str) -> Result<TableMetadata> {
        serde_json::from_str(json).map_err(|e| Error::json(e.to_string()))
    }

    /// Parse table metadata from a JSON file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<TableMetadata> {
        let path = path.as_ref();
        let file = File::open(path).map_err(|e| Error::io(e.to_string()))?;

        // Check if gzipped
        let is_gzip = path
            .to_str()
            .map(|s| s.ends_with(".gz") || s.ends_with(".metadata.json.gz"))
            .unwrap_or(false);

        let mut metadata: TableMetadata = if is_gzip {
            let decoder = flate2::read::GzDecoder::new(BufReader::new(file));
            serde_json::from_reader(decoder).map_err(|e| Error::json(e.to_string()))?
        } else {
            let reader = BufReader::new(file);
            serde_json::from_reader(reader).map_err(|e| Error::json(e.to_string()))?
        };

        // Set the metadata location
        metadata.metadata_location = path.to_str().map(|s| s.to_string());

        Ok(metadata)
    }

    /// Parse table metadata from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<TableMetadata> {
        serde_json::from_slice(bytes).map_err(|e| Error::json(e.to_string()))
    }

    /// Serialize table metadata to JSON string
    pub fn to_json(metadata: &TableMetadata) -> Result<String> {
        serde_json::to_string_pretty(metadata).map_err(|e| Error::json(e.to_string()))
    }

    /// Serialize table metadata to JSON bytes
    pub fn to_bytes(metadata: &TableMetadata) -> Result<Vec<u8>> {
        serde_json::to_vec(metadata).map_err(|e| Error::json(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_METADATA_V2: &str = r#"
    {
        "format-version": 2,
        "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
        "location": "s3://bucket/warehouse/ns/table",
        "last-sequence-number": 34,
        "last-updated-ms": 1602638573590,
        "last-column-id": 3,
        "current-schema-id": 1,
        "schemas": [
            {
                "schema-id": 0,
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"}
                ]
            },
            {
                "schema-id": 1,
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"},
                    {"id": 2, "name": "data", "required": false, "type": "string"},
                    {"id": 3, "name": "category", "required": false, "type": "string"}
                ]
            }
        ],
        "default-spec-id": 0,
        "partition-specs": [
            {"spec-id": 0, "fields": []}
        ],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [
            {"order-id": 0, "fields": []}
        ],
        "properties": {
            "owner": "test_user",
            "write.format.default": "parquet"
        },
        "current-snapshot-id": 3055729675574597004,
        "snapshots": [
            {
                "snapshot-id": 3051729675574597004,
                "timestamp-ms": 1515100955770,
                "manifest-list": "s3://bucket/warehouse/ns/table/metadata/snap-3051729675574597004-1-c190f4a8.avro",
                "summary": {"operation": "append"}
            },
            {
                "snapshot-id": 3055729675574597004,
                "parent-snapshot-id": 3051729675574597004,
                "timestamp-ms": 1555100955770,
                "manifest-list": "s3://bucket/warehouse/ns/table/metadata/snap-3055729675574597004-1-c190f4a8.avro",
                "summary": {"operation": "append"},
                "schema-id": 1
            }
        ],
        "snapshot-log": [
            {"timestamp-ms": 1515100955770, "snapshot-id": 3051729675574597004},
            {"timestamp-ms": 1555100955770, "snapshot-id": 3055729675574597004}
        ],
        "metadata-log": [
            {"timestamp-ms": 1515100955770, "metadata-file": "s3://bucket/warehouse/ns/table/metadata/v1.metadata.json"}
        ],
        "refs": {
            "main": {"snapshot-id": 3055729675574597004, "type": "branch"}
        }
    }
    "#;

    #[test]
    fn test_parse_v2_metadata() {
        let metadata = TableMetadataParser::from_json(SAMPLE_METADATA_V2).unwrap();

        assert_eq!(metadata.format_version, 2);
        assert_eq!(metadata.table_uuid, "9c12d441-03fe-4693-9a96-a0705ddf69c1");
        assert_eq!(metadata.location, "s3://bucket/warehouse/ns/table");
        assert_eq!(metadata.last_sequence_number, 34);
        assert_eq!(metadata.last_column_id, 3);
        assert_eq!(metadata.current_schema_id, 1);
        assert_eq!(metadata.schemas.len(), 2);
        assert_eq!(metadata.partition_specs.len(), 1);
        assert_eq!(metadata.sort_orders.len(), 1);
        assert_eq!(metadata.snapshots.len(), 2);
        assert_eq!(metadata.snapshot_log.len(), 2);
        assert_eq!(metadata.metadata_log.len(), 1);
        assert!(metadata.refs.contains_key("main"));
    }

    #[test]
    fn test_current_schema() {
        let metadata = TableMetadataParser::from_json(SAMPLE_METADATA_V2).unwrap();
        let schema = metadata.current_schema().unwrap();
        assert_eq!(schema.schema_id, 1);
        assert_eq!(schema.fields.len(), 3);
    }

    #[test]
    fn test_current_snapshot() {
        let metadata = TableMetadataParser::from_json(SAMPLE_METADATA_V2).unwrap();
        let snapshot = metadata.current_snapshot().unwrap();
        assert_eq!(snapshot.snapshot_id, 3055729675574597004);
        assert_eq!(snapshot.parent_snapshot_id, Some(3051729675574597004));
    }

    #[test]
    fn test_properties() {
        let metadata = TableMetadataParser::from_json(SAMPLE_METADATA_V2).unwrap();
        assert_eq!(metadata.property("owner"), Some("test_user"));
        assert_eq!(metadata.property("write.format.default"), Some("parquet"));
        assert!(metadata.property("nonexistent").is_none());
    }

    #[test]
    fn test_main_branch() {
        let metadata = TableMetadataParser::from_json(SAMPLE_METADATA_V2).unwrap();
        let main = metadata.main_branch().unwrap();
        assert_eq!(main.snapshot_id, 3055729675574597004);
        assert!(main.is_branch());
    }

    #[test]
    fn test_v3_metadata_with_row_lineage() {
        let json = r#"
        {
            "format-version": 3,
            "table-uuid": "test-uuid",
            "location": "s3://bucket/table",
            "last-sequence-number": 1,
            "last-updated-ms": 1700000000000,
            "last-column-id": 1,
            "current-schema-id": 0,
            "schemas": [
                {"schema-id": 0, "type": "struct", "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]}
            ],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 999,
            "default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "current-snapshot-id": null,
            "snapshots": [],
            "next-row-id": 1000
        }
        "#;

        let metadata = TableMetadataParser::from_json(json).unwrap();
        assert!(metadata.is_v3());
        assert!(metadata.has_row_lineage());
        assert_eq!(metadata.next_row_id, Some(1000));
    }

    #[test]
    fn test_serialize_roundtrip() {
        let metadata = TableMetadataParser::from_json(SAMPLE_METADATA_V2).unwrap();
        let json = TableMetadataParser::to_json(&metadata).unwrap();
        let reparsed = TableMetadataParser::from_json(&json).unwrap();

        assert_eq!(metadata.format_version, reparsed.format_version);
        assert_eq!(metadata.table_uuid, reparsed.table_uuid);
        assert_eq!(metadata.location, reparsed.location);
        assert_eq!(metadata.current_snapshot_id, reparsed.current_snapshot_id);
    }
}
