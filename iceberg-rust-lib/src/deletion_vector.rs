//! Deletion Vector Handler (Iceberg V3)
//!
//! Deletion Vector格式 (Puffin文件中的deletion-vector-v1 blob):
//! - 4字节: 长度 + magic字节的组合长度 (big-endian)
//! - 4字节: Magic序列 0xD1 0xD3 0x39 0x64
//! - N字节: Roaring bitmap数据 (portable格式)
//! - 4字节: CRC-32校验和 (big-endian)

use crate::error::{IcebergError, Result};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// Magic bytes for deletion vector
const DELETION_VECTOR_MAGIC: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];

/// Magic bytes for Puffin file
const PUFFIN_MAGIC: [u8; 4] = [0x50, 0x46, 0x41, 0x31];

/// Deletion vector for a data file
#[derive(Debug, Clone)]
pub struct DeletionVector {
    /// Roaring treemap storing deleted positions
    pub deleted_positions: RoaringTreemap,
    /// Referenced data file path
    pub referenced_data_file: String,
    /// Cardinality (number of deleted rows)
    pub cardinality: u64,
}

impl DeletionVector {
    /// Create a new empty deletion vector
    pub fn new(referenced_data_file: impl Into<String>) -> Self {
        Self {
            deleted_positions: RoaringTreemap::new(),
            referenced_data_file: referenced_data_file.into(),
            cardinality: 0,
        }
    }

    /// Create from a roaring treemap
    pub fn from_treemap(
        treemap: RoaringTreemap,
        referenced_data_file: impl Into<String>,
    ) -> Self {
        let cardinality = treemap.len();
        Self {
            deleted_positions: treemap,
            referenced_data_file: referenced_data_file.into(),
            cardinality,
        }
    }

    /// Check if a position is deleted
    pub fn is_deleted(&self, position: u64) -> bool {
        self.deleted_positions.contains(position)
    }

    /// Add a deleted position
    pub fn add_deleted(&mut self, position: u64) {
        if self.deleted_positions.insert(position) {
            self.cardinality += 1;
        }
    }

    /// Get the number of deleted positions
    pub fn len(&self) -> u64 {
        self.cardinality
    }

    /// Check if the deletion vector is empty
    pub fn is_empty(&self) -> bool {
        self.cardinality == 0
    }

    /// Serialize deletion vector to bytes
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut bitmap_bytes = Vec::new();
        self.deleted_positions.serialize_into(&mut bitmap_bytes)
            .map_err(|e| IcebergError::invalid_deletion_vector(format!("Failed to serialize: {}", e)))?;

        let content_len = 4 + bitmap_bytes.len(); // magic + bitmap
        let mut result = Vec::with_capacity(4 + content_len + 4); // len + content + crc

        // Write length (big-endian)
        result.extend_from_slice(&(content_len as u32).to_be_bytes());

        // Write magic
        result.extend_from_slice(&DELETION_VECTOR_MAGIC);

        // Write bitmap
        result.extend_from_slice(&bitmap_bytes);

        // Calculate and write CRC-32
        let crc = crc32fast::hash(&result[4..]); // CRC of magic + bitmap
        result.extend_from_slice(&crc.to_be_bytes());

        Ok(result)
    }

    /// Deserialize deletion vector from bytes
    pub fn deserialize(
        data: &[u8],
        referenced_data_file: impl Into<String>,
    ) -> Result<Self> {
        if data.len() < 12 {
            return Err(IcebergError::invalid_deletion_vector(
                "Data too short for deletion vector",
            ));
        }

        // Read length (big-endian)
        let content_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;

        if data.len() < 4 + content_len + 4 {
            return Err(IcebergError::invalid_deletion_vector(
                "Data length mismatch",
            ));
        }

        // Verify magic bytes
        let magic = &data[4..8];
        if magic != DELETION_VECTOR_MAGIC {
            return Err(IcebergError::invalid_deletion_vector(
                format!("Invalid magic bytes: {:?}", magic),
            ));
        }

        // Extract bitmap data
        let bitmap_data = &data[8..4 + content_len];

        // Verify CRC
        let stored_crc = u32::from_be_bytes([
            data[4 + content_len],
            data[4 + content_len + 1],
            data[4 + content_len + 2],
            data[4 + content_len + 3],
        ]);
        let computed_crc = crc32fast::hash(&data[4..4 + content_len]);

        if stored_crc != computed_crc {
            return Err(IcebergError::invalid_deletion_vector(
                format!("CRC mismatch: stored={}, computed={}", stored_crc, computed_crc),
            ));
        }

        // Deserialize roaring treemap
        let treemap = RoaringTreemap::deserialize_from(bitmap_data)
            .map_err(|e| IcebergError::invalid_deletion_vector(format!("Failed to deserialize bitmap: {}", e)))?;

        Ok(Self::from_treemap(treemap, referenced_data_file))
    }
}

/// Puffin file footer metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PuffinFileMetadata {
    pub blobs: Vec<BlobMetadata>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// Blob metadata in Puffin file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobMetadata {
    #[serde(rename = "type")]
    pub blob_type: String,
    pub fields: Vec<i32>,
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "sequence-number")]
    pub sequence_number: i64,
    pub offset: i64,
    pub length: i64,
    #[serde(rename = "compression-codec")]
    pub compression_codec: Option<String>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// Reader for deletion vector files (Puffin format)
pub struct DeletionVectorReader;

impl DeletionVectorReader {
    /// Read deletion vector from a Puffin file
    ///
    /// # Arguments
    /// * `puffin_path` - Path to the Puffin file containing deletion vectors
    ///
    /// # Returns
    /// HashMap mapping data file paths to their deletion vectors
    pub fn read_from_file(puffin_path: &str) -> Result<HashMap<String, DeletionVector>> {
        let path = Path::new(puffin_path);
        let mut file = std::fs::File::open(path)?;
        Self::read_from_reader(&mut file)
    }

    /// Read deletion vector from bytes
    pub fn read_from_bytes(data: &[u8]) -> Result<HashMap<String, DeletionVector>> {
        let mut cursor = std::io::Cursor::new(data);
        Self::read_from_reader(&mut cursor)
    }

    /// Read deletion vectors from a reader
    pub fn read_from_reader<R: Read + Seek>(reader: &mut R) -> Result<HashMap<String, DeletionVector>> {
        // Verify Puffin magic at the beginning
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if magic != PUFFIN_MAGIC {
            return Err(IcebergError::invalid_format("Not a valid Puffin file"));
        }

        // Seek to end to read footer
        reader.seek(SeekFrom::End(-8))?;

        // Read footer payload size and flags
        let mut footer_end = [0u8; 8];
        reader.read_exact(&mut footer_end)?;

        // Verify magic at end
        if &footer_end[4..8] != PUFFIN_MAGIC {
            return Err(IcebergError::invalid_format("Invalid Puffin footer magic"));
        }

        let footer_size = i32::from_le_bytes([
            footer_end[0],
            footer_end[1],
            footer_end[2],
            footer_end[3],
        ]) as usize;

        // Read flags
        reader.seek(SeekFrom::End(-12))?;
        let mut flags = [0u8; 4];
        reader.read_exact(&mut flags)?;
        let is_compressed = (flags[0] & 0x01) != 0;

        // Read footer payload
        reader.seek(SeekFrom::End(-12 - footer_size as i64))?;
        let mut footer_payload = vec![0u8; footer_size];
        reader.read_exact(&mut footer_payload)?;

        // Decompress if needed
        let footer_json = if is_compressed {
            lz4_flex::decompress_size_prepended(&footer_payload)
                .map_err(|e| IcebergError::invalid_format(format!("Failed to decompress footer: {}", e)))?
        } else {
            footer_payload
        };

        // Parse footer JSON
        let metadata: PuffinFileMetadata = serde_json::from_slice(&footer_json)?;

        // Read deletion vector blobs
        let mut result = HashMap::new();

        for blob in metadata.blobs {
            if blob.blob_type == "deletion-vector-v1" {
                // Get referenced data file
                let referenced_file = blob.properties.get("referenced-data-file")
                    .ok_or_else(|| IcebergError::invalid_deletion_vector(
                        "Missing referenced-data-file property"
                    ))?
                    .clone();

                // Read blob data
                reader.seek(SeekFrom::Start(blob.offset as u64))?;
                let mut blob_data = vec![0u8; blob.length as usize];
                reader.read_exact(&mut blob_data)?;

                // Parse deletion vector
                let dv = DeletionVector::deserialize(&blob_data, &referenced_file)?;
                result.insert(referenced_file, dv);
            }
        }

        Ok(result)
    }

    /// Read deletion vector for a specific data file
    pub fn read_for_data_file(
        puffin_path: &str,
        data_file_path: &str,
    ) -> Result<Option<DeletionVector>> {
        let all_dvs = Self::read_from_file(puffin_path)?;
        Ok(all_dvs.into_iter()
            .find(|(k, _)| k == data_file_path || k.ends_with(data_file_path))
            .map(|(_, v)| v))
    }
}

/// Filter a record batch using a deletion vector
pub fn filter_batch_with_deletion_vector(
    batch: &arrow::record_batch::RecordBatch,
    dv: &DeletionVector,
    start_position: u64,
) -> Result<arrow::record_batch::RecordBatch> {
    use arrow::array::BooleanArray;
    use arrow::compute::filter_record_batch;

    let num_rows = batch.num_rows();
    let mut filter_array = Vec::with_capacity(num_rows);

    for i in 0..num_rows {
        let position = start_position + i as u64;
        filter_array.push(!dv.is_deleted(position));
    }

    let filter = BooleanArray::from(filter_array);
    let filtered_batch = filter_record_batch(batch, &filter)?;

    Ok(filtered_batch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deletion_vector_operations() {
        let mut dv = DeletionVector::new("test/data.parquet");
        dv.add_deleted(1);
        dv.add_deleted(5);
        dv.add_deleted(10);
        dv.add_deleted(100);

        assert!(dv.is_deleted(1));
        assert!(dv.is_deleted(5));
        assert!(dv.is_deleted(10));
        assert!(dv.is_deleted(100));
        assert!(!dv.is_deleted(2));
        assert!(!dv.is_deleted(50));
        assert_eq!(dv.len(), 4);
    }

    #[test]
    fn test_deletion_vector_serialize_deserialize() {
        let mut dv = DeletionVector::new("test/data.parquet");
        dv.add_deleted(1);
        dv.add_deleted(100);
        dv.add_deleted(10000);

        let serialized = dv.serialize().unwrap();
        let deserialized = DeletionVector::deserialize(&serialized, "test/data.parquet").unwrap();

        assert!(deserialized.is_deleted(1));
        assert!(deserialized.is_deleted(100));
        assert!(deserialized.is_deleted(10000));
        assert!(!deserialized.is_deleted(2));
        assert_eq!(deserialized.len(), 3);
    }
}
