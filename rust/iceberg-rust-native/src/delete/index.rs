//! DeleteFileIndex - efficient delete file lookup
//!
//! The index organizes delete files for efficient lookup by:
//! - Partition (for both position and equality deletes)
//! - File path (for position deletes referencing specific files)
//! - DV path (for deletion vectors)
//!
//! Sequence number filtering is used to find deletes that apply to a given data file.

use rustc_hash::FxHashMap;

use super::equality::{DataFileStats, EqualityDeletes};
use super::position::PositionDeletes;
use crate::manifest::{ContentFile, DataFile, DeleteFile};
use crate::types::{FileContent, PartitionData};

/// Partition key for indexing by spec ID and partition data
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct PartitionKey {
    /// Partition spec ID
    pub spec_id: i32,
    /// Partition hash (for fast lookup)
    partition_hash: u64,
}

impl PartitionKey {
    /// Create a new partition key
    pub fn new(spec_id: i32, partition: &PartitionData) -> Self {
        use std::hash::{Hash, Hasher};
        let mut hasher = rustc_hash::FxHasher::default();
        partition.hash(&mut hasher);
        PartitionKey {
            spec_id,
            partition_hash: hasher.finish(),
        }
    }
}

/// Index of delete files for efficient lookup
///
/// The index supports:
/// - Global equality deletes (for unpartitioned tables)
/// - Partition-scoped equality deletes
/// - Partition-scoped position deletes
/// - Path-scoped position deletes
/// - Deletion vectors (DVs)
///
/// # Example
///
/// ```rust,ignore
/// let index = DeleteFileIndex::builder()
///     .add_delete_files(delete_files)
///     .min_sequence_number(100)
///     .build();
///
/// // Find deletes for a data file
/// let deletes = index.for_data_file(data_file.data_sequence_number(), &data_file);
/// ```
#[derive(Debug)]
pub struct DeleteFileIndex {
    /// Global equality deletes (for unpartitioned tables)
    global_eq_deletes: Option<EqualityDeletes>,
    /// Equality deletes by partition
    eq_deletes_by_partition: FxHashMap<PartitionKey, EqualityDeletes>,
    /// Position deletes by partition
    pos_deletes_by_partition: FxHashMap<PartitionKey, PositionDeletes>,
    /// Position deletes by referenced file path
    pos_deletes_by_path: FxHashMap<String, PositionDeletes>,
    /// Deletion vectors by referenced data file path
    dv_by_path: FxHashMap<String, DeleteFile>,
    /// Whether the index has any equality deletes
    has_eq_deletes: bool,
    /// Whether the index has any position deletes
    has_pos_deletes: bool,
    /// Total number of delete files indexed
    file_count: usize,
}

impl DeleteFileIndex {
    /// Create a new builder
    pub fn builder() -> DeleteFileIndexBuilder {
        DeleteFileIndexBuilder::new()
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.file_count == 0
    }

    /// Check if the index has equality deletes
    pub fn has_equality_deletes(&self) -> bool {
        self.has_eq_deletes
    }

    /// Check if the index has position deletes
    pub fn has_position_deletes(&self) -> bool {
        self.has_pos_deletes
    }

    /// Get the total number of delete files indexed
    pub fn file_count(&self) -> usize {
        self.file_count
    }

    /// Find delete files that apply to a data file
    ///
    /// Returns an empty vec if no deletes apply.
    pub fn for_data_file(&self, seq: i64, file: &DataFile) -> Vec<DeleteFile> {
        if self.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::new();

        // Find global equality deletes
        if let Some(ref global) = self.global_eq_deletes {
            let stats = self.extract_data_file_stats(file);
            result.extend(global.filter(seq, &stats));
        }

        // Find partition equality deletes
        let partition_key = PartitionKey::new(file.spec_id(), file.partition());
        if let Some(eq_deletes) = self.eq_deletes_by_partition.get(&partition_key) {
            let stats = self.extract_data_file_stats(file);
            result.extend(eq_deletes.filter(seq, &stats));
        }

        // Check for DV first
        if let Some(dv) = self.dv_by_path.get(file.file_path()) {
            // DV takes precedence over position deletes
            if dv.data_sequence_number().unwrap_or(0) >= seq {
                result.push(dv.clone());
                return result;
            }
        }

        // Find partition position deletes
        if let Some(pos_deletes) = self.pos_deletes_by_partition.get(&partition_key) {
            result.extend(pos_deletes.filter(seq));
        }

        // Find path position deletes
        if let Some(pos_deletes) = self.pos_deletes_by_path.get(file.file_path()) {
            result.extend(pos_deletes.filter(seq));
        }

        result
    }

    /// Find delete files for a manifest entry
    pub fn for_entry(&self, entry_seq: i64, file: &DataFile) -> Vec<DeleteFile> {
        self.for_data_file(entry_seq, file)
    }

    /// Get all referenced delete files
    pub fn all_delete_files(&self) -> Vec<DeleteFile> {
        let mut result = Vec::new();

        if let Some(ref global) = self.global_eq_deletes {
            result.extend(global.referenced_delete_files());
        }

        for deletes in self.eq_deletes_by_partition.values() {
            result.extend(deletes.referenced_delete_files());
        }

        for deletes in self.pos_deletes_by_partition.values() {
            result.extend(deletes.referenced_delete_files());
        }

        for deletes in self.pos_deletes_by_path.values() {
            result.extend(deletes.referenced_delete_files());
        }

        for dv in self.dv_by_path.values() {
            result.push(dv.clone());
        }

        result
    }

    /// Extract statistics from a data file for filtering
    fn extract_data_file_stats(&self, file: &DataFile) -> DataFileStats {
        DataFileStats {
            lower_bounds: file.lower_bounds().cloned(),
            upper_bounds: file.upper_bounds().cloned(),
            null_value_counts: file.null_value_counts().cloned(),
            value_counts: file.value_counts().cloned(),
        }
    }
}

/// Builder for DeleteFileIndex
pub struct DeleteFileIndexBuilder {
    delete_files: Vec<DeleteFile>,
    min_sequence_number: i64,
    /// Partition spec ID to use for unpartitioned detection
    /// If None, treats tables with empty partitions as unpartitioned
    unpartitioned_spec_id: Option<i32>,
}

impl Default for DeleteFileIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DeleteFileIndexBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        DeleteFileIndexBuilder {
            delete_files: Vec::new(),
            min_sequence_number: 0,
            unpartitioned_spec_id: None,
        }
    }

    /// Add delete files to the index
    pub fn add_delete_files(mut self, files: impl IntoIterator<Item = DeleteFile>) -> Self {
        self.delete_files.extend(files);
        self
    }

    /// Add a single delete file
    pub fn add_delete_file(mut self, file: DeleteFile) -> Self {
        self.delete_files.push(file);
        self
    }

    /// Set minimum sequence number filter
    ///
    /// Only delete files with sequence number > min_seq will be indexed.
    pub fn min_sequence_number(mut self, min_seq: i64) -> Self {
        self.min_sequence_number = min_seq;
        self
    }

    /// Set the spec ID that indicates unpartitioned tables
    pub fn unpartitioned_spec_id(mut self, spec_id: i32) -> Self {
        self.unpartitioned_spec_id = Some(spec_id);
        self
    }

    /// Build the index
    pub fn build(self) -> DeleteFileIndex {
        let mut global_eq_deletes: Option<EqualityDeletes> = None;
        let mut eq_deletes_by_partition: FxHashMap<PartitionKey, EqualityDeletes> =
            FxHashMap::default();
        let mut pos_deletes_by_partition: FxHashMap<PartitionKey, PositionDeletes> =
            FxHashMap::default();
        let mut pos_deletes_by_path: FxHashMap<String, PositionDeletes> = FxHashMap::default();
        let mut dv_by_path: FxHashMap<String, DeleteFile> = FxHashMap::default();

        let mut file_count = 0;
        let mut has_eq_deletes = false;
        let mut has_pos_deletes = false;

        for file in self.delete_files {
            // Filter by sequence number
            if file.data_sequence_number().unwrap_or(0) <= self.min_sequence_number {
                continue;
            }

            file_count += 1;

            match file.content() {
                FileContent::PositionDeletes => {
                    has_pos_deletes = true;

                    // Check if it's a DV (deletion vector)
                    if let Some(ref_path) = file.referenced_data_file() {
                        // It's a DV - index by referenced path
                        dv_by_path.insert(ref_path.to_string(), file);
                    } else {
                        // Regular position delete
                        // Check if it references a specific path
                        let partition_key = PartitionKey::new(file.spec_id(), file.partition());
                        pos_deletes_by_partition
                            .entry(partition_key)
                            .or_insert_with(PositionDeletes::new)
                            .add(file);
                    }
                }
                FileContent::EqualityDeletes => {
                    has_eq_deletes = true;

                    // Check if partition is empty (unpartitioned table)
                    let is_unpartitioned = file.partition().is_empty()
                        || self
                            .unpartitioned_spec_id
                            .map(|id| file.spec_id() == id)
                            .unwrap_or(false);

                    if is_unpartitioned {
                        // Global equality delete
                        global_eq_deletes
                            .get_or_insert_with(EqualityDeletes::new)
                            .add(file);
                    } else {
                        // Partition-scoped equality delete
                        let partition_key = PartitionKey::new(file.spec_id(), file.partition());
                        eq_deletes_by_partition
                            .entry(partition_key)
                            .or_insert_with(EqualityDeletes::new)
                            .add(file);
                    }
                }
                FileContent::Data => {
                    // Ignore data files
                }
            }
        }

        DeleteFileIndex {
            global_eq_deletes,
            eq_deletes_by_partition,
            pos_deletes_by_partition,
            pos_deletes_by_path,
            dv_by_path,
            has_eq_deletes,
            has_pos_deletes,
            file_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::ContentFileData;
    use crate::types::FileFormat;

    fn make_data_file(path: &str, spec_id: i32, partition: PartitionData, seq: i64) -> DataFile {
        let mut data = ContentFileData::default();
        data.content = FileContent::Data;
        data.file_path = path.to_string();
        data.file_format = FileFormat::Parquet;
        data.spec_id = spec_id;
        data.partition = partition;
        data.data_sequence_number = Some(seq);
        data.record_count = 1000;
        DataFile::from(data)
    }

    fn make_pos_delete_file(path: &str, spec_id: i32, partition: PartitionData, seq: i64) -> DeleteFile {
        let mut data = ContentFileData::default();
        data.content = FileContent::PositionDeletes;
        data.file_path = path.to_string();
        data.file_format = FileFormat::Parquet;
        data.spec_id = spec_id;
        data.partition = partition;
        data.data_sequence_number = Some(seq);
        data.record_count = 100;
        DeleteFile::from(data)
    }

    fn make_eq_delete_file(
        path: &str,
        spec_id: i32,
        partition: PartitionData,
        seq: i64,
        eq_fields: Vec<i32>,
    ) -> DeleteFile {
        let mut data = ContentFileData::default();
        data.content = FileContent::EqualityDeletes;
        data.file_path = path.to_string();
        data.file_format = FileFormat::Parquet;
        data.spec_id = spec_id;
        data.partition = partition;
        data.data_sequence_number = Some(seq);
        data.record_count = 100;
        data.equality_field_ids = if eq_fields.is_empty() {
            None
        } else {
            Some(eq_fields.into())
        };
        DeleteFile::from(data)
    }

    fn make_dv(path: &str, ref_path: &str, seq: i64) -> DeleteFile {
        let mut data = ContentFileData::default();
        data.content = FileContent::PositionDeletes;
        data.file_path = path.to_string();
        data.file_format = FileFormat::Parquet;
        data.spec_id = 0;
        data.partition = PartitionData::empty();
        data.data_sequence_number = Some(seq);
        data.record_count = 10;
        data.referenced_data_file = Some(ref_path.to_string());
        DeleteFile::from(data)
    }

    #[test]
    fn test_empty_index() {
        let index = DeleteFileIndex::builder().build();
        assert!(index.is_empty());
        assert!(!index.has_equality_deletes());
        assert!(!index.has_position_deletes());
        assert_eq!(index.file_count(), 0);
    }

    #[test]
    fn test_position_deletes_by_partition() {
        let partition = PartitionData::empty();

        let index = DeleteFileIndex::builder()
            .add_delete_file(make_pos_delete_file("del1.parquet", 0, partition.clone(), 10))
            .add_delete_file(make_pos_delete_file("del2.parquet", 0, partition.clone(), 20))
            .build();

        assert!(!index.is_empty());
        assert!(index.has_position_deletes());
        assert!(!index.has_equality_deletes());
        assert_eq!(index.file_count(), 2);

        let data_file = make_data_file("data.parquet", 0, partition.clone(), 5);

        // Data file at seq 5 should see both deletes (at 10 and 20)
        let deletes = index.for_data_file(5, &data_file);
        assert_eq!(deletes.len(), 2);

        // Data file at seq 15 should see only delete at 20
        let deletes = index.for_data_file(15, &data_file);
        assert_eq!(deletes.len(), 1);

        // Data file at seq 25 should see no deletes
        let deletes = index.for_data_file(25, &data_file);
        assert_eq!(deletes.len(), 0);
    }

    #[test]
    fn test_equality_deletes_global() {
        let partition = PartitionData::empty();

        let index = DeleteFileIndex::builder()
            .add_delete_file(make_eq_delete_file(
                "eq_del.parquet",
                0,
                partition.clone(),
                10,
                vec![1],
            ))
            .build();

        assert!(!index.is_empty());
        assert!(index.has_equality_deletes());
        assert!(!index.has_position_deletes());

        let data_file = make_data_file("data.parquet", 0, partition.clone(), 5);

        // Data file at seq 5 should see the equality delete (apply seq 9)
        let deletes = index.for_data_file(5, &data_file);
        assert_eq!(deletes.len(), 1);

        // Data file at seq 10 should not see the delete (apply seq 9 < 10)
        let deletes = index.for_data_file(10, &data_file);
        assert_eq!(deletes.len(), 0);
    }

    #[test]
    fn test_dv_takes_precedence() {
        let partition = PartitionData::empty();

        let index = DeleteFileIndex::builder()
            .add_delete_file(make_pos_delete_file("pos_del.parquet", 0, partition.clone(), 10))
            .add_delete_file(make_dv("dv.bin", "data.parquet", 15))
            .build();

        let data_file = make_data_file("data.parquet", 0, partition.clone(), 5);

        // Should see DV but not position delete (DV takes precedence)
        let deletes = index.for_data_file(5, &data_file);
        assert_eq!(deletes.len(), 1);
        assert!(deletes[0].referenced_data_file().is_some());
    }

    #[test]
    fn test_min_sequence_number_filter() {
        let partition = PartitionData::empty();

        let index = DeleteFileIndex::builder()
            .add_delete_file(make_pos_delete_file("del1.parquet", 0, partition.clone(), 10))
            .add_delete_file(make_pos_delete_file("del2.parquet", 0, partition.clone(), 20))
            .min_sequence_number(15)
            .build();

        // Only del2 should be indexed (seq 20 > min 15)
        assert_eq!(index.file_count(), 1);

        let data_file = make_data_file("data.parquet", 0, partition.clone(), 5);
        let deletes = index.for_data_file(5, &data_file);
        assert_eq!(deletes.len(), 1);
    }

    #[test]
    fn test_partition_isolation() {
        let partition1 = PartitionData::new(vec![crate::types::PartitionValue::Int(1)]);
        let partition2 = PartitionData::new(vec![crate::types::PartitionValue::Int(2)]);

        let index = DeleteFileIndex::builder()
            .add_delete_file(make_pos_delete_file("del1.parquet", 0, partition1.clone(), 10))
            .add_delete_file(make_pos_delete_file("del2.parquet", 0, partition2.clone(), 10))
            .build();

        // Data file in partition1 should only see del1
        let data_file1 = make_data_file("data1.parquet", 0, partition1.clone(), 5);
        let deletes = index.for_data_file(5, &data_file1);
        assert_eq!(deletes.len(), 1);
        assert_eq!(deletes[0].file_path(), "del1.parquet");

        // Data file in partition2 should only see del2
        let data_file2 = make_data_file("data2.parquet", 0, partition2.clone(), 5);
        let deletes = index.for_data_file(5, &data_file2);
        assert_eq!(deletes.len(), 1);
        assert_eq!(deletes[0].file_path(), "del2.parquet");
    }

    #[test]
    fn test_all_delete_files() {
        let partition = PartitionData::empty();

        let index = DeleteFileIndex::builder()
            .add_delete_file(make_pos_delete_file("pos1.parquet", 0, partition.clone(), 10))
            .add_delete_file(make_eq_delete_file("eq1.parquet", 0, partition.clone(), 20, vec![1]))
            .add_delete_file(make_dv("dv.bin", "data.parquet", 30))
            .build();

        let all_files = index.all_delete_files();
        assert_eq!(all_files.len(), 3);
    }
}
