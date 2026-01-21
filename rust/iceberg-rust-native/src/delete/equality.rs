//! Equality delete file indexing
//!
//! Equality deletes specify rows to delete by matching equality field values.
//! They use range overlap checks to efficiently filter delete files.

use std::sync::RwLock;

use rustc_hash::FxHashMap;

use crate::manifest::{ContentFile, DeleteFile};
use super::position::find_start_index;

/// Wrapper for equality delete files with cached metadata
#[derive(Debug, Clone)]
pub struct EqualityDeleteFile {
    /// The underlying delete file
    file: DeleteFile,
    /// Sequence number this delete applies to (data_sequence_number - 1)
    apply_sequence_number: i64,
    /// Equality field IDs
    equality_field_ids: Vec<i32>,
    /// Converted lower bounds (field_id -> value)
    lower_bounds: FxHashMap<i32, Vec<u8>>,
    /// Converted upper bounds (field_id -> value)
    upper_bounds: FxHashMap<i32, Vec<u8>>,
}

impl EqualityDeleteFile {
    /// Create a new equality delete file wrapper
    pub fn new(file: DeleteFile) -> Self {
        // Equality deletes apply to data files with sequence number < delete's sequence number
        let apply_sequence_number = file.data_sequence_number().unwrap_or(0).saturating_sub(1);

        let equality_field_ids = file
            .equality_field_ids()
            .map(|ids| ids.to_vec())
            .unwrap_or_default();

        let lower_bounds = file
            .lower_bounds()
            .cloned()
            .unwrap_or_default();

        let upper_bounds = file
            .upper_bounds()
            .cloned()
            .unwrap_or_default();

        EqualityDeleteFile {
            file,
            apply_sequence_number,
            equality_field_ids,
            lower_bounds,
            upper_bounds,
        }
    }

    /// Get the wrapped delete file
    pub fn file(&self) -> &DeleteFile {
        &self.file
    }

    /// Get the sequence number this delete applies to
    pub fn apply_sequence_number(&self) -> i64 {
        self.apply_sequence_number
    }

    /// Get the equality field IDs
    pub fn equality_field_ids(&self) -> &[i32] {
        &self.equality_field_ids
    }

    /// Check if this delete has both lower and upper bounds
    pub fn has_bounds(&self) -> bool {
        !self.lower_bounds.is_empty() && !self.upper_bounds.is_empty()
    }

    /// Get lower bound for a field
    pub fn lower_bound(&self, field_id: i32) -> Option<&[u8]> {
        self.lower_bounds.get(&field_id).map(|v| v.as_slice())
    }

    /// Get upper bound for a field
    pub fn upper_bound(&self, field_id: i32) -> Option<&[u8]> {
        self.upper_bounds.get(&field_id).map(|v| v.as_slice())
    }

    /// Get null value counts
    pub fn null_value_counts(&self) -> Option<FxHashMap<i32, i64>> {
        self.file.null_value_counts().cloned()
    }

    /// Get value counts
    pub fn value_counts(&self) -> Option<FxHashMap<i32, i64>> {
        self.file.value_counts().cloned()
    }
}

/// A group of equality delete files sorted by sequence number
///
/// Equality deletes apply to data files written before them.
/// Additional filtering is done by checking if data file ranges overlap
/// with delete file ranges.
#[derive(Debug)]
pub struct EqualityDeletes {
    /// Buffer for files before indexing
    buffer: RwLock<Option<Vec<EqualityDeleteFile>>>,
    /// Indexed sequence numbers (sorted)
    seqs: RwLock<Vec<i64>>,
    /// Indexed delete files (sorted by apply sequence number)
    files: RwLock<Vec<EqualityDeleteFile>>,
}

impl Default for EqualityDeletes {
    fn default() -> Self {
        Self::new()
    }
}

impl EqualityDeletes {
    /// Create a new empty EqualityDeletes
    pub fn new() -> Self {
        EqualityDeletes {
            buffer: RwLock::new(Some(Vec::new())),
            seqs: RwLock::new(Vec::new()),
            files: RwLock::new(Vec::new()),
        }
    }

    /// Add a delete file to the index
    pub fn add(&self, file: DeleteFile) {
        let mut buffer = self.buffer.write().unwrap();
        if let Some(ref mut buf) = *buffer {
            buf.push(EqualityDeleteFile::new(file));
        } else {
            panic!("Cannot add files after indexing");
        }
    }

    /// Filter delete files that apply to a data file
    ///
    /// Returns delete files that:
    /// 1. Have apply_sequence_number >= data file sequence number
    /// 2. Have overlapping ranges with the data file (if bounds are available)
    pub fn filter(&self, seq: i64, data_file: &DataFileStats) -> Vec<DeleteFile> {
        self.index_if_needed();

        let seqs = self.seqs.read().unwrap();
        let files = self.files.read().unwrap();

        if files.is_empty() {
            return Vec::new();
        }

        let start = find_start_index(&seqs, seq);

        if start >= files.len() {
            return Vec::new();
        }

        let mut matching = Vec::new();

        for file in &files[start..] {
            if can_contain_eq_deletes_for_file(data_file, file) {
                matching.push(file.file().clone());
            }
        }

        matching
    }

    /// Filter without data file stats (returns all applicable deletes)
    pub fn filter_all(&self, seq: i64) -> Vec<DeleteFile> {
        self.index_if_needed();

        let seqs = self.seqs.read().unwrap();
        let files = self.files.read().unwrap();

        if files.is_empty() {
            return Vec::new();
        }

        let start = find_start_index(&seqs, seq);

        if start >= files.len() {
            return Vec::new();
        }

        files[start..].iter().map(|f| f.file().clone()).collect()
    }

    /// Get all referenced delete files
    pub fn referenced_delete_files(&self) -> Vec<DeleteFile> {
        self.index_if_needed();
        self.files
            .read()
            .unwrap()
            .iter()
            .map(|f| f.file().clone())
            .collect()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.index_if_needed();
        self.files.read().unwrap().is_empty()
    }

    /// Get the number of delete files
    pub fn len(&self) -> usize {
        self.index_if_needed();
        self.files.read().unwrap().len()
    }

    /// Perform lazy indexing if needed
    fn index_if_needed(&self) {
        // Check if already indexed
        {
            let buffer = self.buffer.read().unwrap();
            if buffer.is_none() {
                return;
            }
        }

        // Need to index
        let mut buffer = self.buffer.write().unwrap();
        if let Some(buf) = buffer.take() {
            let mut indexed_files = buf;
            // Sort by apply sequence number
            indexed_files.sort_by_key(|f| f.apply_sequence_number());

            // Extract sequence numbers
            let indexed_seqs: Vec<i64> = indexed_files
                .iter()
                .map(|f| f.apply_sequence_number())
                .collect();

            // Store indexed data
            *self.seqs.write().unwrap() = indexed_seqs;
            *self.files.write().unwrap() = indexed_files;
        }
    }
}

/// Data file statistics for filtering
#[derive(Debug, Clone)]
pub struct DataFileStats {
    /// Lower bounds by field ID
    pub lower_bounds: Option<FxHashMap<i32, Vec<u8>>>,
    /// Upper bounds by field ID
    pub upper_bounds: Option<FxHashMap<i32, Vec<u8>>>,
    /// Null value counts by field ID
    pub null_value_counts: Option<FxHashMap<i32, i64>>,
    /// Value counts by field ID
    pub value_counts: Option<FxHashMap<i32, i64>>,
}

impl DataFileStats {
    /// Create new data file stats
    pub fn new() -> Self {
        DataFileStats {
            lower_bounds: None,
            upper_bounds: None,
            null_value_counts: None,
            value_counts: None,
        }
    }

    /// Set lower bounds
    pub fn with_lower_bounds(mut self, bounds: FxHashMap<i32, Vec<u8>>) -> Self {
        self.lower_bounds = Some(bounds);
        self
    }

    /// Set upper bounds
    pub fn with_upper_bounds(mut self, bounds: FxHashMap<i32, Vec<u8>>) -> Self {
        self.upper_bounds = Some(bounds);
        self
    }

    /// Set null value counts
    pub fn with_null_value_counts(mut self, counts: FxHashMap<i32, i64>) -> Self {
        self.null_value_counts = Some(counts);
        self
    }

    /// Set value counts
    pub fn with_value_counts(mut self, counts: FxHashMap<i32, i64>) -> Self {
        self.value_counts = Some(counts);
        self
    }
}

impl Default for DataFileStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if a delete file can contain deletes for a data file
///
/// Uses statistics to determine if the ranges overlap.
fn can_contain_eq_deletes_for_file(data_file: &DataFileStats, delete_file: &EqualityDeleteFile) -> bool {
    // If delete file has no bounds, assume it may match
    if !delete_file.has_bounds() {
        return true;
    }

    // If data file has no bounds, assume it may match
    let data_lowers = match &data_file.lower_bounds {
        Some(l) => l,
        None => return true,
    };
    let data_uppers = match &data_file.upper_bounds {
        Some(u) => u,
        None => return true,
    };

    let delete_null_counts = delete_file.null_value_counts();
    let delete_value_counts = delete_file.value_counts();

    // Check each equality field
    for &field_id in delete_file.equality_field_ids() {
        // Check null value handling
        let data_has_nulls = contains_null(&data_file.null_value_counts, field_id);
        let delete_has_nulls = contains_null(&delete_null_counts, field_id);

        if data_has_nulls && delete_has_nulls {
            // Both have nulls, must apply delete
            continue;
        }

        // Check if data is all null but delete has no nulls
        if all_null(&data_file.null_value_counts, &data_file.value_counts, field_id)
            && all_non_null(&delete_null_counts, field_id)
        {
            return false;
        }

        // Check if delete is all null but data has no nulls
        if all_null(&delete_null_counts, &delete_value_counts, field_id)
            && all_non_null(&data_file.null_value_counts, field_id)
        {
            return false;
        }

        // Check range overlap
        let data_lower = data_lowers.get(&field_id);
        let data_upper = data_uppers.get(&field_id);
        let delete_lower = delete_file.lower_bound(field_id);
        let delete_upper = delete_file.upper_bound(field_id);

        if let (Some(dl), Some(du), Some(del_l), Some(del_u)) =
            (data_lower, data_upper, delete_lower, delete_upper)
        {
            if !ranges_overlap(dl, du, del_l, del_u) {
                return false;
            }
        }
    }

    true
}

/// Check if null value count indicates null values are present
fn contains_null(null_counts: &Option<FxHashMap<i32, i64>>, field_id: i32) -> bool {
    match null_counts {
        Some(counts) => counts.get(&field_id).map(|&c| c > 0).unwrap_or(true),
        None => true, // Unknown, assume may have nulls
    }
}

/// Check if all values are null
fn all_null(
    null_counts: &Option<FxHashMap<i32, i64>>,
    value_counts: &Option<FxHashMap<i32, i64>>,
    field_id: i32,
) -> bool {
    match (null_counts, value_counts) {
        (Some(nc), Some(vc)) => {
            let null_count = nc.get(&field_id);
            let value_count = vc.get(&field_id);
            match (null_count, value_count) {
                (Some(&n), Some(&v)) => n == v && n > 0,
                _ => false,
            }
        }
        _ => false,
    }
}

/// Check if all values are non-null
fn all_non_null(null_counts: &Option<FxHashMap<i32, i64>>, field_id: i32) -> bool {
    match null_counts {
        Some(counts) => counts.get(&field_id).map(|&c| c == 0).unwrap_or(false),
        None => false,
    }
}

/// Check if two byte ranges overlap
///
/// Assumes lexicographic ordering (suitable for most Iceberg types).
fn ranges_overlap(data_lower: &[u8], data_upper: &[u8], delete_lower: &[u8], delete_upper: &[u8]) -> bool {
    // No overlap if data is entirely after delete
    if data_lower > delete_upper {
        return false;
    }

    // No overlap if delete is entirely after data
    if delete_lower > data_upper {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::ContentFileData;
    use crate::types::{FileContent, FileFormat};

    fn make_eq_delete_file(path: &str, seq: i64, eq_fields: Vec<i32>) -> DeleteFile {
        let mut data = ContentFileData::default();
        data.content = FileContent::EqualityDeletes;
        data.file_path = path.to_string();
        data.file_format = FileFormat::Parquet;
        data.data_sequence_number = Some(seq);
        data.record_count = 100;
        data.equality_field_ids = if eq_fields.is_empty() {
            None
        } else {
            Some(eq_fields.into())
        };
        DeleteFile::from(data)
    }

    #[test]
    fn test_equality_deletes_empty() {
        let deletes = EqualityDeletes::new();
        assert!(deletes.is_empty());
        assert_eq!(deletes.len(), 0);
        assert!(deletes.filter_all(100).is_empty());
    }

    #[test]
    fn test_equality_deletes_add_and_filter() {
        let deletes = EqualityDeletes::new();

        // Equality deletes apply to files with seq < delete_seq
        // So a delete at seq 10 applies to data at seq <= 9
        deletes.add(make_eq_delete_file("delete1.parquet", 10, vec![1]));
        deletes.add(make_eq_delete_file("delete2.parquet", 20, vec![1]));
        deletes.add(make_eq_delete_file("delete3.parquet", 30, vec![1]));

        assert_eq!(deletes.len(), 3);

        // Data file at seq 5 should see all deletes (apply seqs: 9, 19, 29)
        let result = deletes.filter_all(5);
        assert_eq!(result.len(), 3);

        // Data file at seq 15 should see deletes at 20 and 30 (apply seqs: 19, 29)
        let result = deletes.filter_all(15);
        assert_eq!(result.len(), 2);

        // Data file at seq 25 should see delete at 30 (apply seq: 29)
        let result = deletes.filter_all(25);
        assert_eq!(result.len(), 1);

        // Data file at seq 35 should see no deletes
        let result = deletes.filter_all(35);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_equality_delete_file_wrapper() {
        let file = make_eq_delete_file("delete.parquet", 10, vec![1, 2, 3]);
        let wrapper = EqualityDeleteFile::new(file);

        assert_eq!(wrapper.apply_sequence_number(), 9);
        assert_eq!(wrapper.equality_field_ids(), &[1, 2, 3]);
    }

    #[test]
    fn test_ranges_overlap() {
        // Overlapping ranges
        assert!(ranges_overlap(&[1], &[5], &[3], &[7]));
        assert!(ranges_overlap(&[3], &[7], &[1], &[5]));

        // Touching ranges
        assert!(ranges_overlap(&[1], &[5], &[5], &[10]));

        // Non-overlapping ranges
        assert!(!ranges_overlap(&[1], &[5], &[6], &[10]));
        assert!(!ranges_overlap(&[6], &[10], &[1], &[5]));

        // Same range
        assert!(ranges_overlap(&[1], &[5], &[1], &[5]));

        // One contains the other
        assert!(ranges_overlap(&[1], &[10], &[3], &[7]));
        assert!(ranges_overlap(&[3], &[7], &[1], &[10]));
    }
}
