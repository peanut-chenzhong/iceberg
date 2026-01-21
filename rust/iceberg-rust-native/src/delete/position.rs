//! Position delete file indexing
//!
//! Position deletes specify exact rows to delete by file path and position.
//! They are sorted by sequence number for efficient lookup.

use std::sync::RwLock;

use crate::manifest::{ContentFile, DeleteFile};

/// A group of position delete files sorted by sequence number
///
/// Position deletes apply to data files written before them (by sequence number).
/// Files are indexed lazily for efficient building.
#[derive(Debug)]
pub struct PositionDeletes {
    /// Buffer for files before indexing
    buffer: RwLock<Option<Vec<DeleteFile>>>,
    /// Indexed sequence numbers (sorted)
    seqs: RwLock<Vec<i64>>,
    /// Indexed delete files (sorted by sequence number)
    files: RwLock<Vec<DeleteFile>>,
}

impl Default for PositionDeletes {
    fn default() -> Self {
        Self::new()
    }
}

impl PositionDeletes {
    /// Create a new empty PositionDeletes
    pub fn new() -> Self {
        PositionDeletes {
            buffer: RwLock::new(Some(Vec::new())),
            seqs: RwLock::new(Vec::new()),
            files: RwLock::new(Vec::new()),
        }
    }

    /// Add a delete file to the index
    pub fn add(&self, file: DeleteFile) {
        let mut buffer = self.buffer.write().unwrap();
        if let Some(ref mut buf) = *buffer {
            buf.push(file);
        } else {
            panic!("Cannot add files after indexing");
        }
    }

    /// Filter delete files that apply to a data file with the given sequence number
    ///
    /// Returns delete files with sequence numbers > data file sequence number
    pub fn filter(&self, seq: i64) -> Vec<DeleteFile> {
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

        if start == 0 {
            return files.clone();
        }

        files[start..].to_vec()
    }

    /// Get all referenced delete files
    pub fn referenced_delete_files(&self) -> Vec<DeleteFile> {
        self.index_if_needed();
        self.files.read().unwrap().clone()
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
            // Sort by sequence number
            indexed_files.sort_by_key(|f| f.data_sequence_number().unwrap_or(0));

            // Extract sequence numbers
            let indexed_seqs: Vec<i64> = indexed_files
                .iter()
                .map(|f| f.data_sequence_number().unwrap_or(0))
                .collect();

            // Store indexed data
            *self.seqs.write().unwrap() = indexed_seqs;
            *self.files.write().unwrap() = indexed_files;
        }
    }
}

/// Find the start index for files that apply to data with the given sequence number
///
/// Returns the index of the first delete file with sequence number > seq.
/// Uses binary search for O(log n) lookup.
pub fn find_start_index(seqs: &[i64], seq: i64) -> usize {
    match seqs.binary_search(&seq) {
        Ok(pos) => {
            // Found exact match, need to find first occurrence
            let mut start = pos;
            while start > 0 && seqs[start - 1] >= seq {
                start -= 1;
            }
            start
        }
        Err(pos) => {
            // Not found, pos is where it would be inserted
            pos
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::ContentFileData;
    use crate::types::{FileContent, FileFormat};

    fn make_delete_file(path: &str, seq: i64) -> DeleteFile {
        let mut data = ContentFileData::default();
        data.content = FileContent::PositionDeletes;
        data.file_path = path.to_string();
        data.file_format = FileFormat::Parquet;
        data.data_sequence_number = Some(seq);
        data.record_count = 100;
        DeleteFile::from(data)
    }

    #[test]
    fn test_position_deletes_empty() {
        let deletes = PositionDeletes::new();
        assert!(deletes.is_empty());
        assert_eq!(deletes.len(), 0);
        assert!(deletes.filter(100).is_empty());
    }

    #[test]
    fn test_position_deletes_add_and_filter() {
        let deletes = PositionDeletes::new();

        deletes.add(make_delete_file("delete1.parquet", 10));
        deletes.add(make_delete_file("delete2.parquet", 20));
        deletes.add(make_delete_file("delete3.parquet", 30));

        assert_eq!(deletes.len(), 3);

        // Data file at seq 5 should see all deletes (10, 20, 30)
        let result = deletes.filter(5);
        assert_eq!(result.len(), 3);

        // Data file at seq 15 should see deletes at 20 and 30
        let result = deletes.filter(15);
        assert_eq!(result.len(), 2);

        // Data file at seq 25 should see delete at 30
        let result = deletes.filter(25);
        assert_eq!(result.len(), 1);

        // Data file at seq 35 should see no deletes
        let result = deletes.filter(35);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_position_deletes_duplicate_seqs() {
        let deletes = PositionDeletes::new();

        deletes.add(make_delete_file("delete1.parquet", 10));
        deletes.add(make_delete_file("delete2.parquet", 10));
        deletes.add(make_delete_file("delete3.parquet", 20));

        // Data file at seq 5 should see all 3 deletes
        let result = deletes.filter(5);
        assert_eq!(result.len(), 3);

        // Data file at seq 10 should see all 3 deletes (seq >= 10)
        let result = deletes.filter(10);
        assert_eq!(result.len(), 3);

        // Data file at seq 15 should see 1 delete (seq 20)
        let result = deletes.filter(15);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_find_start_index() {
        let seqs = vec![5, 10, 10, 15, 20, 25];

        // Before all
        assert_eq!(find_start_index(&seqs, 1), 0);

        // At first
        assert_eq!(find_start_index(&seqs, 5), 0);

        // Between values
        assert_eq!(find_start_index(&seqs, 7), 1);

        // At duplicate
        assert_eq!(find_start_index(&seqs, 10), 1);

        // After all
        assert_eq!(find_start_index(&seqs, 30), 6);
    }
}
