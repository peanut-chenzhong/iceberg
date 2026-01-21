//! ManifestGroup - parallel manifest file reading
//!
//! This module provides efficient parallel reading of multiple manifest files,
//! similar to Java's ManifestGroup. It supports:
//! - Parallel manifest file reading using Rayon
//! - Unified filtering across all manifests
//! - Aggregated statistics
//! - Memory-efficient streaming

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rayon::prelude::*;

use super::content_file::ContentFile;
use super::entry::{DataManifestEntry, DeleteManifestEntry};
use super::filter::{InclusiveMetricsEvaluator, PartitionEvaluator, PartitionSet};
use super::reader::{ManifestContent, ManifestReadStats, ManifestReader, ManifestReaderBuilder};
use crate::error::{Error, Result};
use crate::expr::evaluator::BoundExpressionTree;

/// Information about a manifest file
#[derive(Debug, Clone)]
pub struct ManifestFile {
    /// Path to the manifest file
    pub path: PathBuf,
    /// Content type (data or deletes)
    pub content: ManifestContent,
    /// First row ID for this manifest (for row lineage)
    pub first_row_id: Option<i64>,
    /// Number of added entries in manifest
    pub added_files_count: Option<i64>,
    /// Number of existing entries in manifest
    pub existing_files_count: Option<i64>,
    /// Number of deleted entries in manifest
    pub deleted_files_count: Option<i64>,
    /// Partition spec ID
    pub partition_spec_id: i32,
    /// Sequence number
    pub sequence_number: i64,
    /// Minimum sequence number of data files
    pub min_sequence_number: i64,
}

impl ManifestFile {
    /// Create a new manifest file info
    pub fn new(path: impl Into<PathBuf>, content: ManifestContent) -> Self {
        ManifestFile {
            path: path.into(),
            content,
            first_row_id: None,
            added_files_count: None,
            existing_files_count: None,
            deleted_files_count: None,
            partition_spec_id: 0,
            sequence_number: 0,
            min_sequence_number: 0,
        }
    }

    /// Create a data manifest file info
    pub fn data(path: impl Into<PathBuf>) -> Self {
        Self::new(path, ManifestContent::Data)
    }

    /// Create a delete manifest file info
    pub fn deletes(path: impl Into<PathBuf>) -> Self {
        Self::new(path, ManifestContent::Deletes)
    }

    /// Set the first row ID
    pub fn with_first_row_id(mut self, id: i64) -> Self {
        self.first_row_id = Some(id);
        self
    }

    /// Set file counts
    pub fn with_counts(mut self, added: i64, existing: i64, deleted: i64) -> Self {
        self.added_files_count = Some(added);
        self.existing_files_count = Some(existing);
        self.deleted_files_count = Some(deleted);
        self
    }

    /// Set partition spec ID
    pub fn with_spec_id(mut self, spec_id: i32) -> Self {
        self.partition_spec_id = spec_id;
        self
    }

    /// Set sequence numbers
    pub fn with_sequence_numbers(mut self, seq: i64, min_seq: i64) -> Self {
        self.sequence_number = seq;
        self.min_sequence_number = min_seq;
        self
    }

    /// Check if this manifest might contain matching files
    /// based on sequence number filtering
    pub fn might_contain_matching_files(&self, min_sequence_number: Option<i64>) -> bool {
        match min_sequence_number {
            Some(min_seq) => self.min_sequence_number >= min_seq,
            None => true,
        }
    }
}

/// Builder for ManifestGroup
pub struct ManifestGroupBuilder {
    manifests: Vec<ManifestFile>,
    partition_filter: Option<BoundExpressionTree>,
    metrics_evaluator: Option<InclusiveMetricsEvaluator>,
    partition_set: Option<PartitionSet>,
    include_deleted: bool,
    keep_stats: bool,
    case_sensitive: bool,
    parallelism: Option<usize>,
    min_sequence_number: Option<i64>,
}

impl Default for ManifestGroupBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ManifestGroupBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        ManifestGroupBuilder {
            manifests: Vec::new(),
            partition_filter: None,
            metrics_evaluator: None,
            partition_set: None,
            include_deleted: false,
            keep_stats: true,
            case_sensitive: true,
            parallelism: None,
            min_sequence_number: None,
        }
    }

    /// Add a manifest file
    pub fn add_manifest(mut self, manifest: ManifestFile) -> Self {
        self.manifests.push(manifest);
        self
    }

    /// Add multiple manifest files
    pub fn add_manifests(mut self, manifests: impl IntoIterator<Item = ManifestFile>) -> Self {
        self.manifests.extend(manifests);
        self
    }

    /// Set partition filter
    pub fn filter_partitions(mut self, expr: BoundExpressionTree) -> Self {
        self.partition_filter = Some(expr);
        self
    }

    /// Set metrics evaluator
    pub fn metrics_evaluator(mut self, evaluator: InclusiveMetricsEvaluator) -> Self {
        self.metrics_evaluator = Some(evaluator);
        self
    }

    /// Set partition set filter
    pub fn filter_partition_set(mut self, partition_set: PartitionSet) -> Self {
        self.partition_set = Some(partition_set);
        self
    }

    /// Include deleted entries
    pub fn include_deleted(mut self, include: bool) -> Self {
        self.include_deleted = include;
        self
    }

    /// Keep statistics
    pub fn keep_stats(mut self, keep: bool) -> Self {
        self.keep_stats = keep;
        self
    }

    /// Set case sensitivity
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Set parallelism (number of threads)
    /// If not set, uses Rayon's default (number of CPUs)
    pub fn parallelism(mut self, num_threads: usize) -> Self {
        self.parallelism = Some(num_threads);
        self
    }

    /// Set minimum sequence number filter
    pub fn min_sequence_number(mut self, min_seq: i64) -> Self {
        self.min_sequence_number = Some(min_seq);
        self
    }

    /// Build the manifest group
    pub fn build(self) -> ManifestGroup {
        ManifestGroup {
            manifests: self.manifests,
            partition_filter: self.partition_filter,
            metrics_evaluator: self.metrics_evaluator,
            partition_set: self.partition_set,
            include_deleted: self.include_deleted,
            keep_stats: self.keep_stats,
            case_sensitive: self.case_sensitive,
            parallelism: self.parallelism,
            min_sequence_number: self.min_sequence_number,
        }
    }
}

/// ManifestGroup - reads multiple manifest files in parallel
///
/// This is the main entry point for efficient manifest reading.
/// It uses Rayon for parallel processing and supports:
/// - Parallel manifest file reading
/// - Unified filtering across all manifests
/// - Aggregated statistics
///
/// # Example
///
/// ```rust,ignore
/// let entries = ManifestGroup::builder()
///     .add_manifest(ManifestFile::data("manifest1.avro"))
///     .add_manifest(ManifestFile::data("manifest2.avro"))
///     .filter_partitions(partition_expr)
///     .parallelism(4)
///     .build()
///     .read_data_entries()?;
/// ```
pub struct ManifestGroup {
    manifests: Vec<ManifestFile>,
    partition_filter: Option<BoundExpressionTree>,
    metrics_evaluator: Option<InclusiveMetricsEvaluator>,
    partition_set: Option<PartitionSet>,
    include_deleted: bool,
    keep_stats: bool,
    case_sensitive: bool,
    parallelism: Option<usize>,
    min_sequence_number: Option<i64>,
}

impl ManifestGroup {
    /// Create a new builder
    pub fn builder() -> ManifestGroupBuilder {
        ManifestGroupBuilder::new()
    }

    /// Get the number of manifests
    pub fn manifest_count(&self) -> usize {
        self.manifests.len()
    }

    /// Read all data entries from all data manifests in parallel
    pub fn read_data_entries(&self) -> Result<ParallelReadResult<DataManifestEntry>> {
        let data_manifests: Vec<_> = self
            .manifests
            .iter()
            .filter(|m| m.content == ManifestContent::Data)
            .filter(|m| m.might_contain_matching_files(self.min_sequence_number))
            .collect();

        if data_manifests.is_empty() {
            return Ok(ParallelReadResult::empty());
        }

        self.read_manifests_parallel(&data_manifests, |reader| reader.read_data_entries())
    }

    /// Read all delete entries from all delete manifests in parallel
    pub fn read_delete_entries(&self) -> Result<ParallelReadResult<DeleteManifestEntry>> {
        let delete_manifests: Vec<_> = self
            .manifests
            .iter()
            .filter(|m| m.content == ManifestContent::Deletes)
            .filter(|m| m.might_contain_matching_files(self.min_sequence_number))
            .collect();

        if delete_manifests.is_empty() {
            return Ok(ParallelReadResult::empty());
        }

        self.read_manifests_parallel(&delete_manifests, |reader| reader.read_delete_entries())
    }

    /// Read manifests in parallel using provided read function
    fn read_manifests_parallel<T, F>(
        &self,
        manifests: &[&ManifestFile],
        read_fn: F,
    ) -> Result<ParallelReadResult<T>>
    where
        T: Send,
        F: Fn(ManifestReader<std::io::BufReader<std::fs::File>>) -> Result<Vec<T>> + Sync,
    {
        // Configure thread pool if needed
        let pool = if let Some(num_threads) = self.parallelism {
            Some(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(num_threads)
                    .build()
                    .map_err(|e| Error::other(format!("Failed to create thread pool: {}", e)))?,
            )
        } else {
            None
        };

        // Atomic counters for statistics
        let total_entries = Arc::new(AtomicU64::new(0));
        let skipped_manifests = Arc::new(AtomicU64::new(0));
        let processed_manifests = Arc::new(AtomicU64::new(0));

        // Clone references for parallel access
        let partition_filter = &self.partition_filter;
        let metrics_evaluator = &self.metrics_evaluator;
        let partition_set = &self.partition_set;
        let include_deleted = self.include_deleted;
        let keep_stats = self.keep_stats;
        let case_sensitive = self.case_sensitive;

        // Process manifests in parallel
        let process = || {
            manifests
                .par_iter()
                .filter_map(|manifest| {
                    // Build reader for this manifest
                    let mut builder = ManifestReaderBuilder::new()
                        .include_deleted(include_deleted)
                        .keep_stats(keep_stats)
                        .case_sensitive(case_sensitive);

                    if let Some(first_row_id) = manifest.first_row_id {
                        builder = builder.first_row_id(first_row_id);
                    }

                    if let Some(ref filter) = partition_filter {
                        builder = builder.filter_partitions(filter.clone());
                    }

                    if let Some(ref evaluator) = metrics_evaluator {
                        builder = builder.metrics_evaluator(evaluator.clone());
                    }

                    if let Some(ref pset) = partition_set {
                        builder = builder.filter_partition_set(pset.clone());
                    }

                    // Open and read manifest
                    match builder.open(&manifest.path) {
                        Ok(reader) => {
                            match read_fn(reader) {
                                Ok(entries) => {
                                    total_entries.fetch_add(entries.len() as u64, Ordering::Relaxed);
                                    processed_manifests.fetch_add(1, Ordering::Relaxed);
                                    Some(Ok(entries))
                                }
                                Err(e) => Some(Err(e)),
                            }
                        }
                        Err(e) => {
                            skipped_manifests.fetch_add(1, Ordering::Relaxed);
                            Some(Err(e))
                        }
                    }
                })
                .collect::<Vec<_>>()
        };

        let results: Vec<Result<Vec<T>>> = if let Some(ref pool) = pool {
            pool.install(process)
        } else {
            process()
        };

        // Collect results and check for errors
        let mut all_entries = Vec::new();
        for result in results {
            match result {
                Ok(entries) => all_entries.extend(entries),
                Err(e) => return Err(e),
            }
        }

        Ok(ParallelReadResult {
            entries: all_entries,
            stats: ParallelReadStats {
                total_manifests: manifests.len() as u64,
                processed_manifests: processed_manifests.load(Ordering::Relaxed),
                skipped_manifests: skipped_manifests.load(Ordering::Relaxed),
                total_entries: total_entries.load(Ordering::Relaxed),
            },
        })
    }

    /// Read data entries with a callback for each manifest
    /// This is useful for progress reporting or streaming processing
    pub fn read_data_entries_with_callback<F>(
        &self,
        mut callback: F,
    ) -> Result<ParallelReadStats>
    where
        F: FnMut(&ManifestFile, Vec<DataManifestEntry>) + Send,
    {
        let data_manifests: Vec<_> = self
            .manifests
            .iter()
            .filter(|m| m.content == ManifestContent::Data)
            .filter(|m| m.might_contain_matching_files(self.min_sequence_number))
            .cloned()
            .collect();

        let total_entries = AtomicU64::new(0);
        let processed_manifests = AtomicU64::new(0);
        let skipped_manifests = AtomicU64::new(0);

        for manifest in &data_manifests {
            let mut builder = ManifestReaderBuilder::new()
                .include_deleted(self.include_deleted)
                .keep_stats(self.keep_stats)
                .case_sensitive(self.case_sensitive);

            if let Some(first_row_id) = manifest.first_row_id {
                builder = builder.first_row_id(first_row_id);
            }

            if let Some(ref filter) = self.partition_filter {
                builder = builder.filter_partitions(filter.clone());
            }

            if let Some(ref evaluator) = self.metrics_evaluator {
                builder = builder.metrics_evaluator(evaluator.clone());
            }

            if let Some(ref pset) = self.partition_set {
                builder = builder.filter_partition_set(pset.clone());
            }

            match builder.open(&manifest.path) {
                Ok(reader) => {
                    match reader.read_data_entries() {
                        Ok(entries) => {
                            total_entries.fetch_add(entries.len() as u64, Ordering::Relaxed);
                            processed_manifests.fetch_add(1, Ordering::Relaxed);
                            callback(manifest, entries);
                        }
                        Err(_) => {
                            skipped_manifests.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                Err(_) => {
                    skipped_manifests.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        Ok(ParallelReadStats {
            total_manifests: data_manifests.len() as u64,
            processed_manifests: processed_manifests.load(Ordering::Relaxed),
            skipped_manifests: skipped_manifests.load(Ordering::Relaxed),
            total_entries: total_entries.load(Ordering::Relaxed),
        })
    }
}

/// Result of parallel manifest reading
#[derive(Debug)]
pub struct ParallelReadResult<T> {
    /// All entries from all manifests
    pub entries: Vec<T>,
    /// Reading statistics
    pub stats: ParallelReadStats,
}

impl<T> ParallelReadResult<T> {
    /// Create an empty result
    pub fn empty() -> Self {
        ParallelReadResult {
            entries: Vec::new(),
            stats: ParallelReadStats::default(),
        }
    }

    /// Get the number of entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Consume and return entries
    pub fn into_entries(self) -> Vec<T> {
        self.entries
    }
}

/// Statistics from parallel reading
#[derive(Debug, Clone, Default)]
pub struct ParallelReadStats {
    /// Total number of manifests
    pub total_manifests: u64,
    /// Number of manifests successfully processed
    pub processed_manifests: u64,
    /// Number of manifests skipped (due to errors or filtering)
    pub skipped_manifests: u64,
    /// Total number of entries read
    pub total_entries: u64,
}

impl ParallelReadStats {
    /// Get the success rate
    pub fn success_rate(&self) -> f64 {
        if self.total_manifests == 0 {
            100.0
        } else {
            (self.processed_manifests as f64 / self.total_manifests as f64) * 100.0
        }
    }
}

/// Parallel iterator over manifest entries
pub struct ParallelManifestIter {
    receiver: crossbeam_channel::Receiver<Result<DataManifestEntry>>,
    _handles: Vec<std::thread::JoinHandle<()>>,
}

impl ParallelManifestIter {
    /// Create a new parallel iterator over manifest files
    pub fn new(
        manifests: Vec<ManifestFile>,
        partition_filter: Option<BoundExpressionTree>,
        metrics_evaluator: Option<InclusiveMetricsEvaluator>,
        partition_set: Option<PartitionSet>,
        num_workers: usize,
    ) -> Self {
        let (sender, receiver) = crossbeam_channel::bounded(1024);

        // Distribute manifests across workers
        let chunks: Vec<Vec<ManifestFile>> = manifests
            .into_iter()
            .enumerate()
            .fold(vec![Vec::new(); num_workers], |mut acc, (i, m)| {
                acc[i % num_workers].push(m);
                acc
            });

        let handles: Vec<_> = chunks
            .into_iter()
            .map(|chunk| {
                let sender = sender.clone();
                let partition_filter = partition_filter.clone();
                let metrics_evaluator = metrics_evaluator.clone();
                let partition_set = partition_set.clone();

                std::thread::spawn(move || {
                    for manifest in chunk {
                        if manifest.content != ManifestContent::Data {
                            continue;
                        }

                        let mut builder = ManifestReaderBuilder::new();

                        if let Some(first_row_id) = manifest.first_row_id {
                            builder = builder.first_row_id(first_row_id);
                        }

                        if let Some(ref filter) = partition_filter {
                            builder = builder.filter_partitions(filter.clone());
                        }

                        if let Some(ref evaluator) = metrics_evaluator {
                            builder = builder.metrics_evaluator(evaluator.clone());
                        }

                        if let Some(ref pset) = partition_set {
                            builder = builder.filter_partition_set(pset.clone());
                        }

                        match builder.open(&manifest.path) {
                            Ok(reader) => {
                                match reader.read_data_entries() {
                                    Ok(entries) => {
                                        for entry in entries {
                                            if sender.send(Ok(entry)).is_err() {
                                                return;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        let _ = sender.send(Err(e));
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = sender.send(Err(e));
                            }
                        }
                    }
                })
            })
            .collect();

        ParallelManifestIter {
            receiver,
            _handles: handles,
        }
    }
}

impl Iterator for ParallelManifestIter {
    type Item = Result<DataManifestEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.recv().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_file_creation() {
        let mf = ManifestFile::data("test.avro")
            .with_first_row_id(100)
            .with_counts(10, 20, 5)
            .with_spec_id(1);

        assert_eq!(mf.content, ManifestContent::Data);
        assert_eq!(mf.first_row_id, Some(100));
        assert_eq!(mf.added_files_count, Some(10));
        assert_eq!(mf.partition_spec_id, 1);
    }

    #[test]
    fn test_manifest_group_builder() {
        let group = ManifestGroup::builder()
            .add_manifest(ManifestFile::data("m1.avro"))
            .add_manifest(ManifestFile::data("m2.avro"))
            .parallelism(4)
            .keep_stats(false)
            .build();

        assert_eq!(group.manifest_count(), 2);
        assert_eq!(group.parallelism, Some(4));
        assert!(!group.keep_stats);
    }

    #[test]
    fn test_parallel_read_stats() {
        let stats = ParallelReadStats {
            total_manifests: 10,
            processed_manifests: 8,
            skipped_manifests: 2,
            total_entries: 1000,
        };

        assert_eq!(stats.success_rate(), 80.0);
    }

    #[test]
    fn test_manifest_sequence_filter() {
        let mf = ManifestFile::data("test.avro").with_sequence_numbers(100, 50);

        assert!(mf.might_contain_matching_files(Some(30)));
        assert!(mf.might_contain_matching_files(Some(50)));
        assert!(!mf.might_contain_matching_files(Some(60)));
        assert!(mf.might_contain_matching_files(None));
    }

    #[test]
    fn test_empty_result() {
        let result: ParallelReadResult<DataManifestEntry> = ParallelReadResult::empty();
        assert!(result.is_empty());
        assert_eq!(result.len(), 0);
    }
}
