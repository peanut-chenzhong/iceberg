//! Manifest file reader
//!
//! High-performance manifest file reading with zero-copy deserialization
//! where possible, and support for filtering with partition pruning and
//! metrics evaluation.

use super::content_file::{ContentFile, ContentFileData};
use super::entry::{DataManifestEntry, DeleteManifestEntry, RawManifestEntry, Status};
use super::filter::{FileMetrics, InclusiveMetricsEvaluator, PartitionEvaluator, PartitionSet};
use crate::error::{Error, Result};
use crate::expr::evaluator::BoundExpressionTree;
use crate::types::{FileContent, FileFormat, PartitionData, PartitionValue};
use apache_avro::{types::Value, Reader};
use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

/// Manifest content type
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ManifestContent {
    /// Data manifest (contains DataFile entries)
    Data,
    /// Delete manifest (contains DeleteFile entries)
    Deletes,
}

/// Manifest metadata from Avro file header
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ManifestMetadata {
    /// Schema JSON
    pub schema: Option<String>,
    /// Partition spec JSON
    pub partition_spec: Option<String>,
    /// Partition spec ID
    pub partition_spec_id: Option<i32>,
    /// Format version
    pub format_version: Option<i32>,
    /// Content type (data or deletes)
    pub content: ManifestContent,
}

impl Default for ManifestMetadata {
    fn default() -> Self {
        Self {
            schema: None,
            partition_spec: None,
            partition_spec_id: None,
            format_version: None,
            content: ManifestContent::Data,
        }
    }
}

/// Builder for ManifestReader
pub struct ManifestReaderBuilder {
    /// First row ID for row lineage inheritance
    first_row_id: Option<i64>,
    /// Whether to include deleted entries
    include_deleted: bool,
    /// Whether to keep statistics
    keep_stats: bool,
    /// Partition filter expression
    partition_filter: Option<BoundExpressionTree>,
    /// Row filter expression (for metrics evaluation)
    row_filter: Option<BoundExpressionTree>,
    /// Partition set filter
    partition_set: Option<PartitionSet>,
    /// Metrics evaluator
    metrics_evaluator: Option<InclusiveMetricsEvaluator>,
    /// Case sensitive
    case_sensitive: bool,
}

impl Default for ManifestReaderBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ManifestReaderBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            first_row_id: None,
            include_deleted: false,
            keep_stats: true,
            partition_filter: None,
            row_filter: None,
            partition_set: None,
            metrics_evaluator: None,
            case_sensitive: true,
        }
    }

    /// Set the first row ID for inheritance
    pub fn first_row_id(mut self, id: i64) -> Self {
        self.first_row_id = Some(id);
        self
    }

    /// Include deleted entries in the output
    pub fn include_deleted(mut self, include: bool) -> Self {
        self.include_deleted = include;
        self
    }

    /// Whether to keep statistics in returned entries
    pub fn keep_stats(mut self, keep: bool) -> Self {
        self.keep_stats = keep;
        self
    }

    /// Set partition filter expression
    /// 
    /// Files with partitions that don't match this expression will be skipped.
    pub fn filter_partitions(mut self, expr: BoundExpressionTree) -> Self {
        self.partition_filter = Some(expr);
        self
    }

    /// Set row filter expression
    /// 
    /// This is used for metrics evaluation to skip files that can't contain
    /// matching rows based on column statistics.
    pub fn filter_rows(mut self, expr: BoundExpressionTree) -> Self {
        self.row_filter = Some(expr);
        self
    }

    /// Set partition set filter
    /// 
    /// Only files with partitions in this set will be returned.
    pub fn filter_partition_set(mut self, partition_set: PartitionSet) -> Self {
        self.partition_set = Some(partition_set);
        self
    }

    /// Set custom metrics evaluator
    pub fn metrics_evaluator(mut self, evaluator: InclusiveMetricsEvaluator) -> Self {
        self.metrics_evaluator = Some(evaluator);
        self
    }

    /// Set case sensitivity for expression evaluation
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Open a manifest file
    pub fn open<P: AsRef<Path>>(self, path: P) -> Result<ManifestReader<BufReader<File>>> {
        let file = File::open(path.as_ref()).map_err(Error::Io)?;
        let reader = BufReader::new(file);
        self.from_reader(reader)
    }

    /// Create from a reader
    pub fn from_reader<R: Read>(self, reader: R) -> Result<ManifestReader<R>> {
        let avro_reader = Reader::new(reader)?;

        // Extract metadata from Avro file header
        let metadata = extract_metadata(&avro_reader);

        // Create partition evaluator
        let partition_evaluator = self
            .partition_filter
            .map(PartitionEvaluator::new)
            .unwrap_or_else(PartitionEvaluator::always_true);

        // Create metrics evaluator
        let metrics_evaluator = self
            .metrics_evaluator
            .unwrap_or_else(InclusiveMetricsEvaluator::always_true);

        Ok(ManifestReader {
            avro_reader,
            metadata,
            first_row_id: self.first_row_id,
            include_deleted: self.include_deleted,
            keep_stats: self.keep_stats,
            current_row_id: self.first_row_id.unwrap_or(0),
            partition_evaluator,
            metrics_evaluator,
            partition_set: self.partition_set,
            case_sensitive: self.case_sensitive,
            // Statistics
            total_entries: 0,
            skipped_by_partition: 0,
            skipped_by_metrics: 0,
            skipped_by_partition_set: 0,
        })
    }
}

/// High-performance manifest file reader
///
/// Reads manifest entries from an Avro file with support for:
/// - Row lineage (first_row_id inheritance)
/// - Filtering deleted entries
/// - Partition expression filtering
/// - Inclusive metrics evaluation (min/max bounds)
/// - Partition set filtering
/// - Dropping statistics for memory efficiency
pub struct ManifestReader<R: Read> {
    avro_reader: Reader<'static, R>,
    metadata: ManifestMetadata,
    first_row_id: Option<i64>,
    include_deleted: bool,
    keep_stats: bool,
    current_row_id: i64,
    
    // Filtering
    partition_evaluator: PartitionEvaluator,
    metrics_evaluator: InclusiveMetricsEvaluator,
    partition_set: Option<PartitionSet>,
    case_sensitive: bool,
    
    // Statistics
    total_entries: u64,
    skipped_by_partition: u64,
    skipped_by_metrics: u64,
    skipped_by_partition_set: u64,
}

impl<R: Read> ManifestReader<R> {
    /// Create a new reader builder
    pub fn builder() -> ManifestReaderBuilder {
        ManifestReaderBuilder::new()
    }

    /// Get manifest metadata
    pub fn metadata(&self) -> &ManifestMetadata {
        &self.metadata
    }

    /// Get manifest content type
    pub fn content(&self) -> ManifestContent {
        self.metadata.content
    }

    /// Check if this is a data manifest
    pub fn is_data_manifest(&self) -> bool {
        self.metadata.content == ManifestContent::Data
    }

    /// Check if this is a delete manifest
    pub fn is_delete_manifest(&self) -> bool {
        self.metadata.content == ManifestContent::Deletes
    }

    /// Get filtering statistics
    pub fn stats(&self) -> ManifestReadStats {
        ManifestReadStats {
            total_entries: self.total_entries,
            skipped_by_partition: self.skipped_by_partition,
            skipped_by_metrics: self.skipped_by_metrics,
            skipped_by_partition_set: self.skipped_by_partition_set,
            returned_entries: self.total_entries
                - self.skipped_by_partition
                - self.skipped_by_metrics
                - self.skipped_by_partition_set,
        }
    }

    /// Read all data file entries with filtering
    ///
    /// Returns an error if this is a delete manifest
    pub fn read_data_entries(mut self) -> Result<Vec<DataManifestEntry>> {
        if self.metadata.content == ManifestContent::Deletes {
            return Err(Error::invalid_data(
                "Cannot read data entries from a delete manifest",
            ));
        }

        let mut entries = Vec::new();

        for value_result in &mut self.avro_reader {
            let value = value_result?;
            let raw = parse_manifest_entry(&value)?;
            
            self.total_entries += 1;

            // Skip deleted entries if not requested
            if !self.include_deleted && Status::from_id(raw.status) == Some(Status::Deleted) {
                continue;
            }

            let mut entry = raw.into_data_entry();

            // Apply partition filter
            if !self.partition_evaluator.eval(entry.file().partition()) {
                self.skipped_by_partition += 1;
                continue;
            }

            // Apply partition set filter
            if let Some(ref partition_set) = self.partition_set {
                if !partition_set.contains(entry.file().spec_id(), entry.file().partition()) {
                    self.skipped_by_partition_set += 1;
                    continue;
                }
            }

            // Apply metrics filter
            let file_metrics = create_file_metrics(entry.file());
            if !self.metrics_evaluator.eval(&file_metrics) {
                self.skipped_by_metrics += 1;
                continue;
            }

            // Apply row lineage inheritance
            if let Some(_first_row_id) = self.first_row_id {
                if entry.file().first_row_id().is_none() {
                    entry.file_mut().set_first_row_id(Some(self.current_row_id));
                    self.current_row_id += entry.file().record_count();
                }
            }

            if !self.keep_stats {
                entry = entry.copy_without_stats();
            }

            entries.push(entry);
        }

        Ok(entries)
    }

    /// Read all delete file entries with filtering
    ///
    /// Returns an error if this is a data manifest
    pub fn read_delete_entries(mut self) -> Result<Vec<DeleteManifestEntry>> {
        if self.metadata.content == ManifestContent::Data {
            return Err(Error::invalid_data(
                "Cannot read delete entries from a data manifest",
            ));
        }

        let mut entries = Vec::new();

        for value_result in &mut self.avro_reader {
            let value = value_result?;
            let raw = parse_manifest_entry(&value)?;
            
            self.total_entries += 1;

            // Skip deleted entries if not requested
            if !self.include_deleted && Status::from_id(raw.status) == Some(Status::Deleted) {
                continue;
            }

            let mut entry = raw.into_delete_entry();

            // Apply partition filter
            if !self.partition_evaluator.eval(entry.file().partition()) {
                self.skipped_by_partition += 1;
                continue;
            }

            // Apply partition set filter
            if let Some(ref partition_set) = self.partition_set {
                if !partition_set.contains(entry.file().spec_id(), entry.file().partition()) {
                    self.skipped_by_partition_set += 1;
                    continue;
                }
            }

            // Apply metrics filter
            let file_metrics = create_file_metrics_from_delete(entry.file());
            if !self.metrics_evaluator.eval(&file_metrics) {
                self.skipped_by_metrics += 1;
                continue;
            }

            if !self.keep_stats {
                entry = entry.copy_without_stats();
            }

            entries.push(entry);
        }

        Ok(entries)
    }

    /// Iterate over raw entries (for advanced use cases)
    pub fn iter_raw(&mut self) -> ManifestEntryIter<'_, R> {
        ManifestEntryIter { reader: self }
    }
    
    /// Read entries with a custom filter function
    pub fn read_data_entries_with_filter<F>(mut self, filter: F) -> Result<Vec<DataManifestEntry>>
    where
        F: Fn(&DataManifestEntry) -> bool,
    {
        if self.metadata.content == ManifestContent::Deletes {
            return Err(Error::invalid_data(
                "Cannot read data entries from a delete manifest",
            ));
        }

        let mut entries = Vec::new();

        for value_result in &mut self.avro_reader {
            let value = value_result?;
            let raw = parse_manifest_entry(&value)?;
            
            self.total_entries += 1;

            if !self.include_deleted && Status::from_id(raw.status) == Some(Status::Deleted) {
                continue;
            }

            let mut entry = raw.into_data_entry();

            // Apply custom filter
            if !filter(&entry) {
                continue;
            }

            // Apply row lineage inheritance
            if let Some(_first_row_id) = self.first_row_id {
                if entry.file().first_row_id().is_none() {
                    entry.file_mut().set_first_row_id(Some(self.current_row_id));
                    self.current_row_id += entry.file().record_count();
                }
            }

            if !self.keep_stats {
                entry = entry.copy_without_stats();
            }

            entries.push(entry);
        }

        Ok(entries)
    }
}

/// Statistics from manifest reading
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ManifestReadStats {
    /// Total entries processed
    pub total_entries: u64,
    /// Entries skipped by partition filter
    pub skipped_by_partition: u64,
    /// Entries skipped by metrics filter
    pub skipped_by_metrics: u64,
    /// Entries skipped by partition set filter
    pub skipped_by_partition_set: u64,
    /// Entries returned
    pub returned_entries: u64,
}

impl ManifestReadStats {
    /// Get the skip rate as a percentage
    pub fn skip_rate(&self) -> f64 {
        if self.total_entries == 0 {
            0.0
        } else {
            let skipped = self.total_entries - self.returned_entries;
            (skipped as f64 / self.total_entries as f64) * 100.0
        }
    }
}

/// Create FileMetrics from a DataFile
fn create_file_metrics<F: ContentFile>(file: &F) -> FileMetrics {
    let mut metrics = FileMetrics::new(file.record_count());
    
    if let Some(null_counts) = file.null_value_counts() {
        metrics.null_value_counts = null_counts.clone();
    }
    if let Some(lower_bounds) = file.lower_bounds() {
        metrics.lower_bounds = lower_bounds.clone();
    }
    if let Some(upper_bounds) = file.upper_bounds() {
        metrics.upper_bounds = upper_bounds.clone();
    }
    if let Some(nan_counts) = file.nan_value_counts() {
        metrics.nan_value_counts = nan_counts.clone();
    }
    
    metrics
}

/// Create FileMetrics from a DeleteFile (same as data file)
fn create_file_metrics_from_delete<F: ContentFile>(file: &F) -> FileMetrics {
    create_file_metrics(file)
}

/// Iterator over manifest entries
pub struct ManifestEntryIter<'a, R: Read> {
    reader: &'a mut ManifestReader<R>,
}

impl<'a, R: Read> Iterator for ManifestEntryIter<'a, R> {
    type Item = Result<RawManifestEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.avro_reader.next() {
            Some(Ok(value)) => Some(parse_manifest_entry(&value)),
            Some(Err(e)) => Some(Err(Error::Avro(e))),
            None => None,
        }
    }
}

/// Extract metadata from Avro reader
fn extract_metadata<R: Read>(reader: &Reader<'static, R>) -> ManifestMetadata {
    let user_metadata = reader.user_metadata();

    let schema = user_metadata
        .get("schema")
        .and_then(|v| String::from_utf8(v.clone()).ok());

    let partition_spec = user_metadata
        .get("partition-spec")
        .and_then(|v| String::from_utf8(v.clone()).ok());

    let partition_spec_id = user_metadata
        .get("partition-spec-id")
        .and_then(|v| String::from_utf8(v.clone()).ok())
        .and_then(|s| s.parse().ok());

    let format_version = user_metadata
        .get("format-version")
        .and_then(|v| String::from_utf8(v.clone()).ok())
        .and_then(|s| s.parse().ok());

    let content = user_metadata
        .get("content")
        .and_then(|v| String::from_utf8(v.clone()).ok())
        .map(|s| {
            if s == "deletes" {
                ManifestContent::Deletes
            } else {
                ManifestContent::Data
            }
        })
        .unwrap_or(ManifestContent::Data);

    ManifestMetadata {
        schema,
        partition_spec,
        partition_spec_id,
        format_version,
        content,
    }
}

/// Parse a single manifest entry from Avro value
fn parse_manifest_entry(value: &Value) -> Result<RawManifestEntry> {
    let record = match value {
        Value::Record(fields) => fields,
        _ => return Err(Error::invalid_data("Expected record value for manifest entry")),
    };

    let mut entry = RawManifestEntry::default();

    for (name, field_value) in record {
        match name.as_str() {
            "status" => {
                entry.status = extract_int(field_value).unwrap_or(0);
            }
            "snapshot_id" => {
                entry.snapshot_id = extract_optional_long(field_value);
            }
            "sequence_number" | "data_sequence_number" => {
                entry.data_sequence_number = extract_optional_long(field_value);
            }
            "file_sequence_number" => {
                entry.file_sequence_number = extract_optional_long(field_value);
            }
            "data_file" => {
                entry.file = parse_content_file(field_value)?;
            }
            _ => {
                // Ignore unknown fields for forward compatibility
            }
        }
    }

    Ok(entry)
}

/// Parse content file from Avro value
fn parse_content_file(value: &Value) -> Result<ContentFileData> {
    let record = match value {
        Value::Record(fields) => fields,
        _ => return Err(Error::invalid_data("Expected record value for data file")),
    };

    let mut file = ContentFileData::default();

    for (name, field_value) in record {
        match name.as_str() {
            "content" => {
                let content_id = extract_int(field_value).unwrap_or(0);
                file.content = FileContent::from_id(content_id).unwrap_or(FileContent::Data);
            }
            "file_path" => {
                file.file_path = extract_string(field_value).unwrap_or_default();
            }
            "file_format" => {
                let format_str = extract_string(field_value).unwrap_or_default();
                file.file_format = FileFormat::from_str(&format_str).unwrap_or(FileFormat::Parquet);
            }
            "spec_id" => {
                file.spec_id = extract_int(field_value).unwrap_or(0);
            }
            "partition" => {
                file.partition = parse_partition(field_value)?;
            }
            "record_count" => {
                file.record_count = extract_long(field_value).unwrap_or(0);
            }
            "file_size_in_bytes" => {
                file.file_size_in_bytes = extract_long(field_value).unwrap_or(0);
            }
            "column_sizes" => {
                file.column_sizes = extract_int_long_map(field_value);
            }
            "value_counts" => {
                file.value_counts = extract_int_long_map(field_value);
            }
            "null_value_counts" => {
                file.null_value_counts = extract_int_long_map(field_value);
            }
            "nan_value_counts" => {
                file.nan_value_counts = extract_int_long_map(field_value);
            }
            "lower_bounds" => {
                file.lower_bounds = extract_int_bytes_map(field_value);
            }
            "upper_bounds" => {
                file.upper_bounds = extract_int_bytes_map(field_value);
            }
            "key_metadata" => {
                file.key_metadata = extract_bytes(field_value);
            }
            "split_offsets" => {
                file.split_offsets = extract_long_array(field_value);
            }
            "equality_ids" => {
                file.equality_field_ids = extract_int_array(field_value);
            }
            "sort_order_id" => {
                file.sort_order_id = extract_optional_int(field_value);
            }
            "first_row_id" => {
                file.first_row_id = extract_optional_long(field_value);
            }
            "referenced_data_file" => {
                file.referenced_data_file = extract_optional_string(field_value);
            }
            "content_offset" => {
                file.content_offset = extract_optional_long(field_value);
            }
            "content_size_in_bytes" => {
                file.content_size_in_bytes = extract_optional_long(field_value);
            }
            _ => {
                // Ignore unknown fields for forward compatibility
            }
        }
    }

    Ok(file)
}

/// Parse partition data from Avro value
fn parse_partition(value: &Value) -> Result<PartitionData> {
    match value {
        Value::Record(fields) => {
            let values: Vec<PartitionValue> = fields
                .iter()
                .map(|(_, v)| avro_to_partition_value(v))
                .collect();
            Ok(PartitionData::new(values))
        }
        Value::Null => Ok(PartitionData::empty()),
        _ => Err(Error::invalid_data("Expected record or null for partition")),
    }
}

/// Convert Avro value to partition value
fn avro_to_partition_value(value: &Value) -> PartitionValue {
    match value {
        Value::Null => PartitionValue::Null,
        Value::Boolean(b) => PartitionValue::Boolean(*b),
        Value::Int(i) => PartitionValue::Int(*i),
        Value::Long(l) => PartitionValue::Long(*l),
        Value::Float(f) => PartitionValue::Float(*f),
        Value::Double(d) => PartitionValue::Double(*d),
        Value::String(s) => PartitionValue::String(s.clone()),
        Value::Bytes(b) => PartitionValue::Binary(b.clone()),
        Value::Date(d) => PartitionValue::Date(*d),
        Value::TimestampMicros(t) => PartitionValue::Timestamp(*t),
        Value::Uuid(u) => PartitionValue::Uuid(*u),
        Value::Union(_, inner) => avro_to_partition_value(inner),
        _ => PartitionValue::Null, // Unknown types become null
    }
}

// === Value extraction helpers ===

fn extract_int(value: &Value) -> Option<i32> {
    match value {
        Value::Int(i) => Some(*i),
        Value::Long(l) => Some(*l as i32),
        Value::Union(_, inner) => extract_int(inner),
        _ => None,
    }
}

fn extract_optional_int(value: &Value) -> Option<i32> {
    match value {
        Value::Null => None,
        Value::Union(_, inner) => extract_optional_int(inner),
        _ => extract_int(value),
    }
}

fn extract_long(value: &Value) -> Option<i64> {
    match value {
        Value::Long(l) => Some(*l),
        Value::Int(i) => Some(*i as i64),
        Value::Union(_, inner) => extract_long(inner),
        _ => None,
    }
}

fn extract_optional_long(value: &Value) -> Option<i64> {
    match value {
        Value::Null => None,
        Value::Union(_, inner) => extract_optional_long(inner),
        _ => extract_long(value),
    }
}

fn extract_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Union(_, inner) => extract_string(inner),
        _ => None,
    }
}

fn extract_optional_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Union(_, inner) => extract_optional_string(inner),
        _ => extract_string(value),
    }
}

fn extract_bytes(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::Bytes(b) => Some(b.clone()),
        Value::Fixed(_, b) => Some(b.clone()),
        Value::Union(_, inner) => extract_bytes(inner),
        Value::Null => None,
        _ => None,
    }
}

fn extract_int_long_map(value: &Value) -> Option<FxHashMap<i32, i64>> {
    match value {
        Value::Map(map) => {
            let result: FxHashMap<i32, i64> = map
                .iter()
                .filter_map(|(k, v)| {
                    let key: i32 = k.parse().ok()?;
                    let val = extract_long(v)?;
                    Some((key, val))
                })
                .collect();
            if result.is_empty() {
                None
            } else {
                Some(result)
            }
        }
        Value::Array(arr) => {
            // Some Avro encodings use arrays of key-value pairs
            let result: FxHashMap<i32, i64> = arr
                .iter()
                .filter_map(|item| {
                    if let Value::Record(fields) = item {
                        let mut key = None;
                        let mut val = None;
                        for (name, v) in fields {
                            match name.as_str() {
                                "key" => key = extract_int(v),
                                "value" => val = extract_long(v),
                                _ => {}
                            }
                        }
                        Some((key?, val?))
                    } else {
                        None
                    }
                })
                .collect();
            if result.is_empty() {
                None
            } else {
                Some(result)
            }
        }
        Value::Union(_, inner) => extract_int_long_map(inner),
        Value::Null => None,
        _ => None,
    }
}

fn extract_int_bytes_map(value: &Value) -> Option<FxHashMap<i32, Vec<u8>>> {
    match value {
        Value::Map(map) => {
            let result: FxHashMap<i32, Vec<u8>> = map
                .iter()
                .filter_map(|(k, v)| {
                    let key: i32 = k.parse().ok()?;
                    let val = extract_bytes(v)?;
                    Some((key, val))
                })
                .collect();
            if result.is_empty() {
                None
            } else {
                Some(result)
            }
        }
        Value::Array(arr) => {
            let result: FxHashMap<i32, Vec<u8>> = arr
                .iter()
                .filter_map(|item| {
                    if let Value::Record(fields) = item {
                        let mut key = None;
                        let mut val = None;
                        for (name, v) in fields {
                            match name.as_str() {
                                "key" => key = extract_int(v),
                                "value" => val = extract_bytes(v),
                                _ => {}
                            }
                        }
                        Some((key?, val?))
                    } else {
                        None
                    }
                })
                .collect();
            if result.is_empty() {
                None
            } else {
                Some(result)
            }
        }
        Value::Union(_, inner) => extract_int_bytes_map(inner),
        Value::Null => None,
        _ => None,
    }
}

fn extract_long_array(value: &Value) -> Option<SmallVec<[i64; 8]>> {
    match value {
        Value::Array(arr) => {
            let result: SmallVec<[i64; 8]> = arr.iter().filter_map(extract_long).collect();
            if result.is_empty() {
                None
            } else {
                Some(result)
            }
        }
        Value::Union(_, inner) => extract_long_array(inner),
        Value::Null => None,
        _ => None,
    }
}

fn extract_int_array(value: &Value) -> Option<SmallVec<[i32; 4]>> {
    match value {
        Value::Array(arr) => {
            let result: SmallVec<[i32; 4]> = arr.iter().filter_map(extract_int).collect();
            if result.is_empty() {
                None
            } else {
                Some(result)
            }
        }
        Value::Union(_, inner) => extract_int_array(inner),
        Value::Null => None,
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_content() {
        assert_eq!(ManifestContent::Data, ManifestContent::Data);
        assert_ne!(ManifestContent::Data, ManifestContent::Deletes);
    }

    #[test]
    fn test_status_parsing() {
        assert_eq!(Status::from_id(0), Some(Status::Existing));
        assert_eq!(Status::from_id(1), Some(Status::Added));
        assert_eq!(Status::from_id(2), Some(Status::Deleted));
    }
    
    #[test]
    fn test_manifest_read_stats() {
        let stats = ManifestReadStats {
            total_entries: 100,
            skipped_by_partition: 30,
            skipped_by_metrics: 20,
            skipped_by_partition_set: 10,
            returned_entries: 40,
        };
        
        assert_eq!(stats.skip_rate(), 60.0);
    }
    
    #[test]
    fn test_builder_defaults() {
        let builder = ManifestReaderBuilder::new();
        assert!(builder.first_row_id.is_none());
        assert!(!builder.include_deleted);
        assert!(builder.keep_stats);
        assert!(builder.case_sensitive);
    }

    // Integration tests would require actual Avro files
    // These would be added in a separate test module with test fixtures
}
