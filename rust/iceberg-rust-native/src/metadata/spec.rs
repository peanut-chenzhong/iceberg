//! Iceberg Partition and Sort Order specifications
//!
//! This module defines partition specs and sort orders.

use serde::{Deserialize, Serialize};

/// Transform function for partitioning
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Transform {
    /// Identity transform (no transformation)
    Identity,
    /// Year from a date or timestamp
    Year,
    /// Month from a date or timestamp
    Month,
    /// Day from a date or timestamp
    Day,
    /// Hour from a timestamp
    Hour,
    /// Hash with specified number of buckets
    #[serde(rename = "bucket")]
    Bucket(i32),
    /// Truncate to specified width
    #[serde(rename = "truncate")]
    Truncate(i32),
    /// Void transform (always returns null)
    Void,
}

/// Number of days from year 0 (ISO calendar) to Unix epoch (1970-01-01)
const EPOCH_DAY: i64 = 719528;
/// Number of hours per day
const HOURS_PER_DAY: i64 = 24;
/// Number of micros per hour
const MICROS_PER_HOUR: i64 = 3600_000_000;

impl Transform {
    /// Parse transform from string
    pub fn from_str(s: &str) -> Option<Self> {
        if s == "identity" {
            return Some(Transform::Identity);
        }
        if s == "year" {
            return Some(Transform::Year);
        }
        if s == "month" {
            return Some(Transform::Month);
        }
        if s == "day" {
            return Some(Transform::Day);
        }
        if s == "hour" {
            return Some(Transform::Hour);
        }
        if s == "void" {
            return Some(Transform::Void);
        }
        if let Some(rest) = s.strip_prefix("bucket[") {
            if let Some(num_str) = rest.strip_suffix(']') {
                if let Ok(n) = num_str.parse() {
                    return Some(Transform::Bucket(n));
                }
            }
        }
        if let Some(rest) = s.strip_prefix("truncate[") {
            if let Some(num_str) = rest.strip_suffix(']') {
                if let Ok(n) = num_str.parse() {
                    return Some(Transform::Truncate(n));
                }
            }
        }
        None
    }

    /// Convert transform to string representation
    pub fn to_string(&self) -> String {
        match self {
            Transform::Identity => "identity".to_string(),
            Transform::Year => "year".to_string(),
            Transform::Month => "month".to_string(),
            Transform::Day => "day".to_string(),
            Transform::Hour => "hour".to_string(),
            Transform::Void => "void".to_string(),
            Transform::Bucket(n) => format!("bucket[{}]", n),
            Transform::Truncate(n) => format!("truncate[{}]", n),
        }
    }

    /// Check if this is a temporal transform (year, month, day, hour)
    pub fn is_temporal(&self) -> bool {
        matches!(self, Transform::Year | Transform::Month | Transform::Day | Transform::Hour)
    }

    /// Check if this transform preserves ordering
    pub fn preserves_order(&self) -> bool {
        matches!(
            self,
            Transform::Identity
                | Transform::Year
                | Transform::Month
                | Transform::Day
                | Transform::Hour
                | Transform::Truncate(_)
        )
    }

    /// Check if this transform satisfies another transform
    /// e.g., day satisfies month (because day is more specific)
    pub fn satisfies(&self, other: &Transform) -> bool {
        match (self, other) {
            (a, b) if a == b => true,
            // More specific temporal satisfies less specific
            (Transform::Hour, Transform::Day)
            | (Transform::Hour, Transform::Month)
            | (Transform::Hour, Transform::Year) => true,
            (Transform::Day, Transform::Month) | (Transform::Day, Transform::Year) => true,
            (Transform::Month, Transform::Year) => true,
            _ => false,
        }
    }

    /// Apply the transform to an integer value (date days or timestamp micros)
    pub fn apply_to_timestamp_micros(&self, micros: i64) -> Option<i32> {
        match self {
            Transform::Identity => None, // Identity doesn't change the type
            Transform::Year => {
                let days = micros / (MICROS_PER_HOUR * HOURS_PER_DAY);
                Some(days_to_year(days as i32))
            }
            Transform::Month => {
                let days = micros / (MICROS_PER_HOUR * HOURS_PER_DAY);
                Some(days_to_month(days as i32))
            }
            Transform::Day => {
                let days = micros / (MICROS_PER_HOUR * HOURS_PER_DAY);
                Some(days as i32)
            }
            Transform::Hour => {
                let hours = micros / MICROS_PER_HOUR;
                Some(hours as i32)
            }
            Transform::Void => None,
            Transform::Bucket(n) => {
                // Bucket hash (simplified - actual implementation uses Murmur3)
                let hash = murmur3_x86_32(&micros.to_le_bytes(), 0);
                Some(((hash as i64 & i64::MAX) % (*n as i64)) as i32)
            }
            Transform::Truncate(width) => {
                let w = *width as i64;
                Some((micros - (micros % w).max(0)) as i32)
            }
        }
    }

    /// Apply the transform to a date (days since epoch)
    pub fn apply_to_date(&self, days: i32) -> Option<i32> {
        match self {
            Transform::Identity => Some(days),
            Transform::Year => Some(days_to_year(days)),
            Transform::Month => Some(days_to_month(days)),
            Transform::Day => Some(days),
            Transform::Hour => None, // Can't apply hour to date
            Transform::Void => None,
            Transform::Bucket(n) => {
                let hash = murmur3_x86_32(&days.to_le_bytes(), 0);
                Some(((hash as i64 & i64::MAX) % (*n as i64)) as i32)
            }
            Transform::Truncate(width) => {
                let w = *width;
                Some(days - (days % w).max(0))
            }
        }
    }

    /// Apply the transform to an integer value
    pub fn apply_to_int(&self, value: i32) -> Option<i32> {
        match self {
            Transform::Identity => Some(value),
            Transform::Void => None,
            Transform::Bucket(n) => {
                let hash = murmur3_x86_32(&value.to_le_bytes(), 0);
                Some(((hash as i64 & i64::MAX) % (*n as i64)) as i32)
            }
            Transform::Truncate(width) => {
                let w = *width;
                if value >= 0 {
                    Some(value - (value % w))
                } else {
                    Some(value - (value % w) - w)
                }
            }
            _ => None, // Temporal transforms don't apply to integers
        }
    }

    /// Apply the transform to a long value
    pub fn apply_to_long(&self, value: i64) -> Option<i64> {
        match self {
            Transform::Identity => Some(value),
            Transform::Void => None,
            Transform::Bucket(n) => {
                let hash = murmur3_x86_32(&value.to_le_bytes(), 0);
                Some((hash as i64 & i64::MAX) % (*n as i64))
            }
            Transform::Truncate(width) => {
                let w = *width as i64;
                if value >= 0 {
                    Some(value - (value % w))
                } else {
                    Some(value - (value % w) - w)
                }
            }
            _ => None, // Temporal transforms need specific handling
        }
    }

    /// Apply the transform to a string value
    pub fn apply_to_string(&self, value: &str) -> Option<String> {
        match self {
            Transform::Identity => Some(value.to_string()),
            Transform::Void => None,
            Transform::Bucket(n) => {
                let hash = murmur3_x86_32(value.as_bytes(), 0);
                Some(((hash as i64 & i64::MAX) % (*n as i64)).to_string())
            }
            Transform::Truncate(width) => {
                let w = *width as usize;
                if value.len() <= w {
                    Some(value.to_string())
                } else {
                    Some(value.chars().take(w).collect())
                }
            }
            _ => None,
        }
    }
}

/// Convert days since epoch to year (years since 1970)
fn days_to_year(days: i32) -> i32 {
    // Simplified calculation - for accurate results use proper date library
    // This is an approximation: ~365.25 days per year
    let total_days = days as i64 + EPOCH_DAY;
    let year = (total_days * 400 / 146097) as i32;
    // Adjust back to years since 1970
    year - 1970
}

/// Convert days since epoch to month (months since 1970-01)
fn days_to_month(days: i32) -> i32 {
    // Simplified calculation
    let total_days = days as i64 + EPOCH_DAY;
    let year = (total_days * 400 / 146097) as i32;
    
    // Estimate month within year
    let year_start_days = year_to_days(year);
    let day_of_year = (total_days - year_start_days as i64) as i32;
    let month_of_year = estimate_month(day_of_year, is_leap_year(year));
    
    // Total months since 1970-01
    (year - 1970) * 12 + month_of_year
}

/// Convert year to days since year 0
fn year_to_days(year: i32) -> i32 {
    let y = year - 1;
    let days = 365 * y + y / 4 - y / 100 + y / 400;
    days
}

/// Check if year is a leap year
fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Estimate month from day of year (0-based month)
fn estimate_month(day_of_year: i32, is_leap: bool) -> i32 {
    let days_per_month: [i32; 12] = if is_leap {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    
    let mut remaining = day_of_year;
    for (i, days) in days_per_month.iter().enumerate() {
        if remaining < *days {
            return i as i32;
        }
        remaining -= days;
    }
    11 // December
}

/// Simplified Murmur3 x86 32-bit hash (for bucket transform)
fn murmur3_x86_32(data: &[u8], seed: u32) -> u32 {
    const C1: u32 = 0xcc9e2d51;
    const C2: u32 = 0x1b873593;
    const R1: u32 = 15;
    const R2: u32 = 13;
    const M: u32 = 5;
    const N: u32 = 0xe6546b64;

    let mut hash = seed;
    let len = data.len();
    let n_blocks = len / 4;

    // Process 4-byte blocks
    for i in 0..n_blocks {
        let offset = i * 4;
        let k = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);

        let mut k = k.wrapping_mul(C1);
        k = k.rotate_left(R1);
        k = k.wrapping_mul(C2);

        hash ^= k;
        hash = hash.rotate_left(R2);
        hash = hash.wrapping_mul(M).wrapping_add(N);
    }

    // Process remaining bytes
    let tail = &data[n_blocks * 4..];
    let mut k1: u32 = 0;
    
    match tail.len() {
        3 => {
            k1 ^= (tail[2] as u32) << 16;
            k1 ^= (tail[1] as u32) << 8;
            k1 ^= tail[0] as u32;
            k1 = k1.wrapping_mul(C1);
            k1 = k1.rotate_left(R1);
            k1 = k1.wrapping_mul(C2);
            hash ^= k1;
        }
        2 => {
            k1 ^= (tail[1] as u32) << 8;
            k1 ^= tail[0] as u32;
            k1 = k1.wrapping_mul(C1);
            k1 = k1.rotate_left(R1);
            k1 = k1.wrapping_mul(C2);
            hash ^= k1;
        }
        1 => {
            k1 ^= tail[0] as u32;
            k1 = k1.wrapping_mul(C1);
            k1 = k1.rotate_left(R1);
            k1 = k1.wrapping_mul(C2);
            hash ^= k1;
        }
        _ => {}
    }

    // Finalization
    hash ^= len as u32;
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85ebca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2ae35);
    hash ^= hash >> 16;

    hash
}

/// A partition field
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionField {
    /// Source column ID
    #[serde(rename = "source-id")]
    pub source_id: i32,
    /// Partition field ID
    #[serde(rename = "field-id")]
    pub field_id: i32,
    /// Partition field name
    pub name: String,
    /// Transform to apply
    pub transform: String,
}

impl PartitionField {
    /// Create a new partition field
    pub fn new(source_id: i32, field_id: i32, name: impl Into<String>, transform: impl Into<String>) -> Self {
        Self {
            source_id,
            field_id,
            name: name.into(),
            transform: transform.into(),
        }
    }

    /// Parse the transform
    pub fn parsed_transform(&self) -> Option<Transform> {
        Transform::from_str(&self.transform)
    }

    /// Check if this is an identity transform
    pub fn is_identity(&self) -> bool {
        self.transform == "identity"
    }

    /// Check if this is a void transform (hidden partition)
    pub fn is_void(&self) -> bool {
        self.transform == "void"
    }
}

/// A partition spec
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionSpec {
    /// Partition spec ID
    #[serde(rename = "spec-id")]
    pub spec_id: i32,
    /// Partition fields
    pub fields: Vec<PartitionField>,
}

impl PartitionSpec {
    /// Create a new partition spec
    pub fn new(spec_id: i32, fields: Vec<PartitionField>) -> Self {
        Self { spec_id, fields }
    }

    /// Create an unpartitioned spec (no partition fields)
    pub fn unpartitioned() -> Self {
        Self {
            spec_id: 0,
            fields: Vec::new(),
        }
    }

    /// Check if this spec has no partition fields
    pub fn is_unpartitioned(&self) -> bool {
        self.fields.is_empty() || self.fields.iter().all(|f| f.is_void())
    }

    /// Get partition field by ID
    pub fn field(&self, field_id: i32) -> Option<&PartitionField> {
        self.fields.iter().find(|f| f.field_id == field_id)
    }

    /// Get partition field by name
    pub fn field_by_name(&self, name: &str) -> Option<&PartitionField> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Get fields by source ID
    pub fn fields_by_source_id(&self, source_id: i32) -> Vec<&PartitionField> {
        self.fields.iter().filter(|f| f.source_id == source_id).collect()
    }
}

impl Default for PartitionSpec {
    fn default() -> Self {
        Self::unpartitioned()
    }
}

/// Sort direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SortDirection {
    Asc,
    Desc,
}

impl Default for SortDirection {
    fn default() -> Self {
        SortDirection::Asc
    }
}

/// Null ordering
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NullOrder {
    NullsFirst,
    NullsLast,
}

impl Default for NullOrder {
    fn default() -> Self {
        NullOrder::NullsFirst
    }
}

/// A sort field
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortField {
    /// Source column ID
    #[serde(rename = "source-id")]
    pub source_id: i32,
    /// Transform to apply before sorting
    pub transform: String,
    /// Sort direction
    pub direction: SortDirection,
    /// Null ordering
    #[serde(rename = "null-order")]
    pub null_order: NullOrder,
}

impl SortField {
    /// Create a new sort field
    pub fn new(source_id: i32, direction: SortDirection, null_order: NullOrder) -> Self {
        Self {
            source_id,
            transform: "identity".to_string(),
            direction,
            null_order,
        }
    }

    /// Create a new ascending sort field with nulls first
    pub fn asc(source_id: i32) -> Self {
        Self::new(source_id, SortDirection::Asc, NullOrder::NullsFirst)
    }

    /// Create a new descending sort field with nulls last
    pub fn desc(source_id: i32) -> Self {
        Self::new(source_id, SortDirection::Desc, NullOrder::NullsLast)
    }

    /// Add a transform
    pub fn with_transform(mut self, transform: impl Into<String>) -> Self {
        self.transform = transform.into();
        self
    }
}

/// A sort order
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortOrder {
    /// Sort order ID
    #[serde(rename = "order-id")]
    pub order_id: i32,
    /// Sort fields
    pub fields: Vec<SortField>,
}

impl SortOrder {
    /// Create a new sort order
    pub fn new(order_id: i32, fields: Vec<SortField>) -> Self {
        Self { order_id, fields }
    }

    /// Create an unsorted order
    pub fn unsorted() -> Self {
        Self {
            order_id: 0,
            fields: Vec::new(),
        }
    }

    /// Check if this is unsorted
    pub fn is_unsorted(&self) -> bool {
        self.fields.is_empty()
    }
}

impl Default for SortOrder {
    fn default() -> Self {
        Self::unsorted()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_partition_spec() {
        let json = r#"
        {
            "spec-id": 0,
            "fields": [
                {"source-id": 4, "field-id": 1000, "name": "date", "transform": "day"},
                {"source-id": 1, "field-id": 1001, "name": "id_bucket", "transform": "bucket[16]"}
            ]
        }
        "#;

        let spec: PartitionSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.spec_id, 0);
        assert_eq!(spec.fields.len(), 2);
        assert!(!spec.is_unpartitioned());

        let date_field = &spec.fields[0];
        assert_eq!(date_field.source_id, 4);
        assert_eq!(date_field.transform, "day");
        assert_eq!(date_field.parsed_transform(), Some(Transform::Day));

        let bucket_field = &spec.fields[1];
        assert_eq!(bucket_field.parsed_transform(), Some(Transform::Bucket(16)));
    }

    #[test]
    fn test_unpartitioned_spec() {
        let spec = PartitionSpec::unpartitioned();
        assert!(spec.is_unpartitioned());
    }

    #[test]
    fn test_parse_sort_order() {
        let json = r#"
        {
            "order-id": 1,
            "fields": [
                {"source-id": 1, "transform": "identity", "direction": "asc", "null-order": "nulls-first"},
                {"source-id": 2, "transform": "identity", "direction": "desc", "null-order": "nulls-last"}
            ]
        }
        "#;

        let order: SortOrder = serde_json::from_str(json).unwrap();
        assert_eq!(order.order_id, 1);
        assert_eq!(order.fields.len(), 2);
        assert!(!order.is_unsorted());

        assert_eq!(order.fields[0].direction, SortDirection::Asc);
        assert_eq!(order.fields[0].null_order, NullOrder::NullsFirst);
        assert_eq!(order.fields[1].direction, SortDirection::Desc);
        assert_eq!(order.fields[1].null_order, NullOrder::NullsLast);
    }

    #[test]
    fn test_unsorted_order() {
        let order = SortOrder::unsorted();
        assert!(order.is_unsorted());
    }

    #[test]
    fn test_transform_parsing() {
        assert_eq!(Transform::from_str("identity"), Some(Transform::Identity));
        assert_eq!(Transform::from_str("year"), Some(Transform::Year));
        assert_eq!(Transform::from_str("month"), Some(Transform::Month));
        assert_eq!(Transform::from_str("day"), Some(Transform::Day));
        assert_eq!(Transform::from_str("hour"), Some(Transform::Hour));
        assert_eq!(Transform::from_str("void"), Some(Transform::Void));
        assert_eq!(Transform::from_str("bucket[16]"), Some(Transform::Bucket(16)));
        assert_eq!(Transform::from_str("truncate[10]"), Some(Transform::Truncate(10)));
        assert_eq!(Transform::from_str("unknown"), None);
    }

    #[test]
    fn test_transform_is_temporal() {
        assert!(!Transform::Identity.is_temporal());
        assert!(Transform::Year.is_temporal());
        assert!(Transform::Month.is_temporal());
        assert!(Transform::Day.is_temporal());
        assert!(Transform::Hour.is_temporal());
        assert!(!Transform::Bucket(16).is_temporal());
        assert!(!Transform::Truncate(10).is_temporal());
        assert!(!Transform::Void.is_temporal());
    }

    #[test]
    fn test_transform_preserves_order() {
        assert!(Transform::Identity.preserves_order());
        assert!(Transform::Year.preserves_order());
        assert!(Transform::Month.preserves_order());
        assert!(Transform::Day.preserves_order());
        assert!(Transform::Hour.preserves_order());
        assert!(!Transform::Bucket(16).preserves_order());
        assert!(Transform::Truncate(10).preserves_order());
        assert!(!Transform::Void.preserves_order());
    }

    #[test]
    fn test_transform_satisfies() {
        // Same transform satisfies itself
        assert!(Transform::Identity.satisfies(&Transform::Identity));
        assert!(Transform::Year.satisfies(&Transform::Year));
        
        // More specific temporal satisfies less specific
        assert!(Transform::Hour.satisfies(&Transform::Day));
        assert!(Transform::Hour.satisfies(&Transform::Month));
        assert!(Transform::Hour.satisfies(&Transform::Year));
        assert!(Transform::Day.satisfies(&Transform::Month));
        assert!(Transform::Day.satisfies(&Transform::Year));
        assert!(Transform::Month.satisfies(&Transform::Year));
        
        // Less specific doesn't satisfy more specific
        assert!(!Transform::Year.satisfies(&Transform::Month));
        assert!(!Transform::Year.satisfies(&Transform::Day));
        assert!(!Transform::Month.satisfies(&Transform::Day));
    }

    #[test]
    fn test_transform_apply_to_int() {
        // Identity
        assert_eq!(Transform::Identity.apply_to_int(42), Some(42));
        
        // Truncate
        assert_eq!(Transform::Truncate(10).apply_to_int(42), Some(40));
        assert_eq!(Transform::Truncate(10).apply_to_int(45), Some(40));
        assert_eq!(Transform::Truncate(10).apply_to_int(-5), Some(-10));
        
        // Void
        assert_eq!(Transform::Void.apply_to_int(42), None);
    }

    #[test]
    fn test_transform_apply_to_string() {
        // Identity
        assert_eq!(
            Transform::Identity.apply_to_string("hello"),
            Some("hello".to_string())
        );
        
        // Truncate
        assert_eq!(
            Transform::Truncate(3).apply_to_string("hello"),
            Some("hel".to_string())
        );
        assert_eq!(
            Transform::Truncate(10).apply_to_string("hi"),
            Some("hi".to_string())
        );
        
        // Void
        assert_eq!(Transform::Void.apply_to_string("hello"), None);
    }

    #[test]
    fn test_murmur3_hash() {
        // Test that the hash produces consistent results
        let hash1 = super::murmur3_x86_32(b"hello", 0);
        let hash2 = super::murmur3_x86_32(b"hello", 0);
        assert_eq!(hash1, hash2);
        
        // Different inputs produce different hashes
        let hash3 = super::murmur3_x86_32(b"world", 0);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_bucket_transform() {
        let transform = Transform::Bucket(16);
        
        // Bucket should produce values in range [0, n)
        for i in 0..100 {
            let result = transform.apply_to_int(i).unwrap();
            assert!(result >= 0 && result < 16);
        }
    }
}
