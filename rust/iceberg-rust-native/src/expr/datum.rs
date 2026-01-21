//! Datum - typed values in Iceberg expressions
//!
//! A Datum represents a typed value that can be used in expressions.
//! It supports all Iceberg primitive types.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

/// Represents a typed value in Iceberg
#[derive(Debug, Clone)]
pub enum Datum {
    /// Null value
    Null,
    
    /// Boolean value
    Boolean(bool),
    
    /// 32-bit signed integer
    Int(i32),
    
    /// 64-bit signed integer
    Long(i64),
    
    /// 32-bit IEEE 754 floating point
    Float(f32),
    
    /// 64-bit IEEE 754 floating point
    Double(f64),
    
    /// Date (days from 1970-01-01)
    Date(i32),
    
    /// Time (microseconds from midnight)
    Time(i64),
    
    /// Timestamp without timezone (microseconds from 1970-01-01 00:00:00)
    Timestamp(i64),
    
    /// Timestamp with timezone (microseconds from 1970-01-01 00:00:00 UTC)
    TimestampTz(i64),
    
    /// Timestamp (nanoseconds from 1970-01-01 00:00:00)
    TimestampNanos(i64),
    
    /// Timestamp with timezone (nanoseconds)
    TimestampTzNanos(i64),
    
    /// UTF-8 string
    String(String),
    
    /// UUID
    Uuid([u8; 16]),
    
    /// Fixed-length byte array
    Fixed(Vec<u8>),
    
    /// Variable-length byte array
    Binary(Vec<u8>),
    
    /// Decimal with precision and scale
    Decimal {
        /// Unscaled value as bytes (big-endian two's complement)
        value: Vec<u8>,
        /// Precision
        precision: u8,
        /// Scale
        scale: u8,
    },
}

impl Datum {
    // ============ Constructors ============
    
    /// Create a null datum
    pub fn null() -> Self {
        Datum::Null
    }
    
    /// Create a boolean datum
    pub fn boolean(value: bool) -> Self {
        Datum::Boolean(value)
    }
    
    /// Create an integer datum
    pub fn int(value: i32) -> Self {
        Datum::Int(value)
    }
    
    /// Create a long datum
    pub fn long(value: i64) -> Self {
        Datum::Long(value)
    }
    
    /// Create a float datum
    pub fn float(value: f32) -> Self {
        Datum::Float(value)
    }
    
    /// Create a double datum
    pub fn double(value: f64) -> Self {
        Datum::Double(value)
    }
    
    /// Create a date datum (days from epoch)
    pub fn date(days: i32) -> Self {
        Datum::Date(days)
    }
    
    /// Create a time datum (microseconds from midnight)
    pub fn time(micros: i64) -> Self {
        Datum::Time(micros)
    }
    
    /// Create a timestamp datum (microseconds from epoch)
    pub fn timestamp(micros: i64) -> Self {
        Datum::Timestamp(micros)
    }
    
    /// Create a timestamp with timezone datum
    pub fn timestamp_tz(micros: i64) -> Self {
        Datum::TimestampTz(micros)
    }
    
    /// Create a string datum
    pub fn string(value: impl Into<String>) -> Self {
        Datum::String(value.into())
    }
    
    /// Create a UUID datum
    pub fn uuid(bytes: [u8; 16]) -> Self {
        Datum::Uuid(bytes)
    }
    
    /// Create a UUID from string
    pub fn uuid_from_str(s: &str) -> Option<Self> {
        // Parse UUID string like "550e8400-e29b-41d4-a716-446655440000"
        let s = s.replace('-', "");
        if s.len() != 32 {
            return None;
        }
        let mut bytes = [0u8; 16];
        for i in 0..16 {
            bytes[i] = u8::from_str_radix(&s[i*2..i*2+2], 16).ok()?;
        }
        Some(Datum::Uuid(bytes))
    }
    
    /// Create a fixed-length binary datum
    pub fn fixed(value: Vec<u8>) -> Self {
        Datum::Fixed(value)
    }
    
    /// Create a variable-length binary datum
    pub fn binary(value: Vec<u8>) -> Self {
        Datum::Binary(value)
    }
    
    /// Create a decimal datum
    pub fn decimal(value: Vec<u8>, precision: u8, scale: u8) -> Self {
        Datum::Decimal { value, precision, scale }
    }
    
    // ============ Type checks ============
    
    /// Check if this datum is null
    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }
    
    /// Check if this datum is NaN
    pub fn is_nan(&self) -> bool {
        match self {
            Datum::Float(f) => f.is_nan(),
            Datum::Double(d) => d.is_nan(),
            _ => false,
        }
    }
    
    /// Get type name
    pub fn type_name(&self) -> &'static str {
        match self {
            Datum::Null => "null",
            Datum::Boolean(_) => "boolean",
            Datum::Int(_) => "int",
            Datum::Long(_) => "long",
            Datum::Float(_) => "float",
            Datum::Double(_) => "double",
            Datum::Date(_) => "date",
            Datum::Time(_) => "time",
            Datum::Timestamp(_) => "timestamp",
            Datum::TimestampTz(_) => "timestamptz",
            Datum::TimestampNanos(_) => "timestamp_ns",
            Datum::TimestampTzNanos(_) => "timestamptz_ns",
            Datum::String(_) => "string",
            Datum::Uuid(_) => "uuid",
            Datum::Fixed(_) => "fixed",
            Datum::Binary(_) => "binary",
            Datum::Decimal { .. } => "decimal",
        }
    }
    
    // ============ Value getters ============
    
    /// Get as boolean
    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Datum::Boolean(v) => Some(*v),
            _ => None,
        }
    }
    
    /// Get as i32
    pub fn as_int(&self) -> Option<i32> {
        match self {
            Datum::Int(v) => Some(*v),
            _ => None,
        }
    }
    
    /// Get as i64
    pub fn as_long(&self) -> Option<i64> {
        match self {
            Datum::Long(v) => Some(*v),
            Datum::Int(v) => Some(*v as i64),
            _ => None,
        }
    }
    
    /// Get as f32
    pub fn as_float(&self) -> Option<f32> {
        match self {
            Datum::Float(v) => Some(*v),
            _ => None,
        }
    }
    
    /// Get as f64
    pub fn as_double(&self) -> Option<f64> {
        match self {
            Datum::Double(v) => Some(*v),
            Datum::Float(v) => Some(*v as f64),
            _ => None,
        }
    }
    
    /// Get as string reference
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Datum::String(v) => Some(v.as_str()),
            _ => None,
        }
    }
    
    /// Get as bytes reference
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Datum::Binary(v) => Some(v.as_slice()),
            Datum::Fixed(v) => Some(v.as_slice()),
            _ => None,
        }
    }
    
    // ============ Comparison ============
    
    /// Compare two datums, returns None if types are incompatible or either is null/NaN
    pub fn compare(&self, other: &Datum) -> Option<Ordering> {
        // Null handling: null compares as less than any non-null value
        match (self, other) {
            (Datum::Null, Datum::Null) => return Some(Ordering::Equal),
            (Datum::Null, _) => return Some(Ordering::Less),
            (_, Datum::Null) => return Some(Ordering::Greater),
            _ => {}
        }
        
        // NaN handling: NaN is not comparable
        if self.is_nan() || other.is_nan() {
            return None;
        }
        
        match (self, other) {
            (Datum::Boolean(a), Datum::Boolean(b)) => Some(a.cmp(b)),
            
            (Datum::Int(a), Datum::Int(b)) => Some(a.cmp(b)),
            (Datum::Int(a), Datum::Long(b)) => Some((*a as i64).cmp(b)),
            (Datum::Long(a), Datum::Int(b)) => Some(a.cmp(&(*b as i64))),
            (Datum::Long(a), Datum::Long(b)) => Some(a.cmp(b)),
            
            (Datum::Float(a), Datum::Float(b)) => a.partial_cmp(b),
            (Datum::Float(a), Datum::Double(b)) => (*a as f64).partial_cmp(b),
            (Datum::Double(a), Datum::Float(b)) => a.partial_cmp(&(*b as f64)),
            (Datum::Double(a), Datum::Double(b)) => a.partial_cmp(b),
            
            (Datum::Date(a), Datum::Date(b)) => Some(a.cmp(b)),
            (Datum::Time(a), Datum::Time(b)) => Some(a.cmp(b)),
            (Datum::Timestamp(a), Datum::Timestamp(b)) => Some(a.cmp(b)),
            (Datum::TimestampTz(a), Datum::TimestampTz(b)) => Some(a.cmp(b)),
            (Datum::TimestampNanos(a), Datum::TimestampNanos(b)) => Some(a.cmp(b)),
            (Datum::TimestampTzNanos(a), Datum::TimestampTzNanos(b)) => Some(a.cmp(b)),
            
            (Datum::String(a), Datum::String(b)) => Some(a.cmp(b)),
            (Datum::Uuid(a), Datum::Uuid(b)) => Some(a.cmp(b)),
            
            (Datum::Binary(a), Datum::Binary(b)) => Some(a.cmp(b)),
            (Datum::Fixed(a), Datum::Fixed(b)) => Some(a.cmp(b)),
            (Datum::Binary(a), Datum::Fixed(b)) => Some(a.as_slice().cmp(b.as_slice())),
            (Datum::Fixed(a), Datum::Binary(b)) => Some(a.as_slice().cmp(b.as_slice())),
            
            // Decimal comparison (simplified - assumes same precision/scale)
            (Datum::Decimal { value: a, .. }, Datum::Decimal { value: b, .. }) => {
                Some(a.cmp(b))
            }
            
            _ => None,
        }
    }
    
    /// Check if two datums are equal
    pub fn equals(&self, other: &Datum) -> bool {
        self.compare(other) == Some(Ordering::Equal)
    }
}

impl PartialEq for Datum {
    fn eq(&self, other: &Self) -> bool {
        self.equals(other)
    }
}

impl Eq for Datum {}

impl Hash for Datum {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Datum::Null => {}
            Datum::Boolean(v) => v.hash(state),
            Datum::Int(v) => v.hash(state),
            Datum::Long(v) => v.hash(state),
            Datum::Float(v) => v.to_bits().hash(state),
            Datum::Double(v) => v.to_bits().hash(state),
            Datum::Date(v) => v.hash(state),
            Datum::Time(v) => v.hash(state),
            Datum::Timestamp(v) => v.hash(state),
            Datum::TimestampTz(v) => v.hash(state),
            Datum::TimestampNanos(v) => v.hash(state),
            Datum::TimestampTzNanos(v) => v.hash(state),
            Datum::String(v) => v.hash(state),
            Datum::Uuid(v) => v.hash(state),
            Datum::Fixed(v) => v.hash(state),
            Datum::Binary(v) => v.hash(state),
            Datum::Decimal { value, precision, scale } => {
                value.hash(state);
                precision.hash(state);
                scale.hash(state);
            }
        }
    }
}

impl fmt::Display for Datum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Datum::Null => write!(f, "null"),
            Datum::Boolean(v) => write!(f, "{}", v),
            Datum::Int(v) => write!(f, "{}", v),
            Datum::Long(v) => write!(f, "{}", v),
            Datum::Float(v) => write!(f, "{}", v),
            Datum::Double(v) => write!(f, "{}", v),
            Datum::Date(v) => write!(f, "date({})", v),
            Datum::Time(v) => write!(f, "time({})", v),
            Datum::Timestamp(v) => write!(f, "timestamp({})", v),
            Datum::TimestampTz(v) => write!(f, "timestamptz({})", v),
            Datum::TimestampNanos(v) => write!(f, "timestamp_ns({})", v),
            Datum::TimestampTzNanos(v) => write!(f, "timestamptz_ns({})", v),
            Datum::String(v) => write!(f, "\"{}\"", v),
            Datum::Uuid(v) => {
                // Format as UUID string
                write!(f, "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                    v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
                    v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15])
            }
            Datum::Fixed(v) => write!(f, "fixed({:?})", v),
            Datum::Binary(v) => write!(f, "binary({:?})", v),
            Datum::Decimal { value, precision, scale } => {
                write!(f, "decimal({:?}, {}, {})", value, precision, scale)
            }
        }
    }
}

// ============ From implementations ============

impl From<bool> for Datum {
    fn from(v: bool) -> Self {
        Datum::Boolean(v)
    }
}

impl From<i32> for Datum {
    fn from(v: i32) -> Self {
        Datum::Int(v)
    }
}

impl From<i64> for Datum {
    fn from(v: i64) -> Self {
        Datum::Long(v)
    }
}

impl From<f32> for Datum {
    fn from(v: f32) -> Self {
        Datum::Float(v)
    }
}

impl From<f64> for Datum {
    fn from(v: f64) -> Self {
        Datum::Double(v)
    }
}

impl From<String> for Datum {
    fn from(v: String) -> Self {
        Datum::String(v)
    }
}

impl From<&str> for Datum {
    fn from(v: &str) -> Self {
        Datum::String(v.to_string())
    }
}

impl From<Vec<u8>> for Datum {
    fn from(v: Vec<u8>) -> Self {
        Datum::Binary(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_datum_creation() {
        assert!(Datum::null().is_null());
        assert!(!Datum::boolean(true).is_null());
        assert_eq!(Datum::int(42).as_int(), Some(42));
        assert_eq!(Datum::long(100).as_long(), Some(100));
        assert_eq!(Datum::string("hello").as_str(), Some("hello"));
    }
    
    #[test]
    fn test_datum_comparison() {
        assert_eq!(Datum::int(1).compare(&Datum::int(2)), Some(Ordering::Less));
        assert_eq!(Datum::int(2).compare(&Datum::int(1)), Some(Ordering::Greater));
        assert_eq!(Datum::int(1).compare(&Datum::int(1)), Some(Ordering::Equal));
        
        // Cross-type comparison
        assert_eq!(Datum::int(1).compare(&Datum::long(1)), Some(Ordering::Equal));
        assert_eq!(Datum::float(1.0).compare(&Datum::double(1.0)), Some(Ordering::Equal));
        
        // Null comparison
        assert_eq!(Datum::null().compare(&Datum::null()), Some(Ordering::Equal));
        assert_eq!(Datum::null().compare(&Datum::int(1)), Some(Ordering::Less));
        assert_eq!(Datum::int(1).compare(&Datum::null()), Some(Ordering::Greater));
        
        // NaN comparison
        assert_eq!(Datum::float(f32::NAN).compare(&Datum::float(1.0)), None);
    }
    
    #[test]
    fn test_is_nan() {
        assert!(Datum::float(f32::NAN).is_nan());
        assert!(Datum::double(f64::NAN).is_nan());
        assert!(!Datum::float(1.0).is_nan());
        assert!(!Datum::int(1).is_nan());
    }
    
    #[test]
    fn test_uuid_from_str() {
        let uuid = Datum::uuid_from_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        match uuid {
            Datum::Uuid(bytes) => {
                assert_eq!(bytes[0], 0x55);
                assert_eq!(bytes[15], 0x00);
            }
            _ => panic!("Expected UUID"),
        }
    }
    
    #[test]
    fn test_datum_hash() {
        use std::collections::HashSet;
        
        let mut set = HashSet::new();
        set.insert(Datum::int(1));
        set.insert(Datum::int(2));
        set.insert(Datum::int(1)); // Duplicate
        
        assert_eq!(set.len(), 2);
    }
    
    #[test]
    fn test_datum_display() {
        assert_eq!(format!("{}", Datum::null()), "null");
        assert_eq!(format!("{}", Datum::int(42)), "42");
        assert_eq!(format!("{}", Datum::string("hello")), "\"hello\"");
    }
}
