//! Literal - constant values in expressions
//!
//! A Literal wraps a Datum and provides additional functionality for
//! expression evaluation, including type conversion and comparison.

use std::cmp::Ordering;
use std::fmt;
use super::datum::Datum;

/// A literal constant value in an expression
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Literal {
    /// The underlying datum value
    value: Datum,
}

impl Literal {
    /// Create a new literal from a datum
    pub fn new(value: Datum) -> Self {
        Literal { value }
    }
    
    // ============ Constructors ============
    
    /// Create a null literal
    pub fn null() -> Self {
        Literal::new(Datum::Null)
    }
    
    /// Create a boolean literal
    pub fn boolean(value: bool) -> Self {
        Literal::new(Datum::Boolean(value))
    }
    
    /// Create an integer literal
    pub fn int(value: i32) -> Self {
        Literal::new(Datum::Int(value))
    }
    
    /// Create a long literal
    pub fn long(value: i64) -> Self {
        Literal::new(Datum::Long(value))
    }
    
    /// Create a float literal
    pub fn float(value: f32) -> Self {
        Literal::new(Datum::Float(value))
    }
    
    /// Create a double literal
    pub fn double(value: f64) -> Self {
        Literal::new(Datum::Double(value))
    }
    
    /// Create a date literal (days from epoch)
    pub fn date(days: i32) -> Self {
        Literal::new(Datum::Date(days))
    }
    
    /// Create a time literal (microseconds from midnight)
    pub fn time(micros: i64) -> Self {
        Literal::new(Datum::Time(micros))
    }
    
    /// Create a timestamp literal (microseconds from epoch)
    pub fn timestamp(micros: i64) -> Self {
        Literal::new(Datum::Timestamp(micros))
    }
    
    /// Create a timestamp with timezone literal
    pub fn timestamp_tz(micros: i64) -> Self {
        Literal::new(Datum::TimestampTz(micros))
    }
    
    /// Create a string literal
    pub fn string(value: impl Into<String>) -> Self {
        Literal::new(Datum::String(value.into()))
    }
    
    /// Create a UUID literal
    pub fn uuid(bytes: [u8; 16]) -> Self {
        Literal::new(Datum::Uuid(bytes))
    }
    
    /// Create a binary literal
    pub fn binary(value: Vec<u8>) -> Self {
        Literal::new(Datum::Binary(value))
    }
    
    /// Create a fixed-length binary literal
    pub fn fixed(value: Vec<u8>) -> Self {
        Literal::new(Datum::Fixed(value))
    }
    
    /// Create a decimal literal
    pub fn decimal(value: Vec<u8>, precision: u8, scale: u8) -> Self {
        Literal::new(Datum::Decimal { value, precision, scale })
    }
    
    // ============ Value access ============
    
    /// Get the underlying datum
    pub fn value(&self) -> &Datum {
        &self.value
    }
    
    /// Get the underlying datum (owned)
    pub fn into_value(self) -> Datum {
        self.value
    }
    
    /// Check if this literal is null
    pub fn is_null(&self) -> bool {
        self.value.is_null()
    }
    
    /// Check if this literal is NaN
    pub fn is_nan(&self) -> bool {
        self.value.is_nan()
    }
    
    // ============ Comparison ============
    
    /// Compare this literal with a datum
    pub fn compare(&self, other: &Datum) -> Option<Ordering> {
        self.value.compare(other)
    }
    
    /// Compare two literals
    pub fn compare_literal(&self, other: &Literal) -> Option<Ordering> {
        self.value.compare(&other.value)
    }
    
    // ============ Type conversion ============
    
    /// Convert this literal to a different type
    /// Returns None if conversion is not possible
    pub fn to_type(&self, target_type: &str) -> Option<Literal> {
        match (target_type, &self.value) {
            // Boolean conversions
            ("boolean", Datum::Boolean(_)) => Some(self.clone()),
            
            // Integer conversions
            ("int", Datum::Int(_)) => Some(self.clone()),
            ("int", Datum::Long(v)) => {
                if *v >= i32::MIN as i64 && *v <= i32::MAX as i64 {
                    Some(Literal::int(*v as i32))
                } else {
                    None
                }
            }
            
            // Long conversions
            ("long", Datum::Long(_)) => Some(self.clone()),
            ("long", Datum::Int(v)) => Some(Literal::long(*v as i64)),
            
            // Float conversions
            ("float", Datum::Float(_)) => Some(self.clone()),
            ("float", Datum::Int(v)) => Some(Literal::float(*v as f32)),
            ("float", Datum::Long(v)) => Some(Literal::float(*v as f32)),
            
            // Double conversions
            ("double", Datum::Double(_)) => Some(self.clone()),
            ("double", Datum::Float(v)) => Some(Literal::double(*v as f64)),
            ("double", Datum::Int(v)) => Some(Literal::double(*v as f64)),
            ("double", Datum::Long(v)) => Some(Literal::double(*v as f64)),
            
            // Date conversions
            ("date", Datum::Date(_)) => Some(self.clone()),
            
            // Time conversions
            ("time", Datum::Time(_)) => Some(self.clone()),
            
            // Timestamp conversions
            ("timestamp", Datum::Timestamp(_)) => Some(self.clone()),
            ("timestamp", Datum::TimestampTz(v)) => Some(Literal::timestamp(*v)),
            
            // TimestampTz conversions
            ("timestamptz", Datum::TimestampTz(_)) => Some(self.clone()),
            ("timestamptz", Datum::Timestamp(v)) => Some(Literal::timestamp_tz(*v)),
            
            // String conversions
            ("string", Datum::String(_)) => Some(self.clone()),
            
            // UUID conversions
            ("uuid", Datum::Uuid(_)) => Some(self.clone()),
            
            // Binary conversions
            ("binary", Datum::Binary(_)) => Some(self.clone()),
            ("binary", Datum::Fixed(v)) => Some(Literal::binary(v.clone())),
            
            // Fixed conversions
            ("fixed", Datum::Fixed(_)) => Some(self.clone()),
            
            // Decimal conversions (simplified)
            ("decimal", Datum::Decimal { .. }) => Some(self.clone()),
            
            _ => None,
        }
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

// ============ From implementations ============

impl From<bool> for Literal {
    fn from(v: bool) -> Self {
        Literal::boolean(v)
    }
}

impl From<i32> for Literal {
    fn from(v: i32) -> Self {
        Literal::int(v)
    }
}

impl From<i64> for Literal {
    fn from(v: i64) -> Self {
        Literal::long(v)
    }
}

impl From<f32> for Literal {
    fn from(v: f32) -> Self {
        Literal::float(v)
    }
}

impl From<f64> for Literal {
    fn from(v: f64) -> Self {
        Literal::double(v)
    }
}

impl From<String> for Literal {
    fn from(v: String) -> Self {
        Literal::string(v)
    }
}

impl From<&str> for Literal {
    fn from(v: &str) -> Self {
        Literal::string(v)
    }
}

impl From<Datum> for Literal {
    fn from(v: Datum) -> Self {
        Literal::new(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_literal_creation() {
        let lit = Literal::int(42);
        assert_eq!(lit.value(), &Datum::Int(42));
        
        let lit = Literal::string("hello");
        assert_eq!(lit.value(), &Datum::String("hello".to_string()));
    }
    
    #[test]
    fn test_literal_comparison() {
        let lit1 = Literal::int(1);
        let lit2 = Literal::int(2);
        
        assert_eq!(lit1.compare_literal(&lit2), Some(Ordering::Less));
        assert_eq!(lit2.compare_literal(&lit1), Some(Ordering::Greater));
        assert_eq!(lit1.compare_literal(&lit1), Some(Ordering::Equal));
    }
    
    #[test]
    fn test_type_conversion() {
        // Int to Long
        let lit = Literal::int(42);
        let converted = lit.to_type("long").unwrap();
        assert_eq!(converted.value(), &Datum::Long(42));
        
        // Long to Int (in range)
        let lit = Literal::long(100);
        let converted = lit.to_type("int").unwrap();
        assert_eq!(converted.value(), &Datum::Int(100));
        
        // Long to Int (out of range)
        let lit = Literal::long(i64::MAX);
        assert!(lit.to_type("int").is_none());
        
        // Float to Double
        let lit = Literal::float(1.5);
        let converted = lit.to_type("double").unwrap();
        match converted.value() {
            Datum::Double(v) => assert!((v - 1.5).abs() < 0.001),
            _ => panic!("Expected Double"),
        }
    }
    
    #[test]
    fn test_literal_from() {
        let lit: Literal = 42i32.into();
        assert_eq!(lit.value(), &Datum::Int(42));
        
        let lit: Literal = "hello".into();
        assert_eq!(lit.value(), &Datum::String("hello".to_string()));
    }
    
    #[test]
    fn test_literal_display() {
        assert_eq!(format!("{}", Literal::int(42)), "42");
        assert_eq!(format!("{}", Literal::string("test")), "\"test\"");
    }
}
