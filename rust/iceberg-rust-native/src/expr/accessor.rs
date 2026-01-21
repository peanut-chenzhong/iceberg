//! Accessor - provides access to struct fields by position or name

use super::datum::Datum;

/// Provides access to fields in a struct-like data row
pub trait Accessor: Send + Sync {
    /// Get the value at the given field position
    fn get(&self, pos: usize) -> Datum;
    
    /// Get the number of fields
    fn size(&self) -> usize;
}

/// A simple struct-like row implementation
#[derive(Debug, Clone)]
pub struct StructAccessor {
    values: Vec<Datum>,
}

impl StructAccessor {
    /// Create a new struct accessor with the given values
    pub fn new(values: Vec<Datum>) -> Self {
        StructAccessor { values }
    }
    
    /// Create an empty struct accessor
    pub fn empty() -> Self {
        StructAccessor { values: Vec::new() }
    }
    
    /// Add a value to the struct
    pub fn with_value(mut self, value: Datum) -> Self {
        self.values.push(value);
        self
    }
    
    /// Set value at position
    pub fn set(&mut self, pos: usize, value: Datum) {
        if pos < self.values.len() {
            self.values[pos] = value;
        }
    }
}

impl Accessor for StructAccessor {
    fn get(&self, pos: usize) -> Datum {
        self.values.get(pos).cloned().unwrap_or(Datum::Null)
    }
    
    fn size(&self) -> usize {
        self.values.len()
    }
}

/// Accessor for partition data (HashMap-based)
#[derive(Debug, Clone)]
pub struct PartitionAccessor {
    /// Field ID to position mapping
    id_to_pos: rustc_hash::FxHashMap<i32, usize>,
    /// Values by position
    values: Vec<Datum>,
}

impl PartitionAccessor {
    /// Create a new partition accessor
    pub fn new() -> Self {
        PartitionAccessor {
            id_to_pos: rustc_hash::FxHashMap::default(),
            values: Vec::new(),
        }
    }
    
    /// Add a field with its ID and value
    pub fn with_field(mut self, field_id: i32, value: Datum) -> Self {
        let pos = self.values.len();
        self.id_to_pos.insert(field_id, pos);
        self.values.push(value);
        self
    }
    
    /// Get value by field ID
    pub fn get_by_id(&self, field_id: i32) -> Datum {
        self.id_to_pos
            .get(&field_id)
            .and_then(|pos| self.values.get(*pos))
            .cloned()
            .unwrap_or(Datum::Null)
    }
}

impl Default for PartitionAccessor {
    fn default() -> Self {
        Self::new()
    }
}

impl Accessor for PartitionAccessor {
    fn get(&self, pos: usize) -> Datum {
        self.values.get(pos).cloned().unwrap_or(Datum::Null)
    }
    
    fn size(&self) -> usize {
        self.values.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_struct_accessor() {
        let accessor = StructAccessor::new(vec![
            Datum::int(1),
            Datum::string("hello"),
            Datum::boolean(true),
        ]);
        
        assert_eq!(accessor.get(0), Datum::Int(1));
        assert_eq!(accessor.get(1), Datum::String("hello".to_string()));
        assert_eq!(accessor.get(2), Datum::Boolean(true));
        assert_eq!(accessor.get(3), Datum::Null); // Out of bounds
        assert_eq!(accessor.size(), 3);
    }
    
    #[test]
    fn test_struct_accessor_builder() {
        let accessor = StructAccessor::empty()
            .with_value(Datum::int(42))
            .with_value(Datum::string("test"));
        
        assert_eq!(accessor.size(), 2);
        assert_eq!(accessor.get(0), Datum::Int(42));
    }
    
    #[test]
    fn test_partition_accessor() {
        let accessor = PartitionAccessor::new()
            .with_field(100, Datum::int(2024))
            .with_field(101, Datum::int(1));
        
        assert_eq!(accessor.get_by_id(100), Datum::Int(2024));
        assert_eq!(accessor.get_by_id(101), Datum::Int(1));
        assert_eq!(accessor.get_by_id(999), Datum::Null); // Unknown field
    }
}
