//! Reference - column references in expressions
//!
//! References can be either unbound (by name) or bound (by position).

use std::fmt;
use super::accessor::Accessor;
use super::datum::Datum;

/// An unbound column reference (by name)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Reference {
    /// Column name
    name: String,
}

impl Reference {
    /// Create a new column reference
    pub fn new(name: impl Into<String>) -> Self {
        Reference { name: name.into() }
    }
    
    /// Get the column name
    pub fn name(&self) -> &str {
        &self.name
    }
    
    /// Bind this reference to a specific position
    pub fn bind(&self, field_id: i32, pos: usize, type_name: &str) -> BoundReference {
        BoundReference {
            field_id,
            name: self.name.clone(),
            pos,
            type_name: type_name.to_string(),
        }
    }
}

impl fmt::Display for Reference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ref({})", self.name)
    }
}

/// A bound column reference (by position)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BoundReference {
    /// Field ID in the schema
    field_id: i32,
    /// Column name (for display)
    name: String,
    /// Position in the struct
    pos: usize,
    /// Type name for validation
    type_name: String,
}

impl BoundReference {
    /// Create a new bound reference
    pub fn new(field_id: i32, name: impl Into<String>, pos: usize, type_name: impl Into<String>) -> Self {
        BoundReference {
            field_id,
            name: name.into(),
            pos,
            type_name: type_name.into(),
        }
    }
    
    /// Get the field ID
    pub fn field_id(&self) -> i32 {
        self.field_id
    }
    
    /// Get the column name
    pub fn name(&self) -> &str {
        &self.name
    }
    
    /// Get the position
    pub fn pos(&self) -> usize {
        self.pos
    }
    
    /// Get the type name
    pub fn type_name(&self) -> &str {
        &self.type_name
    }
    
    /// Evaluate this reference against a row
    pub fn eval(&self, accessor: &dyn Accessor) -> Datum {
        accessor.get(self.pos)
    }
}

impl fmt::Display for BoundReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ref({}, id={}, pos={})", self.name, self.field_id, self.pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::accessor::StructAccessor;
    
    #[test]
    fn test_reference_creation() {
        let ref_ = Reference::new("name");
        assert_eq!(ref_.name(), "name");
    }
    
    #[test]
    fn test_reference_bind() {
        let ref_ = Reference::new("age");
        let bound = ref_.bind(1, 0, "int");
        
        assert_eq!(bound.field_id(), 1);
        assert_eq!(bound.name(), "age");
        assert_eq!(bound.pos(), 0);
        assert_eq!(bound.type_name(), "int");
    }
    
    #[test]
    fn test_bound_reference_eval() {
        let bound = BoundReference::new(1, "age", 0, "int");
        let accessor = StructAccessor::new(vec![Datum::int(25)]);
        
        assert_eq!(bound.eval(&accessor), Datum::Int(25));
    }
    
    #[test]
    fn test_reference_display() {
        let ref_ = Reference::new("name");
        assert_eq!(format!("{}", ref_), "ref(name)");
        
        let bound = BoundReference::new(1, "name", 0, "string");
        assert_eq!(format!("{}", bound), "ref(name, id=1, pos=0)");
    }
}
