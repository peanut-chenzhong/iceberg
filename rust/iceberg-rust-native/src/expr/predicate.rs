//! Predicate - boolean expressions for filtering
//!
//! This module provides three types of predicates:
//! - UnaryPredicate: IS NULL, NOT NULL, IS NAN, NOT NAN
//! - LiteralPredicate: comparison with a literal value
//! - SetPredicate: IN, NOT IN

use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use super::accessor::Accessor;
use super::datum::Datum;
use super::literal::Literal;
use super::operation::Operation;
use super::reference::{BoundReference, Reference};
use super::{Expression, BoundExpression};

/// Marker trait for predicates
pub trait Predicate: Expression {
    /// Get the operation
    fn operation(&self) -> Operation;
}

// ============================================================================
// Unbound Predicate
// ============================================================================

/// An unbound predicate (not yet bound to schema)
#[derive(Debug, Clone)]
pub enum UnboundPredicate {
    /// Unary predicate (IS NULL, NOT NULL, etc.)
    Unary {
        op: Operation,
        term: Reference,
    },
    /// Literal predicate (comparison with a single value)
    Literal {
        op: Operation,
        term: Reference,
        literal: Literal,
    },
    /// Set predicate (IN, NOT IN)
    Set {
        op: Operation,
        term: Reference,
        literals: Vec<Literal>,
    },
}

impl UnboundPredicate {
    /// Create a unary predicate
    pub fn unary(op: Operation, term: Reference) -> Self {
        UnboundPredicate::Unary { op, term }
    }
    
    /// Create a literal predicate
    pub fn literal(op: Operation, term: Reference, literal: Literal) -> Self {
        UnboundPredicate::Literal { op, term, literal }
    }
    
    /// Create a set predicate
    pub fn set(op: Operation, term: Reference, literals: Vec<Literal>) -> Self {
        UnboundPredicate::Set { op, term, literals }
    }
    
    /// Get the term (column reference)
    pub fn term(&self) -> &Reference {
        match self {
            UnboundPredicate::Unary { term, .. } => term,
            UnboundPredicate::Literal { term, .. } => term,
            UnboundPredicate::Set { term, .. } => term,
        }
    }
    
    /// Bind this predicate to a bound reference
    pub fn bind(&self, bound_ref: BoundReference) -> BoundPredicate {
        match self {
            UnboundPredicate::Unary { op, .. } => {
                BoundPredicate::Unary(UnaryPredicate {
                    op: *op,
                    term: bound_ref,
                })
            }
            UnboundPredicate::Literal { op, literal, .. } => {
                BoundPredicate::Literal(LiteralPredicate {
                    op: *op,
                    term: bound_ref,
                    literal: literal.clone(),
                })
            }
            UnboundPredicate::Set { op, literals, .. } => {
                let values: HashSet<Datum> = literals
                    .iter()
                    .map(|l| l.value().clone())
                    .collect();
                BoundPredicate::Set(SetPredicate {
                    op: *op,
                    term: bound_ref,
                    values,
                })
            }
        }
    }
}

impl Expression for UnboundPredicate {
    fn op(&self) -> Operation {
        match self {
            UnboundPredicate::Unary { op, .. } => *op,
            UnboundPredicate::Literal { op, .. } => *op,
            UnboundPredicate::Set { op, .. } => *op,
        }
    }
    
    fn negate(&self) -> Option<Arc<dyn Expression>> {
        let negated_op = self.op().negate()?;
        let negated = match self {
            UnboundPredicate::Unary { term, .. } => {
                UnboundPredicate::Unary { op: negated_op, term: term.clone() }
            }
            UnboundPredicate::Literal { term, literal, .. } => {
                UnboundPredicate::Literal { 
                    op: negated_op, 
                    term: term.clone(),
                    literal: literal.clone(),
                }
            }
            UnboundPredicate::Set { term, literals, .. } => {
                UnboundPredicate::Set {
                    op: negated_op,
                    term: term.clone(),
                    literals: literals.clone(),
                }
            }
        };
        Some(Arc::new(negated))
    }
}

impl Predicate for UnboundPredicate {
    fn operation(&self) -> Operation {
        self.op()
    }
}

impl fmt::Display for UnboundPredicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnboundPredicate::Unary { op, term } => {
                write!(f, "{}({})", op, term)
            }
            UnboundPredicate::Literal { op, term, literal } => {
                write!(f, "{} {} {}", term, op, literal)
            }
            UnboundPredicate::Set { op, term, literals } => {
                let values: Vec<String> = literals.iter().map(|l| l.to_string()).collect();
                write!(f, "{} {} ({})", term, op, values.join(", "))
            }
        }
    }
}

// ============================================================================
// Bound Predicate
// ============================================================================

/// A bound predicate (bound to schema, can be evaluated)
#[derive(Debug, Clone)]
pub enum BoundPredicate {
    /// Unary predicate
    Unary(UnaryPredicate),
    /// Literal predicate
    Literal(LiteralPredicate),
    /// Set predicate
    Set(SetPredicate),
}

impl BoundPredicate {
    /// Test this predicate against a value
    pub fn test(&self, accessor: &dyn Accessor) -> bool {
        match self {
            BoundPredicate::Unary(p) => p.test(accessor),
            BoundPredicate::Literal(p) => p.test(accessor),
            BoundPredicate::Set(p) => p.test(accessor),
        }
    }
    
    /// Get the bound reference
    pub fn term(&self) -> &BoundReference {
        match self {
            BoundPredicate::Unary(p) => &p.term,
            BoundPredicate::Literal(p) => &p.term,
            BoundPredicate::Set(p) => &p.term,
        }
    }
}

impl Expression for BoundPredicate {
    fn op(&self) -> Operation {
        match self {
            BoundPredicate::Unary(p) => p.op,
            BoundPredicate::Literal(p) => p.op,
            BoundPredicate::Set(p) => p.op,
        }
    }
    
    fn negate(&self) -> Option<Arc<dyn Expression>> {
        let negated_op = self.op().negate()?;
        let negated = match self {
            BoundPredicate::Unary(p) => {
                BoundPredicate::Unary(UnaryPredicate {
                    op: negated_op,
                    term: p.term.clone(),
                })
            }
            BoundPredicate::Literal(p) => {
                BoundPredicate::Literal(LiteralPredicate {
                    op: negated_op,
                    term: p.term.clone(),
                    literal: p.literal.clone(),
                })
            }
            BoundPredicate::Set(p) => {
                BoundPredicate::Set(SetPredicate {
                    op: negated_op,
                    term: p.term.clone(),
                    values: p.values.clone(),
                })
            }
        };
        Some(Arc::new(negated))
    }
}

impl BoundExpression for BoundPredicate {
    fn eval(&self, accessor: &dyn Accessor) -> Datum {
        Datum::Boolean(self.test(accessor))
    }
}

impl Predicate for BoundPredicate {
    fn operation(&self) -> Operation {
        self.op()
    }
}

impl fmt::Display for BoundPredicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BoundPredicate::Unary(p) => write!(f, "{}", p),
            BoundPredicate::Literal(p) => write!(f, "{}", p),
            BoundPredicate::Set(p) => write!(f, "{}", p),
        }
    }
}

// ============================================================================
// Unary Predicate
// ============================================================================

/// A unary predicate (IS NULL, NOT NULL, IS NAN, NOT NAN)
#[derive(Debug, Clone)]
pub struct UnaryPredicate {
    /// Operation
    pub op: Operation,
    /// Column reference
    pub term: BoundReference,
}

impl UnaryPredicate {
    /// Create a new unary predicate
    pub fn new(op: Operation, term: BoundReference) -> Self {
        UnaryPredicate { op, term }
    }
    
    /// Test this predicate against a row
    pub fn test(&self, accessor: &dyn Accessor) -> bool {
        let value = self.term.eval(accessor);
        match self.op {
            Operation::IsNull => value.is_null(),
            Operation::NotNull => !value.is_null(),
            Operation::IsNaN => value.is_nan(),
            Operation::NotNaN => !value.is_nan(),
            _ => false,
        }
    }
}

impl fmt::Display for UnaryPredicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self.op, self.term)
    }
}

// ============================================================================
// Literal Predicate
// ============================================================================

/// A literal predicate (comparison with a single value)
#[derive(Debug, Clone)]
pub struct LiteralPredicate {
    /// Operation
    pub op: Operation,
    /// Column reference
    pub term: BoundReference,
    /// Literal value to compare against
    pub literal: Literal,
}

impl LiteralPredicate {
    /// Create a new literal predicate
    pub fn new(op: Operation, term: BoundReference, literal: Literal) -> Self {
        LiteralPredicate { op, term, literal }
    }
    
    /// Test this predicate against a row
    pub fn test(&self, accessor: &dyn Accessor) -> bool {
        let value = self.term.eval(accessor);
        
        // Handle null values
        if value.is_null() {
            return false;
        }
        
        // Compare with literal
        let cmp = match self.literal.compare(&value) {
            Some(c) => c,
            None => return false, // Incompatible types or NaN
        };
        
        // Note: cmp is literal.compare(value), so we need to flip the interpretation
        // If literal < value, then value > literal
        match self.op {
            Operation::Lt => cmp == Ordering::Greater,   // value < literal means literal > value
            Operation::LtEq => cmp != Ordering::Less,    // value <= literal means literal >= value
            Operation::Gt => cmp == Ordering::Less,      // value > literal means literal < value
            Operation::GtEq => cmp != Ordering::Greater, // value >= literal means literal <= value
            Operation::Eq => cmp == Ordering::Equal,
            Operation::NotEq => cmp != Ordering::Equal,
            Operation::StartsWith => {
                if let (Datum::String(v), Datum::String(l)) = (&value, self.literal.value()) {
                    v.starts_with(l)
                } else {
                    false
                }
            }
            Operation::NotStartsWith => {
                if let (Datum::String(v), Datum::String(l)) = (&value, self.literal.value()) {
                    !v.starts_with(l)
                } else {
                    true
                }
            }
            _ => false,
        }
    }
    
    /// Get the literal value
    pub fn literal(&self) -> &Literal {
        &self.literal
    }
}

impl fmt::Display for LiteralPredicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.term, self.op, self.literal)
    }
}

// ============================================================================
// Set Predicate
// ============================================================================

/// A set predicate (IN, NOT IN)
#[derive(Debug, Clone)]
pub struct SetPredicate {
    /// Operation (IN or NOT_IN)
    pub op: Operation,
    /// Column reference
    pub term: BoundReference,
    /// Set of values
    pub values: HashSet<Datum>,
}

impl SetPredicate {
    /// Create a new set predicate
    pub fn new(op: Operation, term: BoundReference, values: HashSet<Datum>) -> Self {
        SetPredicate { op, term, values }
    }
    
    /// Test this predicate against a row
    pub fn test(&self, accessor: &dyn Accessor) -> bool {
        let value = self.term.eval(accessor);
        
        // Handle null values
        if value.is_null() {
            return false;
        }
        
        let contains = self.values.contains(&value);
        match self.op {
            Operation::In => contains,
            Operation::NotIn => !contains,
            _ => false,
        }
    }
    
    /// Get the values
    pub fn values(&self) -> &HashSet<Datum> {
        &self.values
    }
}

impl fmt::Display for SetPredicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let values: Vec<String> = self.values.iter().map(|v| v.to_string()).collect();
        write!(f, "{} {} ({})", self.term, self.op, values.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::accessor::StructAccessor;
    
    fn create_accessor(values: Vec<Datum>) -> StructAccessor {
        StructAccessor::new(values)
    }
    
    fn create_bound_ref(name: &str, pos: usize) -> BoundReference {
        BoundReference::new(0, name, pos, "int")
    }
    
    #[test]
    fn test_unary_is_null() {
        let pred = UnaryPredicate::new(Operation::IsNull, create_bound_ref("a", 0));
        
        // Test with null value
        let accessor = create_accessor(vec![Datum::Null]);
        assert!(pred.test(&accessor));
        
        // Test with non-null value
        let accessor = create_accessor(vec![Datum::int(1)]);
        assert!(!pred.test(&accessor));
    }
    
    #[test]
    fn test_unary_not_null() {
        let pred = UnaryPredicate::new(Operation::NotNull, create_bound_ref("a", 0));
        
        let accessor = create_accessor(vec![Datum::int(1)]);
        assert!(pred.test(&accessor));
        
        let accessor = create_accessor(vec![Datum::Null]);
        assert!(!pred.test(&accessor));
    }
    
    #[test]
    fn test_unary_is_nan() {
        let ref_ = BoundReference::new(0, "a", 0, "float");
        let pred = UnaryPredicate::new(Operation::IsNaN, ref_);
        
        let accessor = create_accessor(vec![Datum::float(f32::NAN)]);
        assert!(pred.test(&accessor));
        
        let accessor = create_accessor(vec![Datum::float(1.0)]);
        assert!(!pred.test(&accessor));
    }
    
    #[test]
    fn test_literal_equal() {
        let pred = LiteralPredicate::new(
            Operation::Eq,
            create_bound_ref("a", 0),
            Literal::int(42),
        );
        
        let accessor = create_accessor(vec![Datum::int(42)]);
        assert!(pred.test(&accessor));
        
        let accessor = create_accessor(vec![Datum::int(100)]);
        assert!(!pred.test(&accessor));
    }
    
    #[test]
    fn test_literal_less_than() {
        let pred = LiteralPredicate::new(
            Operation::Lt,
            create_bound_ref("a", 0),
            Literal::int(10),
        );
        
        let accessor = create_accessor(vec![Datum::int(5)]);
        assert!(pred.test(&accessor));
        
        let accessor = create_accessor(vec![Datum::int(10)]);
        assert!(!pred.test(&accessor));
        
        let accessor = create_accessor(vec![Datum::int(15)]);
        assert!(!pred.test(&accessor));
    }
    
    #[test]
    fn test_literal_greater_than() {
        let pred = LiteralPredicate::new(
            Operation::Gt,
            create_bound_ref("a", 0),
            Literal::int(10),
        );
        
        let accessor = create_accessor(vec![Datum::int(15)]);
        assert!(pred.test(&accessor));
        
        let accessor = create_accessor(vec![Datum::int(10)]);
        assert!(!pred.test(&accessor));
        
        let accessor = create_accessor(vec![Datum::int(5)]);
        assert!(!pred.test(&accessor));
    }
    
    #[test]
    fn test_literal_starts_with() {
        let ref_ = BoundReference::new(0, "name", 0, "string");
        let pred = LiteralPredicate::new(
            Operation::StartsWith,
            ref_,
            Literal::string("hello"),
        );
        
        let accessor = create_accessor(vec![Datum::string("hello world")]);
        assert!(pred.test(&accessor));
        
        let accessor = create_accessor(vec![Datum::string("goodbye")]);
        assert!(!pred.test(&accessor));
    }
    
    #[test]
    fn test_set_in() {
        let values: HashSet<Datum> = vec![Datum::int(1), Datum::int(2), Datum::int(3)]
            .into_iter()
            .collect();
        let pred = SetPredicate::new(Operation::In, create_bound_ref("a", 0), values);
        
        let accessor = create_accessor(vec![Datum::int(2)]);
        assert!(pred.test(&accessor));
        
        let accessor = create_accessor(vec![Datum::int(5)]);
        assert!(!pred.test(&accessor));
    }
    
    #[test]
    fn test_set_not_in() {
        let values: HashSet<Datum> = vec![Datum::int(1), Datum::int(2), Datum::int(3)]
            .into_iter()
            .collect();
        let pred = SetPredicate::new(Operation::NotIn, create_bound_ref("a", 0), values);
        
        let accessor = create_accessor(vec![Datum::int(5)]);
        assert!(pred.test(&accessor));
        
        let accessor = create_accessor(vec![Datum::int(2)]);
        assert!(!pred.test(&accessor));
    }
    
    #[test]
    fn test_null_handling() {
        // Comparison predicates should return false for null values
        let pred = LiteralPredicate::new(
            Operation::Eq,
            create_bound_ref("a", 0),
            Literal::int(42),
        );
        
        let accessor = create_accessor(vec![Datum::Null]);
        assert!(!pred.test(&accessor));
    }
    
    #[test]
    fn test_unbound_predicate_bind() {
        let unbound = UnboundPredicate::literal(
            Operation::Eq,
            Reference::new("age"),
            Literal::int(25),
        );
        
        let bound_ref = BoundReference::new(1, "age", 0, "int");
        let bound = unbound.bind(bound_ref);
        
        let accessor = create_accessor(vec![Datum::int(25)]);
        assert!(bound.test(&accessor));
    }
}
