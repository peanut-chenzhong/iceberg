//! Expression system for Iceberg
//!
//! This module provides a complete expression system for Iceberg, including:
//! - Operations (comparison, logical, etc.)
//! - Datum (typed values)
//! - Literals (constant values)
//! - References (column references)
//! - Predicates (unary, literal, set)
//! - Logical expressions (And, Or, Not)
//! - Evaluator (expression evaluation)
//! - Binder (binding expressions to schema)

pub mod operation;
pub mod datum;
pub mod literal;
pub mod reference;
pub mod predicate;
pub mod logical;
pub mod evaluator;
pub mod binder;
pub mod accessor;

pub use operation::Operation;
pub use datum::Datum;
pub use literal::Literal;
pub use reference::{Reference, BoundReference};
pub use predicate::{
    Predicate, UnboundPredicate, BoundPredicate,
    UnaryPredicate, LiteralPredicate, SetPredicate,
};
pub use logical::{And, Or, Not};
pub use evaluator::Evaluator;
pub use binder::Binder;
pub use accessor::Accessor;

use std::fmt::Debug;
use std::sync::Arc;

/// Expression trait - the base of all expressions in Iceberg
pub trait Expression: Debug + Send + Sync {
    /// Returns the operation type of this expression
    fn op(&self) -> Operation;
    
    /// Returns the negation of this expression
    fn negate(&self) -> Option<Arc<dyn Expression>>;
    
    /// Check if this expression is equivalent to another
    fn is_equivalent_to(&self, other: &dyn Expression) -> bool {
        false
    }
}

/// A bound expression that can be evaluated
pub trait BoundExpression: Expression {
    /// Evaluate this expression against a row
    fn eval(&self, accessor: &dyn Accessor) -> Datum;
}

/// Expression factory methods
pub struct Expressions;

impl Expressions {
    /// Create an always true expression
    pub fn always_true() -> Arc<dyn Expression> {
        Arc::new(logical::AlwaysTrue)
    }
    
    /// Create an always false expression
    pub fn always_false() -> Arc<dyn Expression> {
        Arc::new(logical::AlwaysFalse)
    }
    
    /// Create an AND expression
    pub fn and(left: Arc<dyn Expression>, right: Arc<dyn Expression>) -> Arc<dyn Expression> {
        // Short circuit evaluation
        if left.op() == Operation::False || right.op() == Operation::False {
            return Self::always_false();
        }
        if left.op() == Operation::True {
            return right;
        }
        if right.op() == Operation::True {
            return left;
        }
        Arc::new(And::new(left, right))
    }
    
    /// Create an OR expression
    pub fn or(left: Arc<dyn Expression>, right: Arc<dyn Expression>) -> Arc<dyn Expression> {
        // Short circuit evaluation
        if left.op() == Operation::True || right.op() == Operation::True {
            return Self::always_true();
        }
        if left.op() == Operation::False {
            return right;
        }
        if right.op() == Operation::False {
            return left;
        }
        Arc::new(Or::new(left, right))
    }
    
    /// Create a NOT expression
    pub fn not(child: Arc<dyn Expression>) -> Arc<dyn Expression> {
        if child.op() == Operation::True {
            return Self::always_false();
        }
        if child.op() == Operation::False {
            return Self::always_true();
        }
        if let Some(negated) = child.negate() {
            return negated;
        }
        Arc::new(Not::new(child))
    }
    
    /// Create a column reference
    pub fn col(name: &str) -> Reference {
        Reference::new(name)
    }
    
    /// Create an IS NULL predicate
    pub fn is_null(name: &str) -> UnboundPredicate {
        UnboundPredicate::unary(Operation::IsNull, Reference::new(name))
    }
    
    /// Create a NOT NULL predicate
    pub fn not_null(name: &str) -> UnboundPredicate {
        UnboundPredicate::unary(Operation::NotNull, Reference::new(name))
    }
    
    /// Create an IS NAN predicate
    pub fn is_nan(name: &str) -> UnboundPredicate {
        UnboundPredicate::unary(Operation::IsNaN, Reference::new(name))
    }
    
    /// Create a NOT NAN predicate
    pub fn not_nan(name: &str) -> UnboundPredicate {
        UnboundPredicate::unary(Operation::NotNaN, Reference::new(name))
    }
    
    /// Create a less than predicate
    pub fn less_than<T: Into<Literal>>(name: &str, value: T) -> UnboundPredicate {
        UnboundPredicate::literal(Operation::Lt, Reference::new(name), value.into())
    }
    
    /// Create a less than or equal predicate
    pub fn less_than_or_equal<T: Into<Literal>>(name: &str, value: T) -> UnboundPredicate {
        UnboundPredicate::literal(Operation::LtEq, Reference::new(name), value.into())
    }
    
    /// Create a greater than predicate
    pub fn greater_than<T: Into<Literal>>(name: &str, value: T) -> UnboundPredicate {
        UnboundPredicate::literal(Operation::Gt, Reference::new(name), value.into())
    }
    
    /// Create a greater than or equal predicate
    pub fn greater_than_or_equal<T: Into<Literal>>(name: &str, value: T) -> UnboundPredicate {
        UnboundPredicate::literal(Operation::GtEq, Reference::new(name), value.into())
    }
    
    /// Create an equal predicate
    pub fn equal<T: Into<Literal>>(name: &str, value: T) -> UnboundPredicate {
        UnboundPredicate::literal(Operation::Eq, Reference::new(name), value.into())
    }
    
    /// Create a not equal predicate
    pub fn not_equal<T: Into<Literal>>(name: &str, value: T) -> UnboundPredicate {
        UnboundPredicate::literal(Operation::NotEq, Reference::new(name), value.into())
    }
    
    /// Create a starts with predicate
    pub fn starts_with(name: &str, value: &str) -> UnboundPredicate {
        UnboundPredicate::literal(Operation::StartsWith, Reference::new(name), Literal::string(value))
    }
    
    /// Create an IN predicate
    pub fn in_list<T: Into<Literal>>(name: &str, values: Vec<T>) -> UnboundPredicate {
        let literals: Vec<Literal> = values.into_iter().map(|v| v.into()).collect();
        UnboundPredicate::set(Operation::In, Reference::new(name), literals)
    }
    
    /// Create a NOT IN predicate
    pub fn not_in<T: Into<Literal>>(name: &str, values: Vec<T>) -> UnboundPredicate {
        let literals: Vec<Literal> = values.into_iter().map(|v| v.into()).collect();
        UnboundPredicate::set(Operation::NotIn, Reference::new(name), literals)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_always_true() {
        let expr = Expressions::always_true();
        assert_eq!(expr.op(), Operation::True);
    }
    
    #[test]
    fn test_always_false() {
        let expr = Expressions::always_false();
        assert_eq!(expr.op(), Operation::False);
    }
    
    #[test]
    fn test_and_short_circuit() {
        let t = Expressions::always_true();
        let f = Expressions::always_false();
        
        // false AND x = false
        let result = Expressions::and(f.clone(), t.clone());
        assert_eq!(result.op(), Operation::False);
        
        // true AND x = x
        let result = Expressions::and(t.clone(), f.clone());
        assert_eq!(result.op(), Operation::False);
    }
    
    #[test]
    fn test_or_short_circuit() {
        let t = Expressions::always_true();
        let f = Expressions::always_false();
        
        // true OR x = true
        let result = Expressions::or(t.clone(), f.clone());
        assert_eq!(result.op(), Operation::True);
        
        // false OR x = x
        let result = Expressions::or(f.clone(), t.clone());
        assert_eq!(result.op(), Operation::True);
    }
}
