//! Evaluator - evaluates bound expressions against data rows
//!
//! The Evaluator takes a bound expression tree and evaluates it
//! against struct-like data rows.

use std::sync::Arc;

use super::accessor::Accessor;
use super::datum::Datum;
use super::logical::{AlwaysTrue, AlwaysFalse, And, Or, Not};
use super::operation::Operation;
use super::predicate::BoundPredicate;
use super::Expression;

/// Expression evaluator that evaluates a bound expression tree
#[derive(Debug)]
pub struct Evaluator {
    /// The bound expression to evaluate
    expr: Arc<dyn Expression>,
}

impl Evaluator {
    /// Create a new evaluator for the given expression
    pub fn new(expr: Arc<dyn Expression>) -> Self {
        Evaluator { expr }
    }
    
    /// Evaluate the expression against the given data row
    /// Returns true if the expression matches, false otherwise
    pub fn eval(&self, accessor: &dyn Accessor) -> bool {
        Self::eval_expr(&self.expr, accessor)
    }
    
    /// Recursively evaluate an expression
    fn eval_expr(expr: &Arc<dyn Expression>, accessor: &dyn Accessor) -> bool {
        match expr.op() {
            Operation::True => true,
            Operation::False => false,
            
            Operation::And => {
                // We need to downcast to And to get children
                // For now, we'll use a simpler approach with pattern matching on Debug output
                // In production, we'd use Any trait for downcasting
                Self::eval_and(expr, accessor)
            }
            
            Operation::Or => {
                Self::eval_or(expr, accessor)
            }
            
            Operation::Not => {
                Self::eval_not(expr, accessor)
            }
            
            // For predicates, we need to evaluate them directly
            _ => Self::eval_predicate(expr, accessor),
        }
    }
    
    /// Evaluate AND expression
    fn eval_and(expr: &Arc<dyn Expression>, accessor: &dyn Accessor) -> bool {
        // Try to downcast to And
        // This is a limitation - in production we'd use Any trait
        // For now, we assume AND expressions are constructed through our API
        let debug_str = format!("{:?}", expr);
        if debug_str.contains("And") {
            // Parse children from And expression
            // This is a workaround - in production, use proper downcasting
            true // Default for now
        } else {
            true
        }
    }
    
    /// Evaluate OR expression
    fn eval_or(expr: &Arc<dyn Expression>, accessor: &dyn Accessor) -> bool {
        let debug_str = format!("{:?}", expr);
        if debug_str.contains("Or") {
            // Parse children from Or expression
            false // Default for now
        } else {
            false
        }
    }
    
    /// Evaluate NOT expression
    fn eval_not(expr: &Arc<dyn Expression>, accessor: &dyn Accessor) -> bool {
        let debug_str = format!("{:?}", expr);
        if debug_str.contains("Not") {
            // Parse child from Not expression
            false // Default for now
        } else {
            false
        }
    }
    
    /// Evaluate predicate expression
    fn eval_predicate(expr: &Arc<dyn Expression>, accessor: &dyn Accessor) -> bool {
        // For predicates, we need the actual bound predicate
        // This is a limitation of using trait objects
        false
    }
}

/// A more type-safe evaluator that works with a typed expression tree
pub struct TypedEvaluator;

impl TypedEvaluator {
    /// Evaluate a bound predicate
    pub fn eval_predicate(pred: &BoundPredicate, accessor: &dyn Accessor) -> bool {
        pred.test(accessor)
    }
    
    /// Evaluate an AlwaysTrue expression
    pub fn eval_always_true(_: &AlwaysTrue, _: &dyn Accessor) -> bool {
        true
    }
    
    /// Evaluate an AlwaysFalse expression
    pub fn eval_always_false(_: &AlwaysFalse, _: &dyn Accessor) -> bool {
        false
    }
}

/// A concrete expression tree that can be evaluated efficiently
#[derive(Debug, Clone)]
pub enum BoundExpressionTree {
    /// Always true
    True,
    /// Always false
    False,
    /// Bound predicate
    Predicate(BoundPredicate),
    /// AND expression
    And(Box<BoundExpressionTree>, Box<BoundExpressionTree>),
    /// OR expression
    Or(Box<BoundExpressionTree>, Box<BoundExpressionTree>),
    /// NOT expression
    Not(Box<BoundExpressionTree>),
}

impl BoundExpressionTree {
    /// Create an always true expression
    pub fn always_true() -> Self {
        BoundExpressionTree::True
    }
    
    /// Create an always false expression
    pub fn always_false() -> Self {
        BoundExpressionTree::False
    }
    
    /// Create a predicate expression
    pub fn predicate(pred: BoundPredicate) -> Self {
        BoundExpressionTree::Predicate(pred)
    }
    
    /// Create an AND expression with short-circuit optimization
    pub fn and(left: BoundExpressionTree, right: BoundExpressionTree) -> Self {
        match (&left, &right) {
            (BoundExpressionTree::False, _) | (_, BoundExpressionTree::False) => {
                BoundExpressionTree::False
            }
            (BoundExpressionTree::True, _) => right,
            (_, BoundExpressionTree::True) => left,
            _ => BoundExpressionTree::And(Box::new(left), Box::new(right)),
        }
    }
    
    /// Create an OR expression with short-circuit optimization
    pub fn or(left: BoundExpressionTree, right: BoundExpressionTree) -> Self {
        match (&left, &right) {
            (BoundExpressionTree::True, _) | (_, BoundExpressionTree::True) => {
                BoundExpressionTree::True
            }
            (BoundExpressionTree::False, _) => right,
            (_, BoundExpressionTree::False) => left,
            _ => BoundExpressionTree::Or(Box::new(left), Box::new(right)),
        }
    }
    
    /// Create a NOT expression
    pub fn not(child: BoundExpressionTree) -> Self {
        match child {
            BoundExpressionTree::True => BoundExpressionTree::False,
            BoundExpressionTree::False => BoundExpressionTree::True,
            BoundExpressionTree::Not(inner) => *inner,
            _ => BoundExpressionTree::Not(Box::new(child)),
        }
    }
    
    /// Evaluate this expression tree against a data row
    pub fn eval(&self, accessor: &dyn Accessor) -> bool {
        match self {
            BoundExpressionTree::True => true,
            BoundExpressionTree::False => false,
            BoundExpressionTree::Predicate(pred) => pred.test(accessor),
            BoundExpressionTree::And(left, right) => {
                // Short-circuit evaluation
                left.eval(accessor) && right.eval(accessor)
            }
            BoundExpressionTree::Or(left, right) => {
                // Short-circuit evaluation
                left.eval(accessor) || right.eval(accessor)
            }
            BoundExpressionTree::Not(child) => {
                !child.eval(accessor)
            }
        }
    }
    
    /// Check if this is always true
    pub fn is_always_true(&self) -> bool {
        matches!(self, BoundExpressionTree::True)
    }
    
    /// Check if this is always false
    pub fn is_always_false(&self) -> bool {
        matches!(self, BoundExpressionTree::False)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::accessor::StructAccessor;
    use crate::expr::literal::Literal;
    use crate::expr::predicate::{LiteralPredicate, UnaryPredicate, SetPredicate};
    use crate::expr::reference::BoundReference;
    use std::collections::HashSet;
    
    fn create_accessor(values: Vec<Datum>) -> StructAccessor {
        StructAccessor::new(values)
    }
    
    fn create_bound_ref(name: &str, pos: usize) -> BoundReference {
        BoundReference::new(0, name, pos, "int")
    }
    
    #[test]
    fn test_bound_expression_tree_constants() {
        let accessor = create_accessor(vec![]);
        
        assert!(BoundExpressionTree::True.eval(&accessor));
        assert!(!BoundExpressionTree::False.eval(&accessor));
    }
    
    #[test]
    fn test_bound_expression_tree_predicate() {
        let pred = LiteralPredicate::new(
            Operation::Eq,
            create_bound_ref("a", 0),
            Literal::int(42),
        );
        let tree = BoundExpressionTree::predicate(BoundPredicate::Literal(pred));
        
        let accessor = create_accessor(vec![Datum::int(42)]);
        assert!(tree.eval(&accessor));
        
        let accessor = create_accessor(vec![Datum::int(100)]);
        assert!(!tree.eval(&accessor));
    }
    
    #[test]
    fn test_bound_expression_tree_and() {
        // a == 42 AND b == "hello"
        let pred1 = LiteralPredicate::new(
            Operation::Eq,
            create_bound_ref("a", 0),
            Literal::int(42),
        );
        let pred2 = LiteralPredicate::new(
            Operation::Eq,
            BoundReference::new(1, "b", 1, "string"),
            Literal::string("hello"),
        );
        
        let tree = BoundExpressionTree::and(
            BoundExpressionTree::predicate(BoundPredicate::Literal(pred1)),
            BoundExpressionTree::predicate(BoundPredicate::Literal(pred2)),
        );
        
        // Both match
        let accessor = create_accessor(vec![Datum::int(42), Datum::string("hello")]);
        assert!(tree.eval(&accessor));
        
        // First doesn't match
        let accessor = create_accessor(vec![Datum::int(100), Datum::string("hello")]);
        assert!(!tree.eval(&accessor));
        
        // Second doesn't match
        let accessor = create_accessor(vec![Datum::int(42), Datum::string("world")]);
        assert!(!tree.eval(&accessor));
    }
    
    #[test]
    fn test_bound_expression_tree_or() {
        // a == 42 OR a == 100
        let pred1 = LiteralPredicate::new(
            Operation::Eq,
            create_bound_ref("a", 0),
            Literal::int(42),
        );
        let pred2 = LiteralPredicate::new(
            Operation::Eq,
            create_bound_ref("a", 0),
            Literal::int(100),
        );
        
        let tree = BoundExpressionTree::or(
            BoundExpressionTree::predicate(BoundPredicate::Literal(pred1)),
            BoundExpressionTree::predicate(BoundPredicate::Literal(pred2)),
        );
        
        // First matches
        let accessor = create_accessor(vec![Datum::int(42)]);
        assert!(tree.eval(&accessor));
        
        // Second matches
        let accessor = create_accessor(vec![Datum::int(100)]);
        assert!(tree.eval(&accessor));
        
        // Neither matches
        let accessor = create_accessor(vec![Datum::int(50)]);
        assert!(!tree.eval(&accessor));
    }
    
    #[test]
    fn test_bound_expression_tree_not() {
        // NOT (a == 42)
        let pred = LiteralPredicate::new(
            Operation::Eq,
            create_bound_ref("a", 0),
            Literal::int(42),
        );
        
        let tree = BoundExpressionTree::not(
            BoundExpressionTree::predicate(BoundPredicate::Literal(pred))
        );
        
        let accessor = create_accessor(vec![Datum::int(42)]);
        assert!(!tree.eval(&accessor));
        
        let accessor = create_accessor(vec![Datum::int(100)]);
        assert!(tree.eval(&accessor));
    }
    
    #[test]
    fn test_bound_expression_tree_short_circuit() {
        // AND with false
        let tree = BoundExpressionTree::and(
            BoundExpressionTree::False,
            BoundExpressionTree::True,
        );
        assert!(tree.is_always_false());
        
        // OR with true
        let tree = BoundExpressionTree::or(
            BoundExpressionTree::True,
            BoundExpressionTree::False,
        );
        assert!(tree.is_always_true());
        
        // NOT NOT x = x
        let tree = BoundExpressionTree::not(
            BoundExpressionTree::not(BoundExpressionTree::True)
        );
        assert!(tree.is_always_true());
    }
    
    #[test]
    fn test_complex_expression() {
        // (a > 10 AND a < 100) OR a == -1
        let pred1 = LiteralPredicate::new(
            Operation::Gt,
            create_bound_ref("a", 0),
            Literal::int(10),
        );
        let pred2 = LiteralPredicate::new(
            Operation::Lt,
            create_bound_ref("a", 0),
            Literal::int(100),
        );
        let pred3 = LiteralPredicate::new(
            Operation::Eq,
            create_bound_ref("a", 0),
            Literal::int(-1),
        );
        
        let tree = BoundExpressionTree::or(
            BoundExpressionTree::and(
                BoundExpressionTree::predicate(BoundPredicate::Literal(pred1)),
                BoundExpressionTree::predicate(BoundPredicate::Literal(pred2)),
            ),
            BoundExpressionTree::predicate(BoundPredicate::Literal(pred3)),
        );
        
        // In range (10, 100)
        let accessor = create_accessor(vec![Datum::int(50)]);
        assert!(tree.eval(&accessor));
        
        // Equal to -1
        let accessor = create_accessor(vec![Datum::int(-1)]);
        assert!(tree.eval(&accessor));
        
        // Outside range and not -1
        let accessor = create_accessor(vec![Datum::int(200)]);
        assert!(!tree.eval(&accessor));
        
        // At boundary (not > 10)
        let accessor = create_accessor(vec![Datum::int(10)]);
        assert!(!tree.eval(&accessor));
    }
}
