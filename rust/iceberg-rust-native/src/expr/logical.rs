//! Logical expressions - And, Or, Not, True, False

use std::fmt;
use std::sync::Arc;

use super::accessor::Accessor;
use super::datum::Datum;
use super::operation::Operation;
use super::{Expression, BoundExpression};

// ============================================================================
// Always True
// ============================================================================

/// An expression that always evaluates to true
#[derive(Debug, Clone, Copy)]
pub struct AlwaysTrue;

impl Expression for AlwaysTrue {
    fn op(&self) -> Operation {
        Operation::True
    }
    
    fn negate(&self) -> Option<Arc<dyn Expression>> {
        Some(Arc::new(AlwaysFalse))
    }
    
    fn is_equivalent_to(&self, other: &dyn Expression) -> bool {
        other.op() == Operation::True
    }
}

impl BoundExpression for AlwaysTrue {
    fn eval(&self, _accessor: &dyn Accessor) -> Datum {
        Datum::Boolean(true)
    }
}

impl fmt::Display for AlwaysTrue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "true")
    }
}

// ============================================================================
// Always False
// ============================================================================

/// An expression that always evaluates to false
#[derive(Debug, Clone, Copy)]
pub struct AlwaysFalse;

impl Expression for AlwaysFalse {
    fn op(&self) -> Operation {
        Operation::False
    }
    
    fn negate(&self) -> Option<Arc<dyn Expression>> {
        Some(Arc::new(AlwaysTrue))
    }
    
    fn is_equivalent_to(&self, other: &dyn Expression) -> bool {
        other.op() == Operation::False
    }
}

impl BoundExpression for AlwaysFalse {
    fn eval(&self, _accessor: &dyn Accessor) -> Datum {
        Datum::Boolean(false)
    }
}

impl fmt::Display for AlwaysFalse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "false")
    }
}

// ============================================================================
// And
// ============================================================================

/// Logical AND expression
#[derive(Debug)]
pub struct And {
    /// Left operand
    left: Arc<dyn Expression>,
    /// Right operand
    right: Arc<dyn Expression>,
}

impl And {
    /// Create a new AND expression
    pub fn new(left: Arc<dyn Expression>, right: Arc<dyn Expression>) -> Self {
        And { left, right }
    }
    
    /// Get the left operand
    pub fn left(&self) -> &Arc<dyn Expression> {
        &self.left
    }
    
    /// Get the right operand
    pub fn right(&self) -> &Arc<dyn Expression> {
        &self.right
    }
}

impl Expression for And {
    fn op(&self) -> Operation {
        Operation::And
    }
    
    fn negate(&self) -> Option<Arc<dyn Expression>> {
        // not(and(a, b)) => or(not(a), not(b)) - De Morgan's law
        let left_negated = self.left.negate()?;
        let right_negated = self.right.negate()?;
        Some(Arc::new(Or::new(left_negated, right_negated)))
    }
    
    fn is_equivalent_to(&self, other: &dyn Expression) -> bool {
        if other.op() != Operation::And {
            return false;
        }
        // We can't easily compare the children without downcasting
        // For now, just return false for non-identical expressions
        false
    }
}

impl fmt::Display for And {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({:?} and {:?})", self.left, self.right)
    }
}

// ============================================================================
// Or
// ============================================================================

/// Logical OR expression
#[derive(Debug)]
pub struct Or {
    /// Left operand
    left: Arc<dyn Expression>,
    /// Right operand
    right: Arc<dyn Expression>,
}

impl Or {
    /// Create a new OR expression
    pub fn new(left: Arc<dyn Expression>, right: Arc<dyn Expression>) -> Self {
        Or { left, right }
    }
    
    /// Get the left operand
    pub fn left(&self) -> &Arc<dyn Expression> {
        &self.left
    }
    
    /// Get the right operand
    pub fn right(&self) -> &Arc<dyn Expression> {
        &self.right
    }
}

impl Expression for Or {
    fn op(&self) -> Operation {
        Operation::Or
    }
    
    fn negate(&self) -> Option<Arc<dyn Expression>> {
        // not(or(a, b)) => and(not(a), not(b)) - De Morgan's law
        let left_negated = self.left.negate()?;
        let right_negated = self.right.negate()?;
        Some(Arc::new(And::new(left_negated, right_negated)))
    }
    
    fn is_equivalent_to(&self, other: &dyn Expression) -> bool {
        if other.op() != Operation::Or {
            return false;
        }
        false
    }
}

impl fmt::Display for Or {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({:?} or {:?})", self.left, self.right)
    }
}

// ============================================================================
// Not
// ============================================================================

/// Logical NOT expression
#[derive(Debug)]
pub struct Not {
    /// Child operand
    child: Arc<dyn Expression>,
}

impl Not {
    /// Create a new NOT expression
    pub fn new(child: Arc<dyn Expression>) -> Self {
        Not { child }
    }
    
    /// Get the child operand
    pub fn child(&self) -> &Arc<dyn Expression> {
        &self.child
    }
}

impl Expression for Not {
    fn op(&self) -> Operation {
        Operation::Not
    }
    
    fn negate(&self) -> Option<Arc<dyn Expression>> {
        // not(not(x)) => x
        Some(self.child.clone())
    }
    
    fn is_equivalent_to(&self, other: &dyn Expression) -> bool {
        if other.op() != Operation::Not {
            return false;
        }
        false
    }
}

impl fmt::Display for Not {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "not({:?})", self.child)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_always_true() {
        let expr = AlwaysTrue;
        assert_eq!(expr.op(), Operation::True);
        
        // Test negation
        let negated = expr.negate().unwrap();
        assert_eq!(negated.op(), Operation::False);
    }
    
    #[test]
    fn test_always_false() {
        let expr = AlwaysFalse;
        assert_eq!(expr.op(), Operation::False);
        
        // Test negation
        let negated = expr.negate().unwrap();
        assert_eq!(negated.op(), Operation::True);
    }
    
    #[test]
    fn test_and_expression() {
        let left: Arc<dyn Expression> = Arc::new(AlwaysTrue);
        let right: Arc<dyn Expression> = Arc::new(AlwaysFalse);
        let and_expr = And::new(left, right);
        
        assert_eq!(and_expr.op(), Operation::And);
        assert_eq!(and_expr.left().op(), Operation::True);
        assert_eq!(and_expr.right().op(), Operation::False);
    }
    
    #[test]
    fn test_and_negate() {
        // not(and(true, false)) => or(false, true)
        let left: Arc<dyn Expression> = Arc::new(AlwaysTrue);
        let right: Arc<dyn Expression> = Arc::new(AlwaysFalse);
        let and_expr = And::new(left, right);
        
        let negated = and_expr.negate().unwrap();
        assert_eq!(negated.op(), Operation::Or);
    }
    
    #[test]
    fn test_or_expression() {
        let left: Arc<dyn Expression> = Arc::new(AlwaysTrue);
        let right: Arc<dyn Expression> = Arc::new(AlwaysFalse);
        let or_expr = Or::new(left, right);
        
        assert_eq!(or_expr.op(), Operation::Or);
    }
    
    #[test]
    fn test_or_negate() {
        // not(or(true, false)) => and(false, true)
        let left: Arc<dyn Expression> = Arc::new(AlwaysTrue);
        let right: Arc<dyn Expression> = Arc::new(AlwaysFalse);
        let or_expr = Or::new(left, right);
        
        let negated = or_expr.negate().unwrap();
        assert_eq!(negated.op(), Operation::And);
    }
    
    #[test]
    fn test_not_expression() {
        let child: Arc<dyn Expression> = Arc::new(AlwaysTrue);
        let not_expr = Not::new(child);
        
        assert_eq!(not_expr.op(), Operation::Not);
    }
    
    #[test]
    fn test_not_negate() {
        // not(not(true)) => true
        let child: Arc<dyn Expression> = Arc::new(AlwaysTrue);
        let not_expr = Not::new(child);
        
        let negated = not_expr.negate().unwrap();
        assert_eq!(negated.op(), Operation::True);
    }
    
    #[test]
    fn test_display() {
        assert_eq!(format!("{}", AlwaysTrue), "true");
        assert_eq!(format!("{}", AlwaysFalse), "false");
    }
}
