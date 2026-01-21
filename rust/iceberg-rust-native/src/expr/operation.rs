//! Expression operations

use std::fmt;

/// All operations supported by Iceberg expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operation {
    // Constants
    /// Always true
    True,
    /// Always false
    False,
    
    // Unary predicates
    /// IS NULL
    IsNull,
    /// IS NOT NULL
    NotNull,
    /// IS NaN
    IsNaN,
    /// IS NOT NaN
    NotNaN,
    
    // Comparison predicates
    /// Less than (<)
    Lt,
    /// Less than or equal (<=)
    LtEq,
    /// Greater than (>)
    Gt,
    /// Greater than or equal (>=)
    GtEq,
    /// Equal (=)
    Eq,
    /// Not equal (!=)
    NotEq,
    
    // Set predicates
    /// IN (value list)
    In,
    /// NOT IN (value list)
    NotIn,
    
    // Logical operators
    /// Logical NOT
    Not,
    /// Logical AND
    And,
    /// Logical OR
    Or,
    
    // String predicates
    /// STARTS WITH
    StartsWith,
    /// NOT STARTS WITH
    NotStartsWith,
    
    // Aggregate operations (for completeness)
    /// COUNT
    Count,
    /// COUNT NULL
    CountNull,
    /// COUNT(*)
    CountStar,
    /// MAX
    Max,
    /// MIN
    Min,
}

impl Operation {
    /// Returns the negation of this operation
    /// 
    /// # Example
    /// ```
    /// use iceberg_rust_native::expr::Operation;
    /// 
    /// assert_eq!(Operation::IsNull.negate(), Some(Operation::NotNull));
    /// assert_eq!(Operation::Lt.negate(), Some(Operation::GtEq));
    /// assert_eq!(Operation::And.negate(), None);
    /// ```
    pub fn negate(&self) -> Option<Operation> {
        match self {
            Operation::IsNull => Some(Operation::NotNull),
            Operation::NotNull => Some(Operation::IsNull),
            Operation::IsNaN => Some(Operation::NotNaN),
            Operation::NotNaN => Some(Operation::IsNaN),
            Operation::Lt => Some(Operation::GtEq),
            Operation::LtEq => Some(Operation::Gt),
            Operation::Gt => Some(Operation::LtEq),
            Operation::GtEq => Some(Operation::Lt),
            Operation::Eq => Some(Operation::NotEq),
            Operation::NotEq => Some(Operation::Eq),
            Operation::In => Some(Operation::NotIn),
            Operation::NotIn => Some(Operation::In),
            Operation::StartsWith => Some(Operation::NotStartsWith),
            Operation::NotStartsWith => Some(Operation::StartsWith),
            _ => None,
        }
    }
    
    /// Returns the equivalent operation when left and right operands are exchanged
    /// 
    /// # Example
    /// ```
    /// use iceberg_rust_native::expr::Operation;
    /// 
    /// assert_eq!(Operation::Lt.flip_lr(), Some(Operation::Gt));
    /// assert_eq!(Operation::Eq.flip_lr(), Some(Operation::Eq));
    /// ```
    pub fn flip_lr(&self) -> Option<Operation> {
        match self {
            Operation::Lt => Some(Operation::Gt),
            Operation::LtEq => Some(Operation::GtEq),
            Operation::Gt => Some(Operation::Lt),
            Operation::GtEq => Some(Operation::LtEq),
            Operation::Eq => Some(Operation::Eq),
            Operation::NotEq => Some(Operation::NotEq),
            Operation::And => Some(Operation::And),
            Operation::Or => Some(Operation::Or),
            _ => None,
        }
    }
    
    /// Check if this is a unary operation
    pub fn is_unary(&self) -> bool {
        matches!(self, 
            Operation::IsNull | Operation::NotNull | 
            Operation::IsNaN | Operation::NotNaN |
            Operation::Not
        )
    }
    
    /// Check if this is a comparison operation
    pub fn is_comparison(&self) -> bool {
        matches!(self,
            Operation::Lt | Operation::LtEq |
            Operation::Gt | Operation::GtEq |
            Operation::Eq | Operation::NotEq
        )
    }
    
    /// Check if this is a set operation
    pub fn is_set(&self) -> bool {
        matches!(self, Operation::In | Operation::NotIn)
    }
    
    /// Check if this is a logical operation
    pub fn is_logical(&self) -> bool {
        matches!(self, Operation::And | Operation::Or | Operation::Not)
    }
    
    /// Parse from string
    pub fn from_str(s: &str) -> Option<Operation> {
        match s.to_uppercase().as_str() {
            "TRUE" => Some(Operation::True),
            "FALSE" => Some(Operation::False),
            "IS_NULL" | "ISNULL" => Some(Operation::IsNull),
            "NOT_NULL" | "NOTNULL" | "IS_NOT_NULL" => Some(Operation::NotNull),
            "IS_NAN" | "ISNAN" => Some(Operation::IsNaN),
            "NOT_NAN" | "NOTNAN" | "IS_NOT_NAN" => Some(Operation::NotNaN),
            "LT" | "<" => Some(Operation::Lt),
            "LT_EQ" | "LTEQ" | "<=" => Some(Operation::LtEq),
            "GT" | ">" => Some(Operation::Gt),
            "GT_EQ" | "GTEQ" | ">=" => Some(Operation::GtEq),
            "EQ" | "=" | "==" => Some(Operation::Eq),
            "NOT_EQ" | "NOTEQ" | "!=" | "<>" => Some(Operation::NotEq),
            "IN" => Some(Operation::In),
            "NOT_IN" | "NOTIN" => Some(Operation::NotIn),
            "NOT" => Some(Operation::Not),
            "AND" | "&&" => Some(Operation::And),
            "OR" | "||" => Some(Operation::Or),
            "STARTS_WITH" | "STARTSWITH" => Some(Operation::StartsWith),
            "NOT_STARTS_WITH" | "NOTSTARTSWITH" => Some(Operation::NotStartsWith),
            _ => None,
        }
    }
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Operation::True => write!(f, "true"),
            Operation::False => write!(f, "false"),
            Operation::IsNull => write!(f, "is_null"),
            Operation::NotNull => write!(f, "not_null"),
            Operation::IsNaN => write!(f, "is_nan"),
            Operation::NotNaN => write!(f, "not_nan"),
            Operation::Lt => write!(f, "<"),
            Operation::LtEq => write!(f, "<="),
            Operation::Gt => write!(f, ">"),
            Operation::GtEq => write!(f, ">="),
            Operation::Eq => write!(f, "="),
            Operation::NotEq => write!(f, "!="),
            Operation::In => write!(f, "in"),
            Operation::NotIn => write!(f, "not_in"),
            Operation::Not => write!(f, "not"),
            Operation::And => write!(f, "and"),
            Operation::Or => write!(f, "or"),
            Operation::StartsWith => write!(f, "starts_with"),
            Operation::NotStartsWith => write!(f, "not_starts_with"),
            Operation::Count => write!(f, "count"),
            Operation::CountNull => write!(f, "count_null"),
            Operation::CountStar => write!(f, "count(*)"),
            Operation::Max => write!(f, "max"),
            Operation::Min => write!(f, "min"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_negate() {
        assert_eq!(Operation::IsNull.negate(), Some(Operation::NotNull));
        assert_eq!(Operation::NotNull.negate(), Some(Operation::IsNull));
        assert_eq!(Operation::Lt.negate(), Some(Operation::GtEq));
        assert_eq!(Operation::LtEq.negate(), Some(Operation::Gt));
        assert_eq!(Operation::Gt.negate(), Some(Operation::LtEq));
        assert_eq!(Operation::GtEq.negate(), Some(Operation::Lt));
        assert_eq!(Operation::Eq.negate(), Some(Operation::NotEq));
        assert_eq!(Operation::NotEq.negate(), Some(Operation::Eq));
        assert_eq!(Operation::In.negate(), Some(Operation::NotIn));
        assert_eq!(Operation::NotIn.negate(), Some(Operation::In));
        assert_eq!(Operation::And.negate(), None);
        assert_eq!(Operation::Or.negate(), None);
    }
    
    #[test]
    fn test_flip_lr() {
        assert_eq!(Operation::Lt.flip_lr(), Some(Operation::Gt));
        assert_eq!(Operation::LtEq.flip_lr(), Some(Operation::GtEq));
        assert_eq!(Operation::Gt.flip_lr(), Some(Operation::Lt));
        assert_eq!(Operation::GtEq.flip_lr(), Some(Operation::LtEq));
        assert_eq!(Operation::Eq.flip_lr(), Some(Operation::Eq));
        assert_eq!(Operation::NotEq.flip_lr(), Some(Operation::NotEq));
    }
    
    #[test]
    fn test_from_str() {
        assert_eq!(Operation::from_str("LT"), Some(Operation::Lt));
        assert_eq!(Operation::from_str("<"), Some(Operation::Lt));
        assert_eq!(Operation::from_str("EQ"), Some(Operation::Eq));
        assert_eq!(Operation::from_str("="), Some(Operation::Eq));
        assert_eq!(Operation::from_str("AND"), Some(Operation::And));
        assert_eq!(Operation::from_str("UNKNOWN"), None);
    }
    
    #[test]
    fn test_is_unary() {
        assert!(Operation::IsNull.is_unary());
        assert!(Operation::NotNull.is_unary());
        assert!(Operation::Not.is_unary());
        assert!(!Operation::Eq.is_unary());
        assert!(!Operation::And.is_unary());
    }
    
    #[test]
    fn test_is_comparison() {
        assert!(Operation::Lt.is_comparison());
        assert!(Operation::Eq.is_comparison());
        assert!(!Operation::IsNull.is_comparison());
        assert!(!Operation::In.is_comparison());
    }
    
    #[test]
    fn test_is_set() {
        assert!(Operation::In.is_set());
        assert!(Operation::NotIn.is_set());
        assert!(!Operation::Eq.is_set());
    }
}
