//! Binder - binds unbound expressions to a schema
//!
//! The Binder takes unbound expressions (referencing columns by name) and
//! binds them to a specific schema, producing bound expressions that can
//! be efficiently evaluated.

use std::collections::HashMap;
use std::sync::Arc;

use super::evaluator::BoundExpressionTree;
use super::literal::Literal;
use super::operation::Operation;
use super::predicate::{BoundPredicate, UnboundPredicate};
use super::reference::BoundReference;
use super::Expression;

/// Schema field information
#[derive(Debug, Clone)]
pub struct SchemaField {
    /// Field ID
    pub field_id: i32,
    /// Field name
    pub name: String,
    /// Field type name
    pub type_name: String,
    /// Field position in struct
    pub position: usize,
    /// Whether the field is required
    pub required: bool,
}

impl SchemaField {
    /// Create a new schema field
    pub fn new(field_id: i32, name: impl Into<String>, type_name: impl Into<String>, position: usize) -> Self {
        SchemaField {
            field_id,
            name: name.into(),
            type_name: type_name.into(),
            position,
            required: false,
        }
    }
    
    /// Set required flag
    pub fn required(mut self, required: bool) -> Self {
        self.required = required;
        self
    }
}

/// Simple schema representation for binding
#[derive(Debug, Clone)]
pub struct Schema {
    /// Fields by name (case-sensitive)
    fields_by_name: HashMap<String, SchemaField>,
    /// Fields by name (case-insensitive)
    fields_by_name_lower: HashMap<String, SchemaField>,
    /// Fields by ID
    fields_by_id: HashMap<i32, SchemaField>,
}

impl Schema {
    /// Create a new schema builder
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }
    
    /// Get a field by name
    pub fn field(&self, name: &str) -> Option<&SchemaField> {
        self.fields_by_name.get(name)
    }
    
    /// Get a field by name (case-insensitive)
    pub fn field_case_insensitive(&self, name: &str) -> Option<&SchemaField> {
        self.fields_by_name_lower.get(&name.to_lowercase())
    }
    
    /// Get a field by ID
    pub fn field_by_id(&self, id: i32) -> Option<&SchemaField> {
        self.fields_by_id.get(&id)
    }
    
    /// Get all field names
    pub fn field_names(&self) -> impl Iterator<Item = &str> {
        self.fields_by_name.keys().map(|s| s.as_str())
    }
}

/// Builder for Schema
#[derive(Debug, Default)]
pub struct SchemaBuilder {
    fields: Vec<SchemaField>,
}

impl SchemaBuilder {
    /// Create a new schema builder
    pub fn new() -> Self {
        SchemaBuilder { fields: Vec::new() }
    }
    
    /// Add a field to the schema
    pub fn field(mut self, field_id: i32, name: impl Into<String>, type_name: impl Into<String>) -> Self {
        let name = name.into();
        let position = self.fields.len();
        self.fields.push(SchemaField::new(field_id, name, type_name, position));
        self
    }
    
    /// Add a required field to the schema
    pub fn required_field(mut self, field_id: i32, name: impl Into<String>, type_name: impl Into<String>) -> Self {
        let name = name.into();
        let position = self.fields.len();
        self.fields.push(SchemaField::new(field_id, name, type_name, position).required(true));
        self
    }
    
    /// Build the schema
    pub fn build(self) -> Schema {
        let mut fields_by_name = HashMap::new();
        let mut fields_by_name_lower = HashMap::new();
        let mut fields_by_id = HashMap::new();
        
        for field in self.fields {
            fields_by_name.insert(field.name.clone(), field.clone());
            fields_by_name_lower.insert(field.name.to_lowercase(), field.clone());
            fields_by_id.insert(field.field_id, field);
        }
        
        Schema {
            fields_by_name,
            fields_by_name_lower,
            fields_by_id,
        }
    }
}

/// Expression binder
#[derive(Debug)]
pub struct Binder {
    /// Schema to bind against
    schema: Schema,
    /// Whether to use case-sensitive matching
    case_sensitive: bool,
}

impl Binder {
    /// Create a new binder for the given schema
    pub fn new(schema: Schema) -> Self {
        Binder {
            schema,
            case_sensitive: true,
        }
    }
    
    /// Set case sensitivity
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }
    
    /// Bind an unbound predicate to the schema
    pub fn bind_predicate(&self, predicate: &UnboundPredicate) -> Result<BoundPredicate, BindError> {
        let field = self.resolve_field(predicate.term().name())?;
        let bound_ref = BoundReference::new(
            field.field_id,
            field.name.clone(),
            field.position,
            field.type_name.clone(),
        );
        Ok(predicate.bind(bound_ref))
    }
    
    /// Bind an unbound predicate to a BoundExpressionTree
    pub fn bind_to_tree(&self, predicate: &UnboundPredicate) -> Result<BoundExpressionTree, BindError> {
        let bound = self.bind_predicate(predicate)?;
        Ok(BoundExpressionTree::predicate(bound))
    }
    
    /// Resolve a field name to a schema field
    fn resolve_field(&self, name: &str) -> Result<&SchemaField, BindError> {
        let field = if self.case_sensitive {
            self.schema.field(name)
        } else {
            self.schema.field_case_insensitive(name)
        };
        
        field.ok_or_else(|| BindError::FieldNotFound(name.to_string()))
    }
}

/// Error during binding
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindError {
    /// Field not found in schema
    FieldNotFound(String),
    /// Type mismatch
    TypeMismatch {
        field: String,
        expected: String,
        actual: String,
    },
    /// Invalid expression
    InvalidExpression(String),
}

impl std::fmt::Display for BindError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BindError::FieldNotFound(name) => write!(f, "Field not found: {}", name),
            BindError::TypeMismatch { field, expected, actual } => {
                write!(f, "Type mismatch for field {}: expected {}, got {}", field, expected, actual)
            }
            BindError::InvalidExpression(msg) => write!(f, "Invalid expression: {}", msg),
        }
    }
}

impl std::error::Error for BindError {}

/// Expression builder that produces bound expression trees
pub struct ExpressionBuilder {
    binder: Binder,
}

impl ExpressionBuilder {
    /// Create a new expression builder
    pub fn new(schema: Schema) -> Self {
        ExpressionBuilder {
            binder: Binder::new(schema),
        }
    }
    
    /// Set case sensitivity
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.binder = self.binder.case_sensitive(case_sensitive);
        self
    }
    
    /// Build an equal expression
    pub fn equal<T: Into<Literal>>(&self, column: &str, value: T) -> Result<BoundExpressionTree, BindError> {
        let pred = UnboundPredicate::literal(
            Operation::Eq,
            super::reference::Reference::new(column),
            value.into(),
        );
        self.binder.bind_to_tree(&pred)
    }
    
    /// Build a not equal expression
    pub fn not_equal<T: Into<Literal>>(&self, column: &str, value: T) -> Result<BoundExpressionTree, BindError> {
        let pred = UnboundPredicate::literal(
            Operation::NotEq,
            super::reference::Reference::new(column),
            value.into(),
        );
        self.binder.bind_to_tree(&pred)
    }
    
    /// Build a less than expression
    pub fn less_than<T: Into<Literal>>(&self, column: &str, value: T) -> Result<BoundExpressionTree, BindError> {
        let pred = UnboundPredicate::literal(
            Operation::Lt,
            super::reference::Reference::new(column),
            value.into(),
        );
        self.binder.bind_to_tree(&pred)
    }
    
    /// Build a less than or equal expression
    pub fn less_than_or_equal<T: Into<Literal>>(&self, column: &str, value: T) -> Result<BoundExpressionTree, BindError> {
        let pred = UnboundPredicate::literal(
            Operation::LtEq,
            super::reference::Reference::new(column),
            value.into(),
        );
        self.binder.bind_to_tree(&pred)
    }
    
    /// Build a greater than expression
    pub fn greater_than<T: Into<Literal>>(&self, column: &str, value: T) -> Result<BoundExpressionTree, BindError> {
        let pred = UnboundPredicate::literal(
            Operation::Gt,
            super::reference::Reference::new(column),
            value.into(),
        );
        self.binder.bind_to_tree(&pred)
    }
    
    /// Build a greater than or equal expression
    pub fn greater_than_or_equal<T: Into<Literal>>(&self, column: &str, value: T) -> Result<BoundExpressionTree, BindError> {
        let pred = UnboundPredicate::literal(
            Operation::GtEq,
            super::reference::Reference::new(column),
            value.into(),
        );
        self.binder.bind_to_tree(&pred)
    }
    
    /// Build an IS NULL expression
    pub fn is_null(&self, column: &str) -> Result<BoundExpressionTree, BindError> {
        let pred = UnboundPredicate::unary(
            Operation::IsNull,
            super::reference::Reference::new(column),
        );
        self.binder.bind_to_tree(&pred)
    }
    
    /// Build a NOT NULL expression
    pub fn not_null(&self, column: &str) -> Result<BoundExpressionTree, BindError> {
        let pred = UnboundPredicate::unary(
            Operation::NotNull,
            super::reference::Reference::new(column),
        );
        self.binder.bind_to_tree(&pred)
    }
    
    /// Build an IN expression
    pub fn in_list<T: Into<Literal>>(&self, column: &str, values: Vec<T>) -> Result<BoundExpressionTree, BindError> {
        let literals: Vec<Literal> = values.into_iter().map(|v| v.into()).collect();
        let pred = UnboundPredicate::set(
            Operation::In,
            super::reference::Reference::new(column),
            literals,
        );
        self.binder.bind_to_tree(&pred)
    }
    
    /// Build a NOT IN expression
    pub fn not_in<T: Into<Literal>>(&self, column: &str, values: Vec<T>) -> Result<BoundExpressionTree, BindError> {
        let literals: Vec<Literal> = values.into_iter().map(|v| v.into()).collect();
        let pred = UnboundPredicate::set(
            Operation::NotIn,
            super::reference::Reference::new(column),
            literals,
        );
        self.binder.bind_to_tree(&pred)
    }
    
    /// Build a STARTS WITH expression
    pub fn starts_with(&self, column: &str, prefix: &str) -> Result<BoundExpressionTree, BindError> {
        let pred = UnboundPredicate::literal(
            Operation::StartsWith,
            super::reference::Reference::new(column),
            Literal::string(prefix),
        );
        self.binder.bind_to_tree(&pred)
    }
    
    /// Build an AND expression
    pub fn and(&self, left: BoundExpressionTree, right: BoundExpressionTree) -> BoundExpressionTree {
        BoundExpressionTree::and(left, right)
    }
    
    /// Build an OR expression
    pub fn or(&self, left: BoundExpressionTree, right: BoundExpressionTree) -> BoundExpressionTree {
        BoundExpressionTree::or(left, right)
    }
    
    /// Build a NOT expression
    pub fn not(&self, child: BoundExpressionTree) -> BoundExpressionTree {
        BoundExpressionTree::not(child)
    }
    
    /// Build an always true expression
    pub fn always_true(&self) -> BoundExpressionTree {
        BoundExpressionTree::always_true()
    }
    
    /// Build an always false expression
    pub fn always_false(&self) -> BoundExpressionTree {
        BoundExpressionTree::always_false()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::accessor::StructAccessor;
    use crate::expr::datum::Datum;
    
    fn create_test_schema() -> Schema {
        Schema::builder()
            .required_field(1, "id", "long")
            .field(2, "name", "string")
            .field(3, "age", "int")
            .field(4, "salary", "double")
            .build()
    }
    
    #[test]
    fn test_schema_builder() {
        let schema = create_test_schema();
        
        let id_field = schema.field("id").unwrap();
        assert_eq!(id_field.field_id, 1);
        assert_eq!(id_field.type_name, "long");
        assert_eq!(id_field.position, 0);
        assert!(id_field.required);
        
        let name_field = schema.field("name").unwrap();
        assert_eq!(name_field.field_id, 2);
        assert_eq!(name_field.position, 1);
        assert!(!name_field.required);
    }
    
    #[test]
    fn test_case_insensitive_lookup() {
        let schema = create_test_schema();
        
        let field = schema.field_case_insensitive("NAME").unwrap();
        assert_eq!(field.name, "name");
        
        let field = schema.field_case_insensitive("Age").unwrap();
        assert_eq!(field.name, "age");
    }
    
    #[test]
    fn test_binder_bind_predicate() {
        let schema = create_test_schema();
        let binder = Binder::new(schema);
        
        let pred = UnboundPredicate::literal(
            Operation::Eq,
            super::super::reference::Reference::new("age"),
            Literal::int(25),
        );
        
        let bound = binder.bind_predicate(&pred).unwrap();
        assert_eq!(bound.term().field_id(), 3);
        assert_eq!(bound.term().pos(), 2);
    }
    
    #[test]
    fn test_binder_field_not_found() {
        let schema = create_test_schema();
        let binder = Binder::new(schema);
        
        let pred = UnboundPredicate::literal(
            Operation::Eq,
            super::super::reference::Reference::new("unknown_field"),
            Literal::int(25),
        );
        
        let result = binder.bind_predicate(&pred);
        assert!(matches!(result, Err(BindError::FieldNotFound(_))));
    }
    
    #[test]
    fn test_expression_builder() {
        let schema = create_test_schema();
        let builder = ExpressionBuilder::new(schema);
        
        // Build: age >= 18 AND age < 65
        let expr = builder.and(
            builder.greater_than_or_equal("age", 18i32).unwrap(),
            builder.less_than("age", 65i32).unwrap(),
        );
        
        // Test with age = 30
        let accessor = StructAccessor::new(vec![
            Datum::long(1),      // id
            Datum::string("John"), // name
            Datum::int(30),      // age
            Datum::double(50000.0), // salary
        ]);
        assert!(expr.eval(&accessor));
        
        // Test with age = 10 (too young)
        let accessor = StructAccessor::new(vec![
            Datum::long(2),
            Datum::string("Child"),
            Datum::int(10),
            Datum::double(0.0),
        ]);
        assert!(!expr.eval(&accessor));
        
        // Test with age = 70 (too old)
        let accessor = StructAccessor::new(vec![
            Datum::long(3),
            Datum::string("Senior"),
            Datum::int(70),
            Datum::double(30000.0),
        ]);
        assert!(!expr.eval(&accessor));
    }
    
    #[test]
    fn test_expression_builder_in_list() {
        let schema = create_test_schema();
        let builder = ExpressionBuilder::new(schema);
        
        // Build: age IN (25, 30, 35)
        let expr = builder.in_list("age", vec![25i32, 30, 35]).unwrap();
        
        let accessor = StructAccessor::new(vec![
            Datum::long(1),
            Datum::string("John"),
            Datum::int(30),
            Datum::double(50000.0),
        ]);
        assert!(expr.eval(&accessor));
        
        let accessor = StructAccessor::new(vec![
            Datum::long(2),
            Datum::string("Jane"),
            Datum::int(28),
            Datum::double(55000.0),
        ]);
        assert!(!expr.eval(&accessor));
    }
    
    #[test]
    fn test_expression_builder_is_null() {
        let schema = create_test_schema();
        let builder = ExpressionBuilder::new(schema);
        
        // Build: name IS NULL
        let expr = builder.is_null("name").unwrap();
        
        let accessor = StructAccessor::new(vec![
            Datum::long(1),
            Datum::Null,
            Datum::int(30),
            Datum::double(50000.0),
        ]);
        assert!(expr.eval(&accessor));
        
        let accessor = StructAccessor::new(vec![
            Datum::long(1),
            Datum::string("John"),
            Datum::int(30),
            Datum::double(50000.0),
        ]);
        assert!(!expr.eval(&accessor));
    }
    
    #[test]
    fn test_expression_builder_starts_with() {
        let schema = create_test_schema();
        let builder = ExpressionBuilder::new(schema);
        
        // Build: name STARTS WITH "John"
        let expr = builder.starts_with("name", "John").unwrap();
        
        let accessor = StructAccessor::new(vec![
            Datum::long(1),
            Datum::string("John Doe"),
            Datum::int(30),
            Datum::double(50000.0),
        ]);
        assert!(expr.eval(&accessor));
        
        let accessor = StructAccessor::new(vec![
            Datum::long(2),
            Datum::string("Jane Doe"),
            Datum::int(28),
            Datum::double(55000.0),
        ]);
        assert!(!expr.eval(&accessor));
    }
    
    #[test]
    fn test_complex_expression() {
        let schema = create_test_schema();
        let builder = ExpressionBuilder::new(schema);
        
        // Build: (age >= 18 AND salary > 40000) OR name STARTS WITH "VIP"
        let expr = builder.or(
            builder.and(
                builder.greater_than_or_equal("age", 18i32).unwrap(),
                builder.greater_than("salary", 40000.0f64).unwrap(),
            ),
            builder.starts_with("name", "VIP").unwrap(),
        );
        
        // Adult with good salary - matches first part
        let accessor = StructAccessor::new(vec![
            Datum::long(1),
            Datum::string("John"),
            Datum::int(30),
            Datum::double(50000.0),
        ]);
        assert!(expr.eval(&accessor));
        
        // VIP member regardless of age/salary - matches second part
        let accessor = StructAccessor::new(vec![
            Datum::long(2),
            Datum::string("VIP Customer"),
            Datum::int(10),
            Datum::double(0.0),
        ]);
        assert!(expr.eval(&accessor));
        
        // Young non-VIP with low salary - doesn't match
        let accessor = StructAccessor::new(vec![
            Datum::long(3),
            Datum::string("Child"),
            Datum::int(10),
            Datum::double(0.0),
        ]);
        assert!(!expr.eval(&accessor));
    }
}
