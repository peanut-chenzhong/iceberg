//! Iceberg Schema types
//!
//! This module defines the schema structure for Iceberg tables, including
//! nested fields and type definitions.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Iceberg primitive type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PrimitiveType {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Date,
    Time,
    Timestamp,
    Timestamptz,
    String,
    Uuid,
    Binary,
    #[serde(rename = "fixed")]
    Fixed(i32),
    #[serde(rename = "decimal")]
    Decimal { precision: i32, scale: i32 },
}

/// Iceberg type (can be primitive or complex)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Type {
    /// Primitive type (e.g., "long", "string")
    Primitive(String),
    /// Struct type with nested fields
    Struct(StructType),
    /// List type
    List(ListType),
    /// Map type
    Map(MapType),
}

impl Type {
    /// Check if this is a primitive type
    pub fn is_primitive(&self) -> bool {
        matches!(self, Type::Primitive(_))
    }

    /// Check if this is a struct type
    pub fn is_struct(&self) -> bool {
        matches!(self, Type::Struct(_))
    }

    /// Check if this is a list type
    pub fn is_list(&self) -> bool {
        matches!(self, Type::List(_))
    }

    /// Check if this is a map type
    pub fn is_map(&self) -> bool {
        matches!(self, Type::Map(_))
    }

    /// Get the type name as a string
    pub fn type_name(&self) -> &str {
        match self {
            Type::Primitive(s) => s,
            Type::Struct(_) => "struct",
            Type::List(_) => "list",
            Type::Map(_) => "map",
        }
    }
}

/// Struct type containing nested fields
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StructType {
    #[serde(rename = "type")]
    pub type_name: String, // Should be "struct"
    pub fields: Vec<NestedField>,
}

/// List type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListType {
    #[serde(rename = "type")]
    pub type_name: String, // Should be "list"
    #[serde(rename = "element-id")]
    pub element_id: i32,
    pub element: Box<Type>,
    #[serde(rename = "element-required")]
    pub element_required: bool,
}

/// Map type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MapType {
    #[serde(rename = "type")]
    pub type_name: String, // Should be "map"
    #[serde(rename = "key-id")]
    pub key_id: i32,
    pub key: Box<Type>,
    #[serde(rename = "value-id")]
    pub value_id: i32,
    pub value: Box<Type>,
    #[serde(rename = "value-required")]
    pub value_required: bool,
}

/// Nested field in a schema
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NestedField {
    /// Field ID
    pub id: i32,
    /// Field name
    pub name: String,
    /// Whether the field is required
    pub required: bool,
    /// Field type
    #[serde(rename = "type")]
    pub field_type: Type,
    /// Optional documentation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// Initial default value (for V3+)
    #[serde(rename = "initial-default", skip_serializing_if = "Option::is_none")]
    pub initial_default: Option<serde_json::Value>,
    /// Write default value (for V3+)
    #[serde(rename = "write-default", skip_serializing_if = "Option::is_none")]
    pub write_default: Option<serde_json::Value>,
}

impl NestedField {
    /// Create a new required field
    pub fn required(id: i32, name: impl Into<String>, field_type: Type) -> Self {
        Self {
            id,
            name: name.into(),
            required: true,
            field_type,
            doc: None,
            initial_default: None,
            write_default: None,
        }
    }

    /// Create a new optional field
    pub fn optional(id: i32, name: impl Into<String>, field_type: Type) -> Self {
        Self {
            id,
            name: name.into(),
            required: false,
            field_type,
            doc: None,
            initial_default: None,
            write_default: None,
        }
    }

    /// Add documentation to the field
    pub fn with_doc(mut self, doc: impl Into<String>) -> Self {
        self.doc = Some(doc.into());
        self
    }
}

/// A simple field definition (used in some contexts)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaField {
    /// Field ID
    pub id: i32,
    /// Field name
    pub name: String,
    /// Type string (e.g., "long", "string")
    #[serde(rename = "type")]
    pub type_name: String,
    /// Whether the field is required
    #[serde(default)]
    pub required: bool,
}

/// Iceberg schema
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    /// Schema ID
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    /// Type (should be "struct")
    #[serde(rename = "type", default = "default_struct_type")]
    pub struct_type: String,
    /// Fields in the schema
    pub fields: Vec<NestedField>,
    /// Identifier field IDs (unique row identifiers)
    #[serde(rename = "identifier-field-ids", default)]
    pub identifier_field_ids: Vec<i32>,
}

fn default_struct_type() -> String {
    "struct".to_string()
}

impl Schema {
    /// Create a new schema
    pub fn new(schema_id: i32, fields: Vec<NestedField>) -> Self {
        Self {
            schema_id,
            struct_type: "struct".to_string(),
            fields,
            identifier_field_ids: Vec::new(),
        }
    }

    /// Create a new schema with identifier field IDs
    pub fn with_identifier_field_ids(mut self, ids: Vec<i32>) -> Self {
        self.identifier_field_ids = ids;
        self
    }

    /// Get a field by ID
    pub fn field(&self, id: i32) -> Option<&NestedField> {
        self.find_field_by_id(&self.fields, id)
    }

    /// Get a field by name
    pub fn field_by_name(&self, name: &str) -> Option<&NestedField> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Get a field by name (case-insensitive)
    pub fn field_by_name_case_insensitive(&self, name: &str) -> Option<&NestedField> {
        let lower = name.to_lowercase();
        self.fields.iter().find(|f| f.name.to_lowercase() == lower)
    }

    /// Get the highest field ID in the schema
    pub fn highest_field_id(&self) -> i32 {
        self.max_field_id(&self.fields)
    }

    /// Build a map of field ID to field
    pub fn fields_by_id(&self) -> HashMap<i32, &NestedField> {
        let mut map = HashMap::new();
        self.collect_fields_by_id(&self.fields, &mut map);
        map
    }

    /// Build a map of field name to field
    pub fn fields_by_name(&self) -> HashMap<&str, &NestedField> {
        self.fields.iter().map(|f| (f.name.as_str(), f)).collect()
    }

    /// Build a map of field name to field ID
    pub fn name_to_id(&self) -> HashMap<String, i32> {
        let mut map = HashMap::new();
        self.collect_name_to_id(&self.fields, "", &mut map);
        map
    }

    /// Build a map of field ID to field name (full path)
    pub fn id_to_name(&self) -> HashMap<i32, String> {
        let mut map = HashMap::new();
        self.collect_id_to_name(&self.fields, "", &mut map);
        map
    }

    /// Create a projection schema selecting only the given field IDs
    ///
    /// This method creates a new schema that contains only the specified fields.
    /// If a parent struct is selected, all its children are included.
    pub fn select(&self, field_ids: &std::collections::HashSet<i32>) -> Schema {
        let selected_fields = self.select_fields(&self.fields, field_ids, true);
        Schema {
            schema_id: self.schema_id,
            struct_type: "struct".to_string(),
            fields: selected_fields,
            identifier_field_ids: self
                .identifier_field_ids
                .iter()
                .filter(|id| field_ids.contains(id))
                .cloned()
                .collect(),
        }
    }

    /// Create a projection schema selecting only the given field IDs (exact)
    ///
    /// Unlike `select`, this method only picks out the exact fields enumerated.
    /// Structs that are explicitly projected are empty unless sub-fields are
    /// explicitly projected.
    pub fn project(&self, field_ids: &std::collections::HashSet<i32>) -> Schema {
        let projected_fields = self.select_fields(&self.fields, field_ids, false);
        Schema {
            schema_id: self.schema_id,
            struct_type: "struct".to_string(),
            fields: projected_fields,
            identifier_field_ids: self
                .identifier_field_ids
                .iter()
                .filter(|id| field_ids.contains(id))
                .cloned()
                .collect(),
        }
    }

    /// Select fields by name
    pub fn select_by_name(&self, names: &[&str], case_sensitive: bool) -> Schema {
        let mut selected_ids = std::collections::HashSet::new();
        for name in names {
            let id = if case_sensitive {
                self.field_by_name(name).map(|f| f.id)
            } else {
                self.field_by_name_case_insensitive(name).map(|f| f.id)
            };
            if let Some(id) = id {
                selected_ids.insert(id);
            }
        }
        self.select(&selected_ids)
    }

    /// Get all field IDs in this schema
    pub fn all_field_ids(&self) -> std::collections::HashSet<i32> {
        let mut ids = std::collections::HashSet::new();
        self.collect_all_field_ids(&self.fields, &mut ids);
        ids
    }

    /// Check if this schema is equivalent to another
    pub fn same_schema(&self, other: &Schema) -> bool {
        self.fields == other.fields && self.identifier_field_ids == other.identifier_field_ids
    }

    /// Check if a field is an identifier field
    pub fn is_identifier_field(&self, field_id: i32) -> bool {
        self.identifier_field_ids.contains(&field_id)
    }

    fn find_field_by_id<'a>(&self, fields: &'a [NestedField], id: i32) -> Option<&'a NestedField> {
        for field in fields {
            if field.id == id {
                return Some(field);
            }
            // Search nested fields
            match &field.field_type {
                Type::Struct(s) => {
                    if let Some(f) = self.find_field_by_id(&s.fields, id) {
                        return Some(f);
                    }
                }
                Type::List(l) => {
                    if l.element_id == id {
                        // Can't return element directly, but note it exists
                    }
                    if let Type::Struct(s) = l.element.as_ref() {
                        if let Some(f) = self.find_field_by_id(&s.fields, id) {
                            return Some(f);
                        }
                    }
                }
                Type::Map(m) => {
                    if m.key_id == id || m.value_id == id {
                        // Can't return key/value directly, but note it exists
                    }
                    if let Type::Struct(s) = m.value.as_ref() {
                        if let Some(f) = self.find_field_by_id(&s.fields, id) {
                            return Some(f);
                        }
                    }
                }
                _ => {}
            }
        }
        None
    }

    fn max_field_id(&self, fields: &[NestedField]) -> i32 {
        let mut max = 0;
        for field in fields {
            max = max.max(field.id);
            match &field.field_type {
                Type::Struct(s) => max = max.max(self.max_field_id(&s.fields)),
                Type::List(l) => {
                    max = max.max(l.element_id);
                    if let Type::Struct(s) = l.element.as_ref() {
                        max = max.max(self.max_field_id(&s.fields));
                    }
                }
                Type::Map(m) => {
                    max = max.max(m.key_id).max(m.value_id);
                    if let Type::Struct(s) = m.value.as_ref() {
                        max = max.max(self.max_field_id(&s.fields));
                    }
                }
                _ => {}
            }
        }
        max
    }

    fn collect_fields_by_id<'a>(
        &self,
        fields: &'a [NestedField],
        map: &mut HashMap<i32, &'a NestedField>,
    ) {
        for field in fields {
            map.insert(field.id, field);
            if let Type::Struct(ref s) = field.field_type {
                self.collect_fields_by_id(&s.fields, map);
            }
        }
    }

    fn collect_name_to_id(&self, fields: &[NestedField], prefix: &str, map: &mut HashMap<String, i32>) {
        for field in fields {
            let full_name = if prefix.is_empty() {
                field.name.clone()
            } else {
                format!("{}.{}", prefix, field.name)
            };
            map.insert(full_name.clone(), field.id);

            match &field.field_type {
                Type::Struct(s) => {
                    self.collect_name_to_id(&s.fields, &full_name, map);
                }
                Type::List(l) => {
                    let element_name = format!("{}.element", full_name);
                    map.insert(element_name.clone(), l.element_id);
                    if let Type::Struct(s) = l.element.as_ref() {
                        self.collect_name_to_id(&s.fields, &element_name, map);
                    }
                }
                Type::Map(m) => {
                    let key_name = format!("{}.key", full_name);
                    let value_name = format!("{}.value", full_name);
                    map.insert(key_name, m.key_id);
                    map.insert(value_name.clone(), m.value_id);
                    if let Type::Struct(s) = m.value.as_ref() {
                        self.collect_name_to_id(&s.fields, &value_name, map);
                    }
                }
                _ => {}
            }
        }
    }

    fn collect_id_to_name(&self, fields: &[NestedField], prefix: &str, map: &mut HashMap<i32, String>) {
        for field in fields {
            let full_name = if prefix.is_empty() {
                field.name.clone()
            } else {
                format!("{}.{}", prefix, field.name)
            };
            map.insert(field.id, full_name.clone());

            match &field.field_type {
                Type::Struct(s) => {
                    self.collect_id_to_name(&s.fields, &full_name, map);
                }
                Type::List(l) => {
                    let element_name = format!("{}.element", full_name);
                    map.insert(l.element_id, element_name.clone());
                    if let Type::Struct(s) = l.element.as_ref() {
                        self.collect_id_to_name(&s.fields, &element_name, map);
                    }
                }
                Type::Map(m) => {
                    let key_name = format!("{}.key", full_name);
                    let value_name = format!("{}.value", full_name);
                    map.insert(m.key_id, key_name);
                    map.insert(m.value_id, value_name.clone());
                    if let Type::Struct(s) = m.value.as_ref() {
                        self.collect_id_to_name(&s.fields, &value_name, map);
                    }
                }
                _ => {}
            }
        }
    }

    fn collect_all_field_ids(&self, fields: &[NestedField], ids: &mut std::collections::HashSet<i32>) {
        for field in fields {
            ids.insert(field.id);
            match &field.field_type {
                Type::Struct(s) => {
                    self.collect_all_field_ids(&s.fields, ids);
                }
                Type::List(l) => {
                    ids.insert(l.element_id);
                    if let Type::Struct(s) = l.element.as_ref() {
                        self.collect_all_field_ids(&s.fields, ids);
                    }
                }
                Type::Map(m) => {
                    ids.insert(m.key_id);
                    ids.insert(m.value_id);
                    if let Type::Struct(s) = m.value.as_ref() {
                        self.collect_all_field_ids(&s.fields, ids);
                    }
                }
                _ => {}
            }
        }
    }

    fn select_fields(
        &self,
        fields: &[NestedField],
        field_ids: &std::collections::HashSet<i32>,
        include_children: bool,
    ) -> Vec<NestedField> {
        let mut selected = Vec::new();

        for field in fields {
            if field_ids.contains(&field.id) {
                if include_children {
                    // Include the whole field (with all nested children)
                    selected.push(field.clone());
                } else {
                    // Project: only include if primitive or recurse into nested
                    match &field.field_type {
                        Type::Primitive(_) => {
                            selected.push(field.clone());
                        }
                        Type::Struct(s) => {
                            let nested = self.select_fields(&s.fields, field_ids, false);
                            selected.push(NestedField {
                                id: field.id,
                                name: field.name.clone(),
                                required: field.required,
                                field_type: Type::Struct(StructType {
                                    type_name: "struct".to_string(),
                                    fields: nested,
                                }),
                                doc: field.doc.clone(),
                                initial_default: field.initial_default.clone(),
                                write_default: field.write_default.clone(),
                            });
                        }
                        Type::List(l) => {
                            // Lists are included as-is when selected
                            selected.push(field.clone());
                        }
                        Type::Map(m) => {
                            // Maps are included as-is when selected
                            selected.push(field.clone());
                        }
                    }
                }
            } else {
                // Field not directly selected, but check if any nested field is selected
                match &field.field_type {
                    Type::Struct(s) => {
                        let nested = self.select_fields(&s.fields, field_ids, include_children);
                        if !nested.is_empty() {
                            selected.push(NestedField {
                                id: field.id,
                                name: field.name.clone(),
                                required: field.required,
                                field_type: Type::Struct(StructType {
                                    type_name: "struct".to_string(),
                                    fields: nested,
                                }),
                                doc: field.doc.clone(),
                                initial_default: field.initial_default.clone(),
                                write_default: field.write_default.clone(),
                            });
                        }
                    }
                    Type::List(l) => {
                        // Check if list element or its children are selected
                        if field_ids.contains(&l.element_id) {
                            selected.push(field.clone());
                        } else if let Type::Struct(s) = l.element.as_ref() {
                            let nested = self.select_fields(&s.fields, field_ids, include_children);
                            if !nested.is_empty() {
                                selected.push(NestedField {
                                    id: field.id,
                                    name: field.name.clone(),
                                    required: field.required,
                                    field_type: Type::List(ListType {
                                        type_name: "list".to_string(),
                                        element_id: l.element_id,
                                        element: Box::new(Type::Struct(StructType {
                                            type_name: "struct".to_string(),
                                            fields: nested,
                                        })),
                                        element_required: l.element_required,
                                    }),
                                    doc: field.doc.clone(),
                                    initial_default: field.initial_default.clone(),
                                    write_default: field.write_default.clone(),
                                });
                            }
                        }
                    }
                    Type::Map(m) => {
                        // Check if map key/value or their children are selected
                        if field_ids.contains(&m.key_id) || field_ids.contains(&m.value_id) {
                            selected.push(field.clone());
                        } else if let Type::Struct(s) = m.value.as_ref() {
                            let nested = self.select_fields(&s.fields, field_ids, include_children);
                            if !nested.is_empty() {
                                selected.push(NestedField {
                                    id: field.id,
                                    name: field.name.clone(),
                                    required: field.required,
                                    field_type: Type::Map(MapType {
                                        type_name: "map".to_string(),
                                        key_id: m.key_id,
                                        key: m.key.clone(),
                                        value_id: m.value_id,
                                        value: Box::new(Type::Struct(StructType {
                                            type_name: "struct".to_string(),
                                            fields: nested,
                                        })),
                                        value_required: m.value_required,
                                    }),
                                    doc: field.doc.clone(),
                                    initial_default: field.initial_default.clone(),
                                    write_default: field.write_default.clone(),
                                });
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        selected
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new(0, Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_schema() {
        let json = r#"
        {
            "schema-id": 0,
            "type": "struct",
            "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {"id": 2, "name": "data", "required": false, "type": "string"}
            ]
        }
        "#;

        let schema: Schema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.schema_id, 0);
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.fields[0].name, "id");
        assert!(schema.fields[0].required);
        assert_eq!(schema.fields[1].name, "data");
        assert!(!schema.fields[1].required);
    }

    #[test]
    fn test_parse_nested_schema() {
        let json = r#"
        {
            "schema-id": 1,
            "type": "struct",
            "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {
                    "id": 2,
                    "name": "location",
                    "required": true,
                    "type": {
                        "type": "struct",
                        "fields": [
                            {"id": 3, "name": "latitude", "required": true, "type": "double"},
                            {"id": 4, "name": "longitude", "required": true, "type": "double"}
                        ]
                    }
                }
            ]
        }
        "#;

        let schema: Schema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.schema_id, 1);
        assert_eq!(schema.fields.len(), 2);

        let location_field = &schema.fields[1];
        assert!(location_field.field_type.is_struct());

        // Test field lookup
        assert!(schema.field(1).is_some());
        assert!(schema.field(3).is_some()); // nested field
        assert!(schema.field(100).is_none());
    }

    #[test]
    fn test_parse_list_and_map() {
        let json = r#"
        {
            "schema-id": 0,
            "type": "struct",
            "fields": [
                {
                    "id": 1,
                    "name": "tags",
                    "required": false,
                    "type": {
                        "type": "list",
                        "element-id": 2,
                        "element": "string",
                        "element-required": false
                    }
                },
                {
                    "id": 3,
                    "name": "properties",
                    "required": false,
                    "type": {
                        "type": "map",
                        "key-id": 4,
                        "key": "string",
                        "value-id": 5,
                        "value": "string",
                        "value-required": false
                    }
                }
            ]
        }
        "#;

        let schema: Schema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.fields.len(), 2);

        assert!(schema.fields[0].field_type.is_list());
        assert!(schema.fields[1].field_type.is_map());
    }

    #[test]
    fn test_highest_field_id() {
        let json = r#"
        {
            "schema-id": 0,
            "type": "struct",
            "fields": [
                {"id": 1, "name": "a", "required": true, "type": "long"},
                {"id": 5, "name": "b", "required": true, "type": "string"},
                {"id": 3, "name": "c", "required": true, "type": "int"}
            ]
        }
        "#;

        let schema: Schema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.highest_field_id(), 5);
    }

    #[test]
    fn test_schema_with_identifier_fields() {
        let json = r#"
        {
            "schema-id": 0,
            "type": "struct",
            "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {"id": 2, "name": "data", "required": false, "type": "string"}
            ],
            "identifier-field-ids": [1]
        }
        "#;

        let schema: Schema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.identifier_field_ids, vec![1]);
    }

    #[test]
    fn test_select_fields() {
        let json = r#"
        {
            "schema-id": 0,
            "type": "struct",
            "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {"id": 2, "name": "data", "required": false, "type": "string"},
                {"id": 3, "name": "category", "required": false, "type": "string"}
            ]
        }
        "#;

        let schema: Schema = serde_json::from_str(json).unwrap();
        
        let mut selected_ids = std::collections::HashSet::new();
        selected_ids.insert(1);
        selected_ids.insert(2);
        
        let projected = schema.select(&selected_ids);
        assert_eq!(projected.fields.len(), 2);
        assert_eq!(projected.fields[0].id, 1);
        assert_eq!(projected.fields[1].id, 2);
    }

    #[test]
    fn test_select_by_name() {
        let json = r#"
        {
            "schema-id": 0,
            "type": "struct",
            "fields": [
                {"id": 1, "name": "ID", "required": true, "type": "long"},
                {"id": 2, "name": "Data", "required": false, "type": "string"},
                {"id": 3, "name": "Category", "required": false, "type": "string"}
            ]
        }
        "#;

        let schema: Schema = serde_json::from_str(json).unwrap();
        
        // Case sensitive - won't find lowercase
        let projected = schema.select_by_name(&["id"], true);
        assert_eq!(projected.fields.len(), 0);
        
        // Case sensitive - exact match
        let projected = schema.select_by_name(&["ID", "Data"], true);
        assert_eq!(projected.fields.len(), 2);
        
        // Case insensitive
        let projected = schema.select_by_name(&["id", "data"], false);
        assert_eq!(projected.fields.len(), 2);
    }

    #[test]
    fn test_select_nested_fields() {
        let json = r#"
        {
            "schema-id": 0,
            "type": "struct",
            "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {
                    "id": 2,
                    "name": "location",
                    "required": true,
                    "type": {
                        "type": "struct",
                        "fields": [
                            {"id": 3, "name": "latitude", "required": true, "type": "double"},
                            {"id": 4, "name": "longitude", "required": true, "type": "double"}
                        ]
                    }
                }
            ]
        }
        "#;

        let schema: Schema = serde_json::from_str(json).unwrap();
        
        // Select only nested latitude field
        let mut selected_ids = std::collections::HashSet::new();
        selected_ids.insert(3); // latitude
        
        let projected = schema.select(&selected_ids);
        assert_eq!(projected.fields.len(), 1); // location struct is included
        assert_eq!(projected.fields[0].name, "location");
        
        if let Type::Struct(ref s) = projected.fields[0].field_type {
            assert_eq!(s.fields.len(), 1);
            assert_eq!(s.fields[0].name, "latitude");
        } else {
            panic!("Expected struct type");
        }
    }

    #[test]
    fn test_name_to_id_mapping() {
        let json = r#"
        {
            "schema-id": 0,
            "type": "struct",
            "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {
                    "id": 2,
                    "name": "location",
                    "required": true,
                    "type": {
                        "type": "struct",
                        "fields": [
                            {"id": 3, "name": "lat", "required": true, "type": "double"},
                            {"id": 4, "name": "lon", "required": true, "type": "double"}
                        ]
                    }
                }
            ]
        }
        "#;

        let schema: Schema = serde_json::from_str(json).unwrap();
        let name_to_id = schema.name_to_id();
        
        assert_eq!(name_to_id.get("id"), Some(&1));
        assert_eq!(name_to_id.get("location"), Some(&2));
        assert_eq!(name_to_id.get("location.lat"), Some(&3));
        assert_eq!(name_to_id.get("location.lon"), Some(&4));
    }

    #[test]
    fn test_all_field_ids() {
        let json = r#"
        {
            "schema-id": 0,
            "type": "struct",
            "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {
                    "id": 2,
                    "name": "tags",
                    "required": false,
                    "type": {
                        "type": "list",
                        "element-id": 3,
                        "element": "string",
                        "element-required": false
                    }
                }
            ]
        }
        "#;

        let schema: Schema = serde_json::from_str(json).unwrap();
        let all_ids = schema.all_field_ids();
        
        assert!(all_ids.contains(&1));
        assert!(all_ids.contains(&2));
        assert!(all_ids.contains(&3)); // list element id
    }

    #[test]
    fn test_same_schema() {
        let schema1 = Schema::new(0, vec![
            NestedField::required(1, "id", Type::Primitive("long".to_string())),
            NestedField::optional(2, "data", Type::Primitive("string".to_string())),
        ]);

        let schema2 = Schema::new(1, vec![
            NestedField::required(1, "id", Type::Primitive("long".to_string())),
            NestedField::optional(2, "data", Type::Primitive("string".to_string())),
        ]);

        // Same fields, different schema ID - should be same
        assert!(schema1.same_schema(&schema2));

        let schema3 = Schema::new(0, vec![
            NestedField::required(1, "id", Type::Primitive("long".to_string())),
        ]);

        // Different fields
        assert!(!schema1.same_schema(&schema3));
    }
}
