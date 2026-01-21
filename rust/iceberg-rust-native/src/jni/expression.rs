//! JNI bindings for Expression evaluation
//!
//! Java class: `org.apache.iceberg.rust.ExpressionJNI`
//!
//! Note: This is a simplified implementation. Full expression serialization
//! requires additional work on the Rust Expression types.

use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jlong, JNI_FALSE};
use jni::JNIEnv;

use super::error::throw_runtime_exception;
use super::util::{box_to_handle, drop_handle, jstring_to_string};

// =============================================================================
// Schema JNI Functions
// =============================================================================

/// Schema field storage
#[derive(Clone)]
struct SchemaFieldData {
    id: i32,
    name: String,
    type_name: String,
    required: bool,
}

/// Schema builder storage
struct SchemaBuilderData {
    fields: Vec<SchemaFieldData>,
}

/// Built schema storage
struct SchemaData {
    fields: Vec<SchemaFieldData>,
}

/// Create a new Schema builder
///
/// Java signature: `native long nativeCreate()`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_SchemaJNI_nativeCreate(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let builder = SchemaBuilderData { fields: Vec::new() };
    box_to_handle(builder)
}

/// Add a field to the schema builder
///
/// Java signature: `native void nativeAddField(long handle, int id, String name, String type, boolean required)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_SchemaJNI_nativeAddField(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    id: jlong,
    name: JString,
    field_type: JString,
    required: jboolean,
) {
    let name_str = match jstring_to_string(&mut env, &name) {
        Ok(s) => s,
        Err(e) => {
            throw_runtime_exception(&mut env, &e.to_string());
            return;
        }
    };

    let type_str = match jstring_to_string(&mut env, &field_type) {
        Ok(s) => s,
        Err(e) => {
            throw_runtime_exception(&mut env, &e.to_string());
            return;
        }
    };

    let builder = unsafe {
        if handle == 0 {
            throw_runtime_exception(&mut env, "Invalid handle");
            return;
        }
        &mut *(handle as *mut SchemaBuilderData)
    };

    builder.fields.push(SchemaFieldData {
        id: id as i32,
        name: name_str,
        type_name: type_str,
        required: required != JNI_FALSE,
    });
}

/// Build the schema
///
/// Java signature: `native long nativeBuild(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_SchemaJNI_nativeBuild(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jlong {
    if handle == 0 {
        throw_runtime_exception(&mut env, "Invalid handle");
        return 0;
    }

    // Take ownership of the builder
    let builder = unsafe { Box::from_raw(handle as *mut SchemaBuilderData) };

    let schema = SchemaData {
        fields: builder.fields,
    };
    box_to_handle(schema)
}

/// Close the schema
///
/// Java signature: `native void nativeClose(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_SchemaJNI_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    unsafe {
        drop_handle::<SchemaData>(handle);
    }
}

// =============================================================================
// Expression Builder JNI Functions (Simplified)
// =============================================================================

/// Expression builder storage
struct ExpressionBuilderData {
    schema: SchemaData,
}

/// Create a new Expression builder
///
/// Java signature: `native long nativeCreate(long schemaHandle)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_ExpressionBuilderJNI_nativeCreate(
    mut env: JNIEnv,
    _class: JClass,
    schema_handle: jlong,
) -> jlong {
    if schema_handle == 0 {
        throw_runtime_exception(&mut env, "Invalid schema handle");
        return 0;
    }

    let schema = unsafe { &*(schema_handle as *const SchemaData) };

    let builder = ExpressionBuilderData {
        schema: SchemaData {
            fields: schema.fields.clone(),
        },
    };
    box_to_handle(builder)
}

/// Close the expression builder
///
/// Java signature: `native void nativeClose(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_ExpressionBuilderJNI_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    unsafe {
        drop_handle::<ExpressionBuilderData>(handle);
    }
}

// Note: Full expression building and evaluation JNI functions require
// adding Serialize/Deserialize derives to BoundExpressionTree and related types.
// This is a TODO for the complete JNI implementation.
