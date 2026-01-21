//! JNI utility functions

use jni::objects::{JByteArray, JObject, JString};
use jni::sys::{jbyteArray, jlong};
use jni::JNIEnv;

use super::error::{JniError, JniResult};

/// Convert a Java String to Rust String
pub fn jstring_to_string(env: &mut JNIEnv, jstr: &JString) -> JniResult<String> {
    env.get_string(jstr)
        .map(|s| s.into())
        .map_err(|e| JniError::StringConversion(e.to_string()))
}

/// Convert a Rust String to Java String
pub fn string_to_jstring<'a>(env: &mut JNIEnv<'a>, s: &str) -> JniResult<JString<'a>> {
    env.new_string(s)
        .map_err(|e| JniError::StringConversion(e.to_string()))
}

/// Convert a Java byte array to Rust Vec<u8>
pub fn jbytes_to_vec(env: &mut JNIEnv, bytes: &JByteArray) -> JniResult<Vec<u8>> {
    env.convert_byte_array(bytes)
        .map_err(|e| JniError::Internal(e.to_string()))
}

/// Convert a Rust Vec<u8> to Java byte array
pub fn vec_to_jbytes<'a>(env: &mut JNIEnv<'a>, bytes: &[u8]) -> JniResult<JByteArray<'a>> {
    env.byte_array_from_slice(bytes)
        .map_err(|e| JniError::Internal(e.to_string()))
}

/// Box a value and return its raw pointer as jlong
pub fn box_to_handle<T>(value: T) -> jlong {
    Box::into_raw(Box::new(value)) as jlong
}

/// Get a reference from a handle
///
/// # Safety
/// The handle must be a valid pointer to a T
pub unsafe fn handle_to_ref<'a, T>(handle: jlong) -> JniResult<&'a T> {
    if handle == 0 {
        return Err(JniError::InvalidHandle);
    }
    Ok(&*(handle as *const T))
}

/// Get a mutable reference from a handle
///
/// # Safety
/// The handle must be a valid pointer to a T
pub unsafe fn handle_to_mut<'a, T>(handle: jlong) -> JniResult<&'a mut T> {
    if handle == 0 {
        return Err(JniError::InvalidHandle);
    }
    Ok(&mut *(handle as *mut T))
}

/// Drop a boxed value from a handle
///
/// # Safety
/// The handle must be a valid pointer to a T that was created with box_to_handle
pub unsafe fn drop_handle<T>(handle: jlong) {
    if handle != 0 {
        let _ = Box::from_raw(handle as *mut T);
    }
}

/// Check if a Java object is null
pub fn is_null(obj: &JObject) -> bool {
    obj.is_null()
}

/// Serialize a value to JSON bytes
pub fn serialize_to_json<T: serde::Serialize>(value: &T) -> JniResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| JniError::Serialization(e.to_string()))
}

/// Deserialize JSON bytes to a value
pub fn deserialize_from_json<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> JniResult<T> {
    serde_json::from_slice(bytes).map_err(|e| JniError::Serialization(e.to_string()))
}

/// Native handle wrapper for safe memory management
#[repr(transparent)]
pub struct NativeHandle {
    handle: jlong,
}

impl NativeHandle {
    /// Create a new handle from a jlong
    pub fn new(handle: jlong) -> Self {
        NativeHandle { handle }
    }

    /// Check if the handle is valid
    pub fn is_valid(&self) -> bool {
        self.handle != 0
    }

    /// Get the raw handle value
    pub fn raw(&self) -> jlong {
        self.handle
    }
}
