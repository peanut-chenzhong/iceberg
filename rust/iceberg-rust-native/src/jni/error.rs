//! JNI error handling utilities

use jni::JNIEnv;

/// Result type for JNI operations
pub type JniResult<T> = Result<T, JniError>;

/// JNI-specific error type
#[derive(Debug)]
pub enum JniError {
    /// Null pointer passed from Java
    NullPointer(String),
    /// Invalid handle
    InvalidHandle,
    /// String conversion error
    StringConversion(String),
    /// Serialization error
    Serialization(String),
    /// Internal Rust error
    Internal(String),
    /// Java exception occurred
    JavaException,
}

impl std::fmt::Display for JniError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JniError::NullPointer(msg) => write!(f, "Null pointer: {}", msg),
            JniError::InvalidHandle => write!(f, "Invalid native handle"),
            JniError::StringConversion(msg) => write!(f, "String conversion error: {}", msg),
            JniError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            JniError::Internal(msg) => write!(f, "Internal error: {}", msg),
            JniError::JavaException => write!(f, "Java exception occurred"),
        }
    }
}

impl std::error::Error for JniError {}

impl From<crate::error::Error> for JniError {
    fn from(e: crate::error::Error) -> Self {
        JniError::Internal(e.to_string())
    }
}

impl From<serde_json::Error> for JniError {
    fn from(e: serde_json::Error) -> Self {
        JniError::Serialization(e.to_string())
    }
}

impl From<jni::errors::Error> for JniError {
    fn from(e: jni::errors::Error) -> Self {
        JniError::Internal(e.to_string())
    }
}

/// Throw a Java exception from Rust
pub fn throw_java_exception(env: &mut JNIEnv, class: &str, message: &str) {
    let _ = env.throw_new(class, message);
}

/// Throw an IOException
pub fn throw_io_exception(env: &mut JNIEnv, message: &str) {
    throw_java_exception(env, "java/io/IOException", message);
}

/// Throw a RuntimeException
pub fn throw_runtime_exception(env: &mut JNIEnv, message: &str) {
    throw_java_exception(env, "java/lang/RuntimeException", message);
}

/// Throw an IllegalArgumentException
pub fn throw_illegal_argument(env: &mut JNIEnv, message: &str) {
    throw_java_exception(env, "java/lang/IllegalArgumentException", message);
}

/// Throw an IllegalStateException
pub fn throw_illegal_state(env: &mut JNIEnv, message: &str) {
    throw_java_exception(env, "java/lang/IllegalStateException", message);
}
