//! JNI bindings for Java interoperability
//!
//! This module provides JNI (Java Native Interface) bindings that allow
//! Java code to call into Rust implementations of Iceberg components.
//!
//! ## Usage in Java
//!
//! ```java
//! // Load the native library
//! System.loadLibrary("iceberg_rust_native");
//!
//! // Use the JNI wrapper classes
//! ManifestReaderJNI reader = new ManifestReaderJNI("path/to/manifest.avro");
//! byte[] entries = reader.readDataEntries();
//! reader.close();
//! ```
//!
//! ## Building
//!
//! ```bash
//! cargo build --release --features jni
//! ```

#[cfg(feature = "jni")]
mod error;
#[cfg(feature = "jni")]
mod manifest;
#[cfg(feature = "jni")]
mod expression;
#[cfg(feature = "jni")]
mod delete;
#[cfg(feature = "jni")]
mod util;

#[cfg(feature = "jni")]
pub use error::*;
#[cfg(feature = "jni")]
pub use manifest::*;
#[cfg(feature = "jni")]
pub use expression::*;
#[cfg(feature = "jni")]
pub use delete::*;
