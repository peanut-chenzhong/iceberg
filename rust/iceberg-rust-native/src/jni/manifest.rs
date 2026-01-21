//! JNI bindings for ManifestReader
//!
//! Java class: `org.apache.iceberg.rust.ManifestReaderJNI`

use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jbyteArray, jint, jlong, JNI_FALSE};
use jni::JNIEnv;
use std::fs::File;
use std::io::BufReader;

use crate::manifest::{ContentFile, ManifestReaderBuilder};

use super::error::{throw_io_exception, throw_runtime_exception};
use super::util::{
    box_to_handle, drop_handle, handle_to_ref, jstring_to_string,
    serialize_to_json, vec_to_jbytes,
};

/// Native handle for ManifestReader - store the path for re-creating
type ManifestReaderConfig = ManifestReaderConfigData;

#[derive(Clone)]
struct ManifestReaderConfigData {
    path: String,
    first_row_id: i64,
    include_deleted: bool,
    keep_stats: bool,
}

/// Manifest entry data for serialization
#[derive(serde::Serialize, serde::Deserialize)]
struct ManifestEntryData {
    status: i32,
    snapshot_id: Option<i64>,
    data_sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
    file_path: String,
    file_format: String,
    spec_id: i32,
    record_count: i64,
    file_size_in_bytes: i64,
    first_row_id: Option<i64>,
    content: i32,
}

// =============================================================================
// JNI Functions
// =============================================================================

/// Create a new ManifestReader
///
/// Java signature: `native long nativeOpen(String path)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_ManifestReaderJNI_nativeOpen(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
) -> jlong {
    let path_str = match jstring_to_string(&mut env, &path) {
        Ok(s) => s,
        Err(e) => {
            throw_io_exception(&mut env, &e.to_string());
            return 0;
        }
    };

    // Verify the file can be opened
    match ManifestReaderBuilder::new().open(&path_str) {
        Ok(_) => {
            let config = ManifestReaderConfigData {
                path: path_str,
                first_row_id: 0,
                include_deleted: false,
                keep_stats: true,
            };
            box_to_handle(config)
        }
        Err(e) => {
            throw_io_exception(&mut env, &format!("Failed to open manifest: {}", e));
            0
        }
    }
}

/// Create a ManifestReader with configuration
///
/// Java signature: `native long nativeOpenWithConfig(String path, long firstRowId, boolean includeDeleted, boolean keepStats)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_ManifestReaderJNI_nativeOpenWithConfig(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    first_row_id: jlong,
    include_deleted: jboolean,
    keep_stats: jboolean,
) -> jlong {
    let path_str = match jstring_to_string(&mut env, &path) {
        Ok(s) => s,
        Err(e) => {
            throw_io_exception(&mut env, &e.to_string());
            return 0;
        }
    };

    let builder = ManifestReaderBuilder::new()
        .first_row_id(first_row_id)
        .include_deleted(include_deleted != JNI_FALSE)
        .keep_stats(keep_stats != JNI_FALSE);

    match builder.open(&path_str) {
        Ok(_) => {
            let config = ManifestReaderConfigData {
                path: path_str,
                first_row_id,
                include_deleted: include_deleted != JNI_FALSE,
                keep_stats: keep_stats != JNI_FALSE,
            };
            box_to_handle(config)
        }
        Err(e) => {
            throw_io_exception(&mut env, &format!("Failed to open manifest: {}", e));
            0
        }
    }
}

/// Read data file entries as JSON bytes
///
/// Java signature: `native byte[] nativeReadDataEntries(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_ManifestReaderJNI_nativeReadDataEntries(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jbyteArray {
    let config = match unsafe { handle_to_ref::<ManifestReaderConfig>(handle) } {
        Ok(c) => c.clone(),
        Err(e) => {
            throw_runtime_exception(&mut env, &e.to_string());
            return std::ptr::null_mut();
        }
    };

    let reader = ManifestReaderBuilder::new()
        .first_row_id(config.first_row_id)
        .include_deleted(config.include_deleted)
        .keep_stats(config.keep_stats);

    let reader = match reader.open(&config.path) {
        Ok(r) => r,
        Err(e) => {
            throw_io_exception(&mut env, &e.to_string());
            return std::ptr::null_mut();
        }
    };

    match reader.read_data_entries() {
        Ok(entries) => {
            let entry_data: Vec<ManifestEntryData> = entries
                .iter()
                .map(|e| ManifestEntryData {
                    status: e.status() as i32,
                    snapshot_id: e.snapshot_id(),
                    data_sequence_number: e.data_sequence_number(),
                    file_sequence_number: e.file_sequence_number(),
                    file_path: e.file().file_path().to_string(),
                    file_format: format!("{:?}", e.file().file_format()),
                    spec_id: e.file().spec_id(),
                    record_count: e.file().record_count(),
                    file_size_in_bytes: e.file().file_size_in_bytes(),
                    first_row_id: e.file().first_row_id(),
                    content: e.file().content() as i32,
                })
                .collect();

            match serialize_to_json(&entry_data) {
                Ok(bytes) => match vec_to_jbytes(&mut env, &bytes) {
                    Ok(arr) => arr.into_raw(),
                    Err(e) => {
                        throw_runtime_exception(&mut env, &e.to_string());
                        std::ptr::null_mut()
                    }
                },
                Err(e) => {
                    throw_runtime_exception(&mut env, &e.to_string());
                    std::ptr::null_mut()
                }
            }
        }
        Err(e) => {
            throw_io_exception(&mut env, &e.to_string());
            std::ptr::null_mut()
        }
    }
}

/// Read delete file entries as JSON bytes
///
/// Java signature: `native byte[] nativeReadDeleteEntries(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_ManifestReaderJNI_nativeReadDeleteEntries(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jbyteArray {
    let config = match unsafe { handle_to_ref::<ManifestReaderConfig>(handle) } {
        Ok(c) => c.clone(),
        Err(e) => {
            throw_runtime_exception(&mut env, &e.to_string());
            return std::ptr::null_mut();
        }
    };

    let reader = ManifestReaderBuilder::new()
        .first_row_id(config.first_row_id)
        .include_deleted(config.include_deleted)
        .keep_stats(config.keep_stats);

    let reader = match reader.open(&config.path) {
        Ok(r) => r,
        Err(e) => {
            throw_io_exception(&mut env, &e.to_string());
            return std::ptr::null_mut();
        }
    };

    match reader.read_delete_entries() {
        Ok(entries) => {
            let entry_data: Vec<ManifestEntryData> = entries
                .iter()
                .map(|e| ManifestEntryData {
                    status: e.status() as i32,
                    snapshot_id: e.snapshot_id(),
                    data_sequence_number: e.data_sequence_number(),
                    file_sequence_number: e.file_sequence_number(),
                    file_path: e.file().file_path().to_string(),
                    file_format: format!("{:?}", e.file().file_format()),
                    spec_id: e.file().spec_id(),
                    record_count: e.file().record_count(),
                    file_size_in_bytes: e.file().file_size_in_bytes(),
                    first_row_id: e.file().first_row_id(),
                    content: e.file().content() as i32,
                })
                .collect();

            match serialize_to_json(&entry_data) {
                Ok(bytes) => match vec_to_jbytes(&mut env, &bytes) {
                    Ok(arr) => arr.into_raw(),
                    Err(e) => {
                        throw_runtime_exception(&mut env, &e.to_string());
                        std::ptr::null_mut()
                    }
                },
                Err(e) => {
                    throw_runtime_exception(&mut env, &e.to_string());
                    std::ptr::null_mut()
                }
            }
        }
        Err(e) => {
            throw_io_exception(&mut env, &e.to_string());
            std::ptr::null_mut()
        }
    }
}

/// Get manifest metadata as JSON bytes
///
/// Java signature: `native byte[] nativeGetMetadata(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_ManifestReaderJNI_nativeGetMetadata(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jbyteArray {
    let config = match unsafe { handle_to_ref::<ManifestReaderConfig>(handle) } {
        Ok(c) => c.clone(),
        Err(e) => {
            throw_runtime_exception(&mut env, &e.to_string());
            return std::ptr::null_mut();
        }
    };

    let reader = match ManifestReaderBuilder::new().open(&config.path) {
        Ok(r) => r,
        Err(e) => {
            throw_io_exception(&mut env, &e.to_string());
            return std::ptr::null_mut();
        }
    };

    let metadata = reader.metadata();
    match serialize_to_json(&metadata) {
        Ok(bytes) => match vec_to_jbytes(&mut env, &bytes) {
            Ok(arr) => arr.into_raw(),
            Err(e) => {
                throw_runtime_exception(&mut env, &e.to_string());
                std::ptr::null_mut()
            }
        },
        Err(e) => {
            throw_runtime_exception(&mut env, &e.to_string());
            std::ptr::null_mut()
        }
    }
}

/// Get read statistics as JSON bytes
///
/// Java signature: `native byte[] nativeGetStats(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_ManifestReaderJNI_nativeGetStats(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jbyteArray {
    let config = match unsafe { handle_to_ref::<ManifestReaderConfig>(handle) } {
        Ok(c) => c.clone(),
        Err(e) => {
            throw_runtime_exception(&mut env, &e.to_string());
            return std::ptr::null_mut();
        }
    };

    let reader = match ManifestReaderBuilder::new().open(&config.path) {
        Ok(r) => r,
        Err(e) => {
            throw_io_exception(&mut env, &e.to_string());
            return std::ptr::null_mut();
        }
    };

    let stats = reader.stats();
    match serialize_to_json(&stats) {
        Ok(bytes) => match vec_to_jbytes(&mut env, &bytes) {
            Ok(arr) => arr.into_raw(),
            Err(e) => {
                throw_runtime_exception(&mut env, &e.to_string());
                std::ptr::null_mut()
            }
        },
        Err(e) => {
            throw_runtime_exception(&mut env, &e.to_string());
            std::ptr::null_mut()
        }
    }
}

/// Close and release the reader
///
/// Java signature: `native void nativeClose(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_ManifestReaderJNI_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    unsafe {
        drop_handle::<ManifestReaderConfig>(handle);
    }
}

// =============================================================================
// ManifestGroup JNI Functions
// =============================================================================

/// Parallel manifest group reader handle
struct ManifestGroupConfig {
    paths: Vec<String>,
    parallelism: usize,
}

/// Create a parallel manifest group reader
///
/// Java signature: `native long nativeCreateGroup(int parallelism)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_ManifestGroupJNI_nativeCreate(
    _env: JNIEnv,
    _class: JClass,
    parallelism: jint,
) -> jlong {
    let config = ManifestGroupConfig {
        paths: Vec::new(),
        parallelism: if parallelism > 0 {
            parallelism as usize
        } else {
            num_cpus::get()
        },
    };
    box_to_handle(config)
}

/// Add a manifest to the group
///
/// Java signature: `native void nativeAddManifest(long handle, String path, long firstRowId)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_ManifestGroupJNI_nativeAddManifest(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    path: JString,
    _first_row_id: jlong,
) {
    let path_str = match jstring_to_string(&mut env, &path) {
        Ok(s) => s,
        Err(e) => {
            throw_runtime_exception(&mut env, &e.to_string());
            return;
        }
    };

    // We need to modify the config, so get a mutable reference
    let config = unsafe {
        if handle == 0 {
            throw_runtime_exception(&mut env, "Invalid handle");
            return;
        }
        &mut *(handle as *mut ManifestGroupConfig)
    };

    config.paths.push(path_str);
}

/// Read all data entries from all manifests in parallel
///
/// Java signature: `native byte[] nativeReadAllDataEntries(long handle, int parallelism)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_ManifestGroupJNI_nativeReadAllDataEntries(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    _parallelism: jint,
) -> jbyteArray {
    let config = match unsafe { handle_to_ref::<ManifestGroupConfig>(handle) } {
        Ok(c) => c,
        Err(e) => {
            throw_runtime_exception(&mut env, &e.to_string());
            return std::ptr::null_mut();
        }
    };

    use crate::manifest::{ManifestFile, ManifestGroup};

    let mut builder = ManifestGroup::builder();
    for path in &config.paths {
        builder = builder.add_manifest(ManifestFile::data(path));
    }
    builder = builder.parallelism(config.parallelism);

    match builder.build().read_data_entries() {
        Ok(result) => {
            let entry_data: Vec<ManifestEntryData> = result
                .entries
                .iter()
                .map(|e| ManifestEntryData {
                    status: e.status() as i32,
                    snapshot_id: e.snapshot_id(),
                    data_sequence_number: e.data_sequence_number(),
                    file_sequence_number: e.file_sequence_number(),
                    file_path: e.file().file_path().to_string(),
                    file_format: format!("{:?}", e.file().file_format()),
                    spec_id: e.file().spec_id(),
                    record_count: e.file().record_count(),
                    file_size_in_bytes: e.file().file_size_in_bytes(),
                    first_row_id: e.file().first_row_id(),
                    content: e.file().content() as i32,
                })
                .collect();

            match serialize_to_json(&entry_data) {
                Ok(bytes) => match vec_to_jbytes(&mut env, &bytes) {
                    Ok(arr) => arr.into_raw(),
                    Err(e) => {
                        throw_runtime_exception(&mut env, &e.to_string());
                        std::ptr::null_mut()
                    }
                },
                Err(e) => {
                    throw_runtime_exception(&mut env, &e.to_string());
                    std::ptr::null_mut()
                }
            }
        }
        Err(e) => {
            throw_io_exception(&mut env, &e.to_string());
            std::ptr::null_mut()
        }
    }
}

/// Close the manifest group
///
/// Java signature: `native void nativeCloseGroup(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_ManifestGroupJNI_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    unsafe {
        drop_handle::<ManifestGroupConfig>(handle);
    }
}

// Placeholder for num_cpus
mod num_cpus {
    pub fn get() -> usize {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    }
}
