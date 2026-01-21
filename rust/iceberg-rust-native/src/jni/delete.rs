//! JNI bindings for DeleteFileIndex
//!
//! Java class: `org.apache.iceberg.rust.DeleteFileIndexJNI`

use jni::objects::{JByteArray, JClass};
use jni::sys::{jboolean, jbyteArray, jint, jlong, JNI_FALSE, JNI_TRUE};
use jni::JNIEnv;

use crate::delete::{DeleteFileIndex, DeleteFileIndexBuilder};
use crate::manifest::{ContentFile, DataFile, DeleteFile};

use super::error::throw_runtime_exception;
use super::util::{
    box_to_handle, deserialize_from_json, drop_handle, handle_to_ref, jbytes_to_vec,
    serialize_to_json, vec_to_jbytes,
};

/// Delete file data for serialization
#[derive(serde::Serialize, serde::Deserialize)]
struct DeleteFileData {
    file_path: String,
    file_format: String,
    spec_id: i32,
    record_count: i64,
    file_size_in_bytes: i64,
    content: i32, // 1 = position deletes, 2 = equality deletes
    data_sequence_number: Option<i64>,
    referenced_data_file: Option<String>,
    equality_field_ids: Option<Vec<i32>>,
}

/// Data file data for serialization (minimal for lookup)
#[derive(serde::Serialize, serde::Deserialize)]
struct DataFileData {
    file_path: String,
    spec_id: i32,
    data_sequence_number: i64,
    partition_json: String,
}

// =============================================================================
// DeleteFileIndex JNI Functions
// =============================================================================

/// Index handle type
type IndexHandle = DeleteFileIndex;

/// Build the index from JSON delete files
///
/// Java signature: `native long nativeBuildWithFiles(byte[] deleteFilesJson, long minSeqNumber)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_DeleteFileIndexJNI_nativeBuildWithFiles(
    mut env: JNIEnv,
    _class: JClass,
    delete_files_json: JByteArray,
    min_seq_number: jlong,
) -> jlong {
    let bytes = match jbytes_to_vec(&mut env, &delete_files_json) {
        Ok(b) => b,
        Err(e) => {
            throw_runtime_exception(&mut env, &e.to_string());
            return 0;
        }
    };

    let delete_file_data: Vec<DeleteFileData> = match deserialize_from_json(&bytes) {
        Ok(d) => d,
        Err(e) => {
            throw_runtime_exception(&mut env, &format!("Failed to parse delete files: {}", e));
            return 0;
        }
    };

    // Convert to DeleteFile objects
    let delete_files: Vec<DeleteFile> = delete_file_data
        .into_iter()
        .map(|d| {
            use crate::manifest::ContentFileData;
            use crate::types::{FileContent, FileFormat, PartitionData};

            let mut data = ContentFileData::default();
            data.file_path = d.file_path;
            data.file_format = match d.file_format.to_lowercase().as_str() {
                "parquet" => FileFormat::Parquet,
                "avro" => FileFormat::Avro,
                "orc" => FileFormat::Orc,
                _ => FileFormat::Parquet,
            };
            data.spec_id = d.spec_id;
            data.record_count = d.record_count;
            data.file_size_in_bytes = d.file_size_in_bytes;
            data.content = match d.content {
                1 => FileContent::PositionDeletes,
                2 => FileContent::EqualityDeletes,
                _ => FileContent::PositionDeletes,
            };
            data.data_sequence_number = d.data_sequence_number;
            data.referenced_data_file = d.referenced_data_file;
            data.equality_field_ids = d.equality_field_ids.map(|ids| ids.into());
            data.partition = PartitionData::empty();

            DeleteFile::from(data)
        })
        .collect();

    let mut builder = DeleteFileIndexBuilder::new();
    builder = builder.add_delete_files(delete_files);

    if min_seq_number > 0 {
        builder = builder.min_sequence_number(min_seq_number);
    }

    let index = builder.build();
    box_to_handle(index)
}

/// Check if the index is empty
///
/// Java signature: `native boolean nativeIsEmpty(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_DeleteFileIndexJNI_nativeIsEmpty(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jboolean {
    let index = match unsafe { handle_to_ref::<IndexHandle>(handle) } {
        Ok(i) => i,
        Err(e) => {
            throw_runtime_exception(&mut env, &e.to_string());
            return JNI_TRUE;
        }
    };

    if index.is_empty() {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

/// Check if the index has equality deletes
///
/// Java signature: `native boolean nativeHasEqualityDeletes(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_DeleteFileIndexJNI_nativeHasEqualityDeletes(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jboolean {
    let index = match unsafe { handle_to_ref::<IndexHandle>(handle) } {
        Ok(i) => i,
        Err(e) => {
            throw_runtime_exception(&mut env, &e.to_string());
            return JNI_FALSE;
        }
    };

    if index.has_equality_deletes() {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

/// Check if the index has position deletes
///
/// Java signature: `native boolean nativeHasPositionDeletes(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_DeleteFileIndexJNI_nativeHasPositionDeletes(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jboolean {
    let index = match unsafe { handle_to_ref::<IndexHandle>(handle) } {
        Ok(i) => i,
        Err(e) => {
            throw_runtime_exception(&mut env, &e.to_string());
            return JNI_FALSE;
        }
    };

    if index.has_position_deletes() {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

/// Get the total file count
///
/// Java signature: `native int nativeGetFileCount(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_DeleteFileIndexJNI_nativeGetFileCount(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jint {
    let index = match unsafe { handle_to_ref::<IndexHandle>(handle) } {
        Ok(i) => i,
        Err(e) => {
            throw_runtime_exception(&mut env, &e.to_string());
            return 0;
        }
    };

    index.file_count() as jint
}

/// Find delete files for a data file
///
/// Java signature: `native byte[] nativeForDataFile(long handle, byte[] dataFileJson)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_DeleteFileIndexJNI_nativeForDataFile(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    data_file_json: JByteArray,
) -> jbyteArray {
    let index = match unsafe { handle_to_ref::<IndexHandle>(handle) } {
        Ok(i) => i,
        Err(e) => {
            throw_runtime_exception(&mut env, &e.to_string());
            return std::ptr::null_mut();
        }
    };

    let bytes = match jbytes_to_vec(&mut env, &data_file_json) {
        Ok(b) => b,
        Err(e) => {
            throw_runtime_exception(&mut env, &e.to_string());
            return std::ptr::null_mut();
        }
    };

    let data_file_data: DataFileData = match deserialize_from_json(&bytes) {
        Ok(d) => d,
        Err(e) => {
            throw_runtime_exception(&mut env, &format!("Failed to parse data file: {}", e));
            return std::ptr::null_mut();
        }
    };

    // Create a DataFile object
    use crate::manifest::ContentFileData;
    use crate::types::{FileContent, FileFormat, PartitionData};

    let partition: PartitionData = match serde_json::from_str(&data_file_data.partition_json) {
        Ok(p) => p,
        Err(_) => PartitionData::empty(),
    };

    let mut data = ContentFileData::default();
    data.file_path = data_file_data.file_path;
    data.file_format = FileFormat::Parquet;
    data.spec_id = data_file_data.spec_id;
    data.content = FileContent::Data;
    data.data_sequence_number = Some(data_file_data.data_sequence_number);
    data.partition = partition;
    data.record_count = 0;
    data.file_size_in_bytes = 0;

    let data_file = DataFile::from(data);

    // Find applicable delete files
    let delete_files = index.for_data_file(data_file_data.data_sequence_number, &data_file);

    // Convert to serializable format
    let result: Vec<DeleteFileData> = delete_files
        .iter()
        .map(|f| DeleteFileData {
            file_path: f.file_path().to_string(),
            file_format: format!("{:?}", f.file_format()),
            spec_id: f.spec_id(),
            record_count: f.record_count(),
            file_size_in_bytes: f.file_size_in_bytes(),
            content: f.content() as i32,
            data_sequence_number: f.data_sequence_number(),
            referenced_data_file: f.referenced_data_file().map(|s| s.to_string()),
            equality_field_ids: f.equality_field_ids().map(|ids| ids.to_vec()),
        })
        .collect();

    match serialize_to_json(&result) {
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

/// Get all delete files
///
/// Java signature: `native byte[] nativeGetAllDeleteFiles(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_DeleteFileIndexJNI_nativeGetAllDeleteFiles(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jbyteArray {
    let index = match unsafe { handle_to_ref::<IndexHandle>(handle) } {
        Ok(i) => i,
        Err(e) => {
            throw_runtime_exception(&mut env, &e.to_string());
            return std::ptr::null_mut();
        }
    };

    let delete_files = index.all_delete_files();

    let result: Vec<DeleteFileData> = delete_files
        .iter()
        .map(|f| DeleteFileData {
            file_path: f.file_path().to_string(),
            file_format: format!("{:?}", f.file_format()),
            spec_id: f.spec_id(),
            record_count: f.record_count(),
            file_size_in_bytes: f.file_size_in_bytes(),
            content: f.content() as i32,
            data_sequence_number: f.data_sequence_number(),
            referenced_data_file: f.referenced_data_file().map(|s| s.to_string()),
            equality_field_ids: f.equality_field_ids().map(|ids| ids.to_vec()),
        })
        .collect();

    match serialize_to_json(&result) {
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

/// Close the index
///
/// Java signature: `native void nativeClose(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_DeleteFileIndexJNI_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    unsafe {
        drop_handle::<IndexHandle>(handle);
    }
}
