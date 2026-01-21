# Iceberg Rust Library

基于 DataFusion 实现的 Iceberg 表文件读写 Rust 库。

## 功能特性

- ✅ **Iceberg V1 表读取** - 支持谓词下推、列裁剪
- ✅ **Iceberg V2 表读取** - 支持 Position Delete 文件
- ✅ **Iceberg V3 表读取** - 支持 Deletion Vector (Puffin 格式)
- ✅ **Parquet 文件写入** - 异步写入，支持多种压缩格式
- ✅ **小文件合并** - 高性能合并，内存使用可控

## 安装

在 `Cargo.toml` 中添加：

```toml
[dependencies]
iceberg-rust-lib = { path = "./iceberg-rust-lib" }
```

## 使用示例

### Iceberg V1 表读取

```rust
use iceberg_rust_lib::{IcebergV1Reader, ReadOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reader = IcebergV1Reader::new();
    
    // 配置读取选项
    let options = ReadOptions::new()
        .with_projection(vec!["id".to_string(), "name".to_string()])
        .with_filter("id > 100 AND name IS NOT NULL")
        .with_batch_size(8192);
    
    // 读取 parquet 文件
    let batches = reader.read("path/to/data.parquet", options).await?;
    
    for batch in batches {
        println!("Rows: {}", batch.num_rows());
    }
    
    Ok(())
}
```

### Iceberg V2 表读取 (Position Delete)

```rust
use iceberg_rust_lib::{IcebergV2Reader, ReadOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reader = IcebergV2Reader::new();
    let options = ReadOptions::new()
        .with_projection(vec!["id".to_string(), "value".to_string()]);
    
    // 读取数据文件，同时应用 position delete
    let batches = reader.read(
        "path/to/data.parquet",
        &["path/to/delete1.parquet", "path/to/delete2.parquet"],
        options
    ).await?;
    
    Ok(())
}
```

### Iceberg V3 表读取 (Deletion Vector)

```rust
use iceberg_rust_lib::{IcebergV3Reader, ReadOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reader = IcebergV3Reader::new();
    let options = ReadOptions::new()
        .with_filter("ts > '2024-01-01'");
    
    // 读取数据文件，同时应用 deletion vector
    let batches = reader.read(
        "path/to/data.parquet",
        "path/to/deletion-vector.puffin",
        options
    ).await?;
    
    Ok(())
}
```

### Parquet 文件写入

```rust
use iceberg_rust_lib::{ParquetWriter, WriteOptions, CompressionType};
use arrow::array::{Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let writer = ParquetWriter::new();
    
    // 配置写入选项
    let options = WriteOptions::new()
        .with_compression(CompressionType::Zstd)
        .with_row_group_size(100000)
        .with_bloom_filter(true, 0.05);
    
    // 创建测试数据
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, true),
    ]));
    
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )?;
    
    // 写入文件
    writer.write(&[batch], "output.parquet", options).await?;
    
    Ok(())
}
```

### 小文件合并

```rust
use iceberg_rust_lib::{FileMerger, MergeOptions, CompressionType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 配置合并选项
    let options = MergeOptions::new()
        .with_target_file_size(256 * 1024 * 1024)  // 256MB
        .with_max_memory(2 * 1024 * 1024 * 1024)   // 2GB
        .with_compression(CompressionType::Zstd)
        .with_sort_columns(vec!["ts".to_string()]);
    
    let merger = FileMerger::new(options);
    
    // 合并为单个文件
    let output = merger.merge_to_single_file(
        &["small1.parquet", "small2.parquet", "small3.parquet"],
        "merged.parquet"
    ).await?;
    
    // 或者合并并自动分割
    let outputs = merger.merge_with_splitting(
        &["small1.parquet", "small2.parquet", "small3.parquet"],
        "./output/",
        "merged"
    ).await?;
    
    Ok(())
}
```

## API 文档

### 读取接口

| 接口 | 描述 | 支持特性 |
|------|------|----------|
| `IcebergV1Reader::read()` | V1 表读取 | 谓词下推、列裁剪、行组过滤 |
| `IcebergV2Reader::read()` | V2 表读取 | 谓词下推、列裁剪、Position Delete |
| `IcebergV3Reader::read()` | V3 表读取 | 谓词下推、列裁剪、Deletion Vector |

### 写入接口

| 接口 | 描述 |
|------|------|
| `ParquetWriter::write()` | 写入 RecordBatch 到 Parquet 文件 |
| `ParquetWriter::write_partitioned()` | 分区写入多个文件 |
| `ParquetWriter::write_stream()` | 流式写入（内存高效） |

### 合并接口

| 接口 | 描述 |
|------|------|
| `FileMerger::merge_to_single_file()` | 合并为单个文件 |
| `FileMerger::merge_with_splitting()` | 合并并按大小分割 |
| `FileMerger::merge_streaming()` | 流式合并（内存高效） |

## 性能优化

### 读取优化

- **谓词下推**: 将过滤条件下推到 Parquet 读取层，减少 IO
- **列裁剪**: 只读取需要的列，减少内存使用
- **行组过滤**: 使用统计信息跳过不匹配的行组
- **Bloom Filter**: 利用 Bloom Filter 快速过滤
- **并行读取**: 使用 DataFusion 的并行执行能力

### 写入优化

- **异步写入**: 使用 tokio 异步 IO
- **压缩**: 支持 Zstd、Snappy、LZ4 等多种压缩
- **字典编码**: 自动使用字典编码压缩重复值
- **Bloom Filter**: 可选生成 Bloom Filter 加速后续查询

### 合并优化

- **内存控制**: 可配置最大内存使用量
- **流式处理**: 避免将所有数据加载到内存
- **并行处理**: 利用多核并行处理

## 支持的存储

- 本地文件系统
- Amazon S3 (`s3://`)
- Google Cloud Storage (`gs://`)
- Azure Blob Storage (`az://`, `abfs://`)
- HDFS (`hdfs://`)

## License

Apache License 2.0
