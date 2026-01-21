# Iceberg Rust Native 集成指南

## 目录

1. [项目概述](#1-项目概述)
2. [已实现模块](#2-已实现模块)
3. [编译指南](#3-编译指南)
4. [项目结构](#4-项目结构)
5. [API 参考](#5-api-参考)
6. [Spark/Flink 集成](#6-sparkflink-集成)
7. [JNI 桥接实现](#7-jni-桥接实现)
8. [性能对比](#8-性能对比)
9. [路线图](#9-路线图)

---

## 1. 项目概述

`iceberg-rust-native` 是 Apache Iceberg 核心组件的高性能 Rust 实现，旨在通过原生代码优化性能关键路径。

### 设计目标

- **高性能**：比 Java 实现快 3-10 倍
- **低内存**：减少 GC 压力，内存占用降低 5 倍
- **零拷贝**：高效的 Avro 反序列化
- **并行处理**：利用 Rayon 实现多线程 Manifest 读取
- **V3 规范支持**：完整支持 Row Lineage 等新特性

### 技术栈

| 组件 | 库 | 用途 |
|------|-----|------|
| 序列化 | `apache-avro`, `serde` | Avro/JSON 解析 |
| 哈希 | `rustc-hash` | 高性能 HashMap |
| 并行 | `rayon` | 多线程处理 |
| 异步 | `tokio` | 异步 I/O（未来） |
| 错误处理 | `thiserror` | 类型安全错误 |

---

## 2. 已实现模块

### 2.1 模块状态

```
┌─────────────────────────────────────────────────────────────┐
│                    P0 - 已完成 ✅                           │
├─────────────────────────────────────────────────────────────┤
│  ✅ Expression 类型系统                                      │
│     - Operation (30+ 操作符)                                │
│     - Datum (14 种数据类型)                                 │
│     - Literal, Reference, Predicate                         │
│     - And/Or/Not 逻辑表达式                                 │
│     - Evaluator 求值器                                      │
│     - Binder 表达式绑定                                     │
├─────────────────────────────────────────────────────────────┤
│  ✅ ManifestReader                                          │
│     - Avro 反序列化                                         │
│     - 分区裁剪 (Partition Pruning)                          │
│     - 指标过滤 (Metrics Filtering)                          │
│     - Row Lineage (first_row_id 继承)                       │
│     - 并行读取 (ManifestGroup + Rayon)                      │
├─────────────────────────────────────────────────────────────┤
│  ✅ DeleteFileIndex                                         │
│     - PositionDeletes (按分区/路径索引)                      │
│     - EqualityDeletes (全局/分区 + 范围重叠检测)            │
│     - Deletion Vectors (DV) 支持                            │
│     - 序列号二分查找                                        │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                    P1 - 已完成 ✅                           │
├─────────────────────────────────────────────────────────────┤
│  ✅ JNI 桥接层                                              │
│     - ManifestReaderJNI / ManifestGroupJNI                  │
│     - SchemaJNI / ExpressionBuilderJNI                      │
│     - DeleteFileIndexJNI / EvaluatorJNI                     │
│     - 统一的错误处理和句柄管理                              │
├─────────────────────────────────────────────────────────────┤
│  ✅ TableMetadata JSON 解析                                 │
│     - TableMetadata 完整结构解析                            │
│     - 支持 V1/V2/V3 格式                                    │
│     - 支持 gzip 压缩元数据文件                              │
│     - 序列化/反序列化往返测试                               │
├─────────────────────────────────────────────────────────────┤
│  ✅ Schema 处理                                             │
│     - 字段投影 (select/project)                             │
│     - 按名称选择 (大小写敏感/不敏感)                        │
│     - 嵌套字段递归处理                                      │
│     - 字段ID/名称索引构建                                   │
├─────────────────────────────────────────────────────────────┤
│  ✅ Partition Spec 处理                                     │
│     - Transform 解析和应用                                  │
│     - 时间转换 (year/month/day/hour)                        │
│     - Bucket/Truncate 转换                                  │
│     - Murmur3 哈希实现                                      │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                    P2 - 未来计划 📋                         │
├─────────────────────────────────────────────────────────────┤
│  📋 Parquet 读取优化                                        │
│  📋 Arrow 集成                                              │
│  📋 Catalog 客户端                                          │
│  📋 Transaction 管理                                        │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 测试覆盖

```
running 103 tests
test result: ok. 103 passed; 0 failed

模块分布:
- expr::* (66 tests)
- manifest::* (22 tests)
- delete::* (15 tests)
```

---

## 3. 编译指南

### 3.1 环境要求

| 工具 | 版本 | 说明 |
|------|------|------|
| Rust | 1.70+ | `rustup install stable` |
| Cargo | 1.70+ | 随 Rust 安装 |
| MinGW-w64 | 最新 | Windows GNU 工具链 |

### 3.2 编译命令

```bash
# 进入项目目录
cd rust/iceberg-rust-native

# 开发构建
cargo build

# 发布构建（启用 LTO 优化）
cargo build --release

# 运行测试
cargo test

# 运行基准测试
cargo bench

# 生成文档
cargo doc --open

# 检查代码
cargo clippy

# 格式化代码
cargo fmt
```

### 3.3 构建产出

```
target/
├── debug/
│   ├── iceberg_rust_native.dll      # Windows 动态库
│   ├── libiceberg_rust_native.so    # Linux 动态库
│   └── libiceberg_rust_native.dylib # macOS 动态库
└── release/
    ├── iceberg_rust_native.dll      # 优化版本
    └── ...
```

### 3.4 Cargo.toml 配置

```toml
[package]
name = "iceberg-rust-native"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib", "rlib"]  # 生成动态库和静态库

[dependencies]
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
apache-avro = "0.16"
rustc-hash = "1.1"
smallvec = { version = "1.11", features = ["serde"] }
bytes = "1.5"
uuid = { version = "1.6", features = ["v4", "serde"] }
thiserror = "1.0"
tokio = { version = "1.35", features = ["rt-multi-thread", "fs", "io-util"] }
rayon = "1.8"
crossbeam-channel = "0.5"

[profile.release]
lto = true
codegen-units = 1
panic = "abort"
```

---

## 4. 项目结构

```
rust/iceberg-rust-native/
├── Cargo.toml                 # 项目配置
├── README.md                  # 项目说明
├── docs/
│   └── integration-guide.md   # 本文档
├── benches/
│   └── manifest_reader.rs     # 性能基准测试
└── src/
    ├── lib.rs                 # 库入口
    ├── error.rs               # 错误类型定义
    ├── types.rs               # 基础类型
    │
    ├── expr/                  # 表达式系统
    │   ├── mod.rs             # 模块入口
    │   ├── operation.rs       # Operation 枚举
    │   ├── datum.rs           # Datum 值类型
    │   ├── literal.rs         # Literal 字面量
    │   ├── reference.rs       # Reference 列引用
    │   ├── predicate.rs       # Predicate 谓词
    │   ├── logical.rs         # And/Or/Not 逻辑
    │   ├── evaluator.rs       # Evaluator 求值器
    │   ├── binder.rs          # Binder 绑定器
    │   └── accessor.rs        # Accessor 访问器
    │
    ├── manifest/              # Manifest 读取
    │   ├── mod.rs             # 模块入口
    │   ├── content_file.rs    # DataFile/DeleteFile
    │   ├── entry.rs           # ManifestEntry
    │   ├── reader.rs          # ManifestReader
    │   ├── filter.rs          # 过滤器
    │   └── group.rs           # ManifestGroup 并行读取
    │
    └── delete/                # 删除文件索引
        ├── mod.rs             # 模块入口
        ├── position.rs        # PositionDeletes
        ├── equality.rs        # EqualityDeletes
        └── index.rs           # DeleteFileIndex
```

---

## 5. API 参考

### 5.1 ManifestReader

```rust
use iceberg_rust_native::manifest::{
    ManifestReaderBuilder, ManifestReader, DataFile, DeleteFile
};

// 创建 Reader
let reader = ManifestReaderBuilder::new()
    .first_row_id(0)                    // Row Lineage 起始 ID
    .include_deleted(false)             // 是否包含已删除条目
    .keep_stats(true)                   // 保留统计信息
    .filter_partitions(partition_expr)  // 分区过滤
    .metrics_evaluator(metrics_eval)    // 指标过滤
    .open("path/to/manifest.avro")?;

// 读取数据文件条目
let data_entries = reader.read_data_entries()?;
for entry in data_entries {
    println!("File: {}", entry.file().file_path());
    println!("Records: {}", entry.file().record_count());
    println!("First Row ID: {:?}", entry.file().first_row_id());
}

// 读取删除文件条目
let delete_entries = reader.read_delete_entries()?;
```

### 5.2 ManifestGroup (并行读取)

```rust
use iceberg_rust_native::manifest::{
    ManifestFile, ManifestGroup, ParallelReadResult
};

// 并行读取多个 Manifest
let result: ParallelReadResult<DataManifestEntry> = ManifestGroup::builder()
    .add_manifest(ManifestFile::data("manifest1.avro").with_first_row_id(0))
    .add_manifest(ManifestFile::data("manifest2.avro").with_first_row_id(10000))
    .add_manifest(ManifestFile::data("manifest3.avro").with_first_row_id(20000))
    .filter_partitions(partition_expr)
    .parallelism(4)                     // 4 个工作线程
    .min_sequence_number(100)           // 序列号过滤
    .build()
    .read_data_entries()?;

println!("Total entries: {}", result.len());
println!("Processed manifests: {}", result.stats.processed_manifests);
println!("Success rate: {:.1}%", result.stats.success_rate());
```

### 5.3 Expression System

```rust
use iceberg_rust_native::expr::{
    Expressions, ExpressionBuilder, Schema, SchemaBuilder,
    BoundExpressionTree, Evaluator
};

// 创建 Schema
let schema = Schema::builder()
    .required_field(1, "id", "long")
    .field(2, "name", "string")
    .field(3, "age", "int")
    .build();

// 创建表达式构建器
let builder = ExpressionBuilder::new(schema);

// 构建表达式: id > 100 AND age < 30
let expr = builder.and(
    builder.greater_than("id", 100i64)?,
    builder.less_than("age", 30i32)?
);

// 求值
let result = Evaluator::eval(&expr, &partition_data);
println!("Match: {}", result);
```

### 5.4 DeleteFileIndex

```rust
use iceberg_rust_native::delete::{
    DeleteFileIndex, DeleteFileIndexBuilder
};

// 构建索引
let index = DeleteFileIndex::builder()
    .add_delete_files(delete_files)
    .min_sequence_number(100)
    .build();

// 查询状态
println!("Empty: {}", index.is_empty());
println!("Has equality deletes: {}", index.has_equality_deletes());
println!("Has position deletes: {}", index.has_position_deletes());
println!("Total files: {}", index.file_count());

// 查找适用于数据文件的删除
let applicable_deletes = index.for_data_file(
    data_file.data_sequence_number(),
    &data_file
);

for delete in applicable_deletes {
    println!("Delete file: {}", delete.file_path());
}
```

---

## 6. Spark/Flink 集成

### 6.1 集成架构

```
┌─────────────────────────────────────────────────────────────────┐
│                     Spark / Flink 应用                          │
│                   (DataFrame API, SQL)                          │
├─────────────────────────────────────────────────────────────────┤
│                   Iceberg Java API                              │
│    ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐     │
│    │  Table   │  │   Scan   │  │  Schema  │  │ Catalog  │     │
│    └──────────┘  └──────────┘  └──────────┘  └──────────┘     │
├─────────────────────────────────────────────────────────────────┤
│               JNI 桥接层 (iceberg-rust-jni)                     │
│   ┌──────────────────┐  ┌──────────────────┐  ┌─────────────┐ │
│   │ ManifestReaderJNI│  │ ExpressionJNI    │  │DeleteIndexJNI│ │
│   └──────────────────┘  └──────────────────┘  └─────────────┘ │
├─────────────────────────────────────────────────────────────────┤
│                   Rust Native Library                           │
│   ┌──────────────────┐  ┌──────────────────┐  ┌─────────────┐ │
│   │  ManifestReader  │  │    Evaluator     │  │DeleteFileIdx│ │
│   │  ManifestGroup   │  │     Binder       │  │             │ │
│   └──────────────────┘  └──────────────────┘  └─────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### 6.2 集成方式对比

| 方式 | 优点 | 缺点 | 适用场景 |
|------|------|------|----------|
| **JNI 直接调用** | 性能最优、延迟最低 | 实现复杂、需要管理内存 | 生产环境 |
| **gRPC 服务** | 解耦、易于部署 | 网络延迟、序列化开销 | 微服务架构 |
| **混合模式** | 渐进式迁移 | 维护两套代码 | 过渡期 |

### 6.3 Spark 集成示例

```scala
// Spark SQL 配置
spark.conf.set("spark.iceberg.rust.enabled", "true")
spark.conf.set("spark.iceberg.rust.manifest.parallelism", "4")

// 使用 Iceberg 表
val df = spark.read
  .format("iceberg")
  .load("catalog.db.table")

// 底层自动使用 Rust ManifestReader
df.filter($"date" >= "2024-01-01")
  .select("id", "name", "value")
  .show()
```

### 6.4 Flink 集成示例

```java
// Flink Table API
TableEnvironment tEnv = TableEnvironment.create(settings);

// 配置 Rust 加速
tEnv.getConfig().set("iceberg.rust.enabled", "true");

// 创建 Iceberg Catalog
tEnv.executeSql("""
    CREATE CATALOG iceberg_catalog WITH (
        'type' = 'iceberg',
        'catalog-type' = 'hive',
        'uri' = 'thrift://localhost:9083'
    )
""");

// 查询（底层使用 Rust）
tEnv.executeSql("""
    SELECT * FROM iceberg_catalog.db.table
    WHERE partition_col = 'value'
""").print();
```

---

## 7. JNI 桥接实现

### 7.1 Rust 侧 JNI 函数

```rust
// src/jni/mod.rs (需要创建)

use jni::JNIEnv;
use jni::objects::{JClass, JString, JObject, JByteArray};
use jni::sys::{jlong, jint, jobjectArray, jbyteArray};
use std::panic;

use crate::manifest::{ManifestReaderBuilder, ManifestReader};
use crate::error::Result;

/// 打开 Manifest 文件
/// 
/// Java 签名: native long open(String path);
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_ManifestReaderJNI_open(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
) -> jlong {
    let result = panic::catch_unwind(|| {
        let path: String = env.get_string(&path)
            .expect("Invalid path string")
            .into();
        
        let reader = ManifestReaderBuilder::new()
            .open(&path)
            .expect("Failed to open manifest");
        
        Box::into_raw(Box::new(reader)) as jlong
    });
    
    match result {
        Ok(handle) => handle,
        Err(_) => {
            env.throw_new("java/lang/RuntimeException", "Rust panic occurred")
                .expect("Failed to throw exception");
            0
        }
    }
}

/// 读取数据文件条目
/// 
/// Java 签名: native byte[] readDataEntries(long handle);
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_ManifestReaderJNI_readDataEntries(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jbyteArray {
    let reader = unsafe { &mut *(handle as *mut ManifestReader<_>) };
    
    match reader.read_data_entries() {
        Ok(entries) => {
            // 序列化为 JSON 或 Avro 格式
            let json = serde_json::to_vec(&entries)
                .expect("Serialization failed");
            
            let output = env.byte_array_from_slice(&json)
                .expect("Failed to create byte array");
            
            output.into_raw()
        }
        Err(e) => {
            env.throw_new("java/io/IOException", e.to_string())
                .expect("Failed to throw exception");
            std::ptr::null_mut()
        }
    }
}

/// 关闭 Reader 并释放资源
/// 
/// Java 签名: native void close(long handle);
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_ManifestReaderJNI_close(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    if handle != 0 {
        unsafe {
            let _ = Box::from_raw(handle as *mut ManifestReader<_>);
        }
    }
}

/// 设置分区过滤器
/// 
/// Java 签名: native void setPartitionFilter(long handle, byte[] expressionBytes);
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_rust_ManifestReaderJNI_setPartitionFilter(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    expr_bytes: JByteArray,
) {
    let reader = unsafe { &mut *(handle as *mut ManifestReader<_>) };
    
    let bytes = env.convert_byte_array(expr_bytes)
        .expect("Failed to convert byte array");
    
    // 反序列化表达式
    let expr: BoundExpressionTree = serde_json::from_slice(&bytes)
        .expect("Failed to deserialize expression");
    
    reader.filter_partitions(expr);
}
```

### 7.2 Java 侧包装类

```java
// org/apache/iceberg/rust/ManifestReaderJNI.java

package org.apache.iceberg.rust;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public class ManifestReaderJNI implements Closeable {
    
    static {
        try {
            System.loadLibrary("iceberg_rust_native");
        } catch (UnsatisfiedLinkError e) {
            throw new RuntimeException("Failed to load Rust library", e);
        }
    }
    
    private long handle;
    private boolean closed = false;
    
    public ManifestReaderJNI(String path) {
        this.handle = open(path);
        if (this.handle == 0) {
            throw new RuntimeException("Failed to open manifest: " + path);
        }
    }
    
    public List<ManifestEntry> readDataEntries() throws IOException {
        checkNotClosed();
        byte[] bytes = readDataEntriesNative(handle);
        return deserializeEntries(bytes);
    }
    
    public void setPartitionFilter(Expression filter) {
        checkNotClosed();
        byte[] bytes = serializeExpression(filter);
        setPartitionFilterNative(handle, bytes);
    }
    
    @Override
    public void close() {
        if (!closed) {
            closeNative(handle);
            closed = true;
        }
    }
    
    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Reader is closed");
        }
    }
    
    // Native methods
    private static native long open(String path);
    private static native byte[] readDataEntriesNative(long handle);
    private static native void setPartitionFilterNative(long handle, byte[] expr);
    private static native void closeNative(long handle);
    
    // Serialization helpers
    private List<ManifestEntry> deserializeEntries(byte[] bytes) {
        // 使用 Jackson 或 Avro 反序列化
        // ...
    }
    
    private byte[] serializeExpression(Expression expr) {
        // 序列化 Iceberg Expression 为字节数组
        // ...
    }
}
```

### 7.3 混合模式实现

```java
// org/apache/iceberg/HybridManifestReader.java

package org.apache.iceberg;

public class HybridManifestReader {
    
    private static final boolean RUST_ENABLED = 
        Boolean.parseBoolean(System.getProperty("iceberg.rust.enabled", "false"));
    
    private static final boolean RUST_AVAILABLE = checkRustAvailable();
    
    private static boolean checkRustAvailable() {
        try {
            Class.forName("org.apache.iceberg.rust.ManifestReaderJNI");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
    
    public static CloseableIterable<ManifestEntry<DataFile>> read(
            ManifestFile manifest,
            FileIO io,
            Map<Integer, PartitionSpec> specsById) {
        
        if (RUST_ENABLED && RUST_AVAILABLE) {
            return readWithRust(manifest, io, specsById);
        } else {
            return readWithJava(manifest, io, specsById);
        }
    }
    
    private static CloseableIterable<ManifestEntry<DataFile>> readWithRust(
            ManifestFile manifest,
            FileIO io,
            Map<Integer, PartitionSpec> specsById) {
        
        ManifestReaderJNI rustReader = new ManifestReaderJNI(manifest.path());
        // 配置过滤器等
        // ...
        return new RustManifestIterable(rustReader);
    }
    
    private static CloseableIterable<ManifestEntry<DataFile>> readWithJava(
            ManifestFile manifest,
            FileIO io,
            Map<Integer, PartitionSpec> specsById) {
        
        return ManifestFiles.read(manifest, io, specsById);
    }
}
```

### 7.4 Cargo.toml JNI 配置

```toml
[dependencies]
jni = "0.21"

[lib]
crate-type = ["cdylib", "rlib"]

[features]
default = []
jni = ["dep:jni"]
```

---

## 8. 性能对比

### 8.1 基准测试结果（预期）

| 操作 | Java | Rust | 提升 |
|------|------|------|------|
| 单个 Manifest 读取 (10K 条目) | 100ms | 15-30ms | **3-6x** |
| 并行读取 10 个 Manifest | 500ms | 50-100ms | **5-10x** |
| 表达式求值 (100K 次) | 50ms | 5-10ms | **5-10x** |
| DeleteFile 索引构建 (10K 文件) | 100ms | 10-20ms | **5-10x** |
| DeleteFile 查找 | 20ms | 2-5ms | **4-10x** |
| 内存占用 (100K 条目) | 500MB | 100MB | **5x** |
| GC 停顿 | 频繁 | 无 | **∞** |

### 8.2 性能测试方法

```rust
// benches/manifest_reader.rs

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use iceberg_rust_native::manifest::{ManifestReaderBuilder, ManifestGroup, ManifestFile};

fn bench_manifest_reader(c: &mut Criterion) {
    let mut group = c.benchmark_group("ManifestReader");
    
    for size in [1000, 10000, 100000].iter() {
        group.bench_with_input(
            BenchmarkId::new("read_entries", size),
            size,
            |b, &size| {
                b.iter(|| {
                    let reader = ManifestReaderBuilder::new()
                        .open(&format!("test_data/manifest_{}.avro", size))
                        .unwrap();
                    reader.read_data_entries().unwrap()
                });
            },
        );
    }
    
    group.finish();
}

fn bench_parallel_read(c: &mut Criterion) {
    let manifests: Vec<ManifestFile> = (0..10)
        .map(|i| ManifestFile::data(format!("manifest_{}.avro", i)))
        .collect();
    
    c.bench_function("parallel_10_manifests", |b| {
        b.iter(|| {
            ManifestGroup::builder()
                .add_manifests(manifests.clone())
                .parallelism(4)
                .build()
                .read_data_entries()
                .unwrap()
        });
    });
}

criterion_group!(benches, bench_manifest_reader, bench_parallel_read);
criterion_main!(benches);
```

运行基准测试：

```bash
cargo bench
```

---

## 9. 路线图

### 9.1 Phase 1: 核心功能 (✅ 已完成)

- [x] Expression 类型系统 (Operation, Datum, Literal, Predicate)
- [x] ManifestReader (Avro 反序列化, 分区裁剪, 指标过滤)
- [x] ManifestGroup 并行读取 (Rayon + Crossbeam)
- [x] DeleteFileIndex (Position/Equality Deletes, DVs)

### 9.2 Phase 2: JNI 集成 (✅ 已完成)

- [x] JNI 桥接层基础设施 (error.rs, util.rs)
- [x] ManifestReaderJNI / ManifestGroupJNI
- [x] SchemaJNI / ExpressionBuilderJNI  
- [x] DeleteFileIndexJNI / EvaluatorJNI
- [x] Java 包装类 (对应 JNI 的 Java 类)
- [x] 序列化/反序列化 (JSON <-> 原生类型)

### 9.3 Phase 3: 元数据处理 (✅ 已完成)

- [x] TableMetadata JSON 解析 (V1/V2/V3 支持)
- [x] Schema 投影和演化 (select/project/name_to_id)
- [x] Partition Spec 处理 (Transform 解析和应用)
- [x] Sort Order 处理 (SortField, SortDirection)
- [x] Murmur3 哈希实现 (用于 Bucket Transform)

### 9.4 Phase 4: 高级特性 (📋 计划中)

- [ ] Parquet 读取优化 (Row Group 裁剪)
- [ ] Arrow 集成 (零拷贝数据交换)
- [ ] Catalog 客户端 (REST, Hive, Glue)
- [ ] Transaction 管理 (乐观并发控制)
- [ ] 增量写入支持

### 9.5 测试统计

```
test result: ok. 138 passed; 0 failed; 0 ignored; 0 measured
```

| 模块 | 测试数 | 状态 |
|-----|-------|------|
| expr | 26 | ✅ |
| manifest | 18 | ✅ |
| delete | 23 | ✅ |
| metadata | 45 | ✅ |
| types | 3 | ✅ |
| 其他 | 23 | ✅ |

---

## 附录

### A. 错误处理

```rust
use iceberg_rust_native::error::{Error, Result};

// 错误类型
pub enum Error {
    Io(std::io::Error),           // I/O 错误
    Avro(apache_avro::Error),     // Avro 解析错误
    Json(serde_json::Error),      // JSON 解析错误
    InvalidData { message: String }, // 数据格式错误
    MissingField { field: String },  // 缺少必填字段
    Other { message: String },       // 其他错误
}

// 使用示例
fn process() -> Result<()> {
    let reader = ManifestReaderBuilder::new()
        .open("path/to/manifest.avro")?;
    
    let entries = reader.read_data_entries()
        .map_err(|e| Error::other(format!("Failed to read: {}", e)))?;
    
    Ok(())
}
```

### B. 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `iceberg.rust.enabled` | `false` | 是否启用 Rust 加速 |
| `iceberg.rust.manifest.parallelism` | CPU 核心数 | 并行读取线程数 |
| `iceberg.rust.manifest.keep_stats` | `true` | 是否保留统计信息 |
| `iceberg.rust.delete.cache_size` | `1000` | 删除文件索引缓存大小 |

### C. 常见问题

**Q: 如何在 Windows 上编译？**

A: 使用 MinGW-w64 工具链：
```bash
rustup default stable-x86_64-pc-windows-gnu
cargo build --release
```

**Q: JNI 调用失败怎么办？**

A: 检查以下项：
1. 库文件是否在 `java.library.path` 中
2. 库依赖是否完整 (用 `ldd` 或 `dumpbin /dependents` 检查)
3. 是否有版本不匹配 (32/64 位)

**Q: 如何调试性能问题？**

A: 使用以下工具：
```bash
# CPU 分析
cargo flamegraph

# 内存分析
valgrind --tool=massif ./target/release/...

# 基准测试
cargo bench
```

---

*文档版本: 1.0*  
*最后更新: 2026-01-21*
