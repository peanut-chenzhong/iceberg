# Iceberg Rust Native

高性能原生 Rust 实现的 Apache Iceberg 核心组件。

## 概述

本项目提供 Apache Iceberg 性能关键路径的优化 Rust 实现，旨在：

- **最大化吞吐量** - 零拷贝反序列化，避免不必要的内存分配
- **最小化延迟** - 无 GC 暂停，编译期优化
- **完整 V3 支持** - 包括 Row Lineage 和 Deletion Vectors

## 特性

### 已实现

- ✅ ManifestEntry 数据结构
- ✅ DataFile / DeleteFile 内容文件
- ✅ Avro manifest 文件读取
- ✅ Row Lineage (first_row_id) 继承
- ✅ 分区数据处理
- ✅ 列统计信息

### 计划中

- 🔲 Expression 求值器
- 🔲 DeleteFileIndex
- 🔲 ManifestGroup 并行处理
- 🔲 TableMetadata JSON 解析
- 🔲 JNI 桥接层

## 使用示例

```rust
use iceberg_rust_native::manifest::{ManifestReader, DataFile};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 读取 manifest 文件
    let reader = ManifestReader::builder()
        .first_row_id(1000)  // Row Lineage 起始 ID
        .keep_stats(true)    // 保留统计信息
        .open("path/to/manifest.avro")?;

    // 获取元数据
    println!("Content: {:?}", reader.content());
    
    // 读取所有数据文件条目
    let entries = reader.read_data_entries()?;
    
    for entry in entries {
        let file = entry.file();
        println!("File: {}", file.file_path());
        println!("  Records: {}", file.record_count());
        println!("  Size: {} bytes", file.file_size_in_bytes());
        
        if let Some(first_row_id) = file.first_row_id() {
            println!("  First Row ID: {}", first_row_id);
        }
    }

    Ok(())
}
```

## 性能对比

| 操作 | Java (iceberg-core) | Rust (iceberg-rust-native) | 提升 |
|------|---------------------|----------------------------|------|
| Manifest 解析 (1000 entries) | ~50ms | ~12ms | 4.2x |
| 内存占用 (per entry) | ~2KB | ~400B | 5x |
| 冷启动时间 | ~1.5s (JVM) | ~5ms | 300x |

*基准测试环境: Apple M2, macOS 14.0*

## 架构

```
iceberg-rust-native/
├── src/
│   ├── lib.rs              # 库入口
│   ├── error.rs            # 错误类型
│   ├── types.rs            # 核心类型定义
│   └── manifest/
│       ├── mod.rs          # Manifest 模块
│       ├── content_file.rs # DataFile/DeleteFile
│       ├── entry.rs        # ManifestEntry
│       └── reader.rs       # ManifestReader
├── benches/
│   └── manifest_reader.rs  # 性能基准测试
└── Cargo.toml
```

## 数据结构设计

### ContentFileData

```rust
pub struct ContentFileData {
    // 必需字段
    pub content: FileContent,           // 0=data, 1=pos_delete, 2=eq_delete
    pub file_path: String,              // 文件 URI
    pub file_format: FileFormat,        // parquet/avro/orc
    pub spec_id: i32,                   // 分区规格 ID
    pub partition: PartitionData,       // 分区值元组
    pub record_count: i64,              // 记录数
    pub file_size_in_bytes: i64,        // 文件大小

    // 统计信息 (可选，用于剪枝)
    pub column_sizes: Option<FxHashMap<i32, i64>>,
    pub value_counts: Option<FxHashMap<i32, i64>>,
    pub null_value_counts: Option<FxHashMap<i32, i64>>,
    pub lower_bounds: Option<FxHashMap<i32, Vec<u8>>>,
    pub upper_bounds: Option<FxHashMap<i32, Vec<u8>>>,

    // Row Lineage (V3)
    pub first_row_id: Option<i64>,      // 继承自 manifest
    pub data_sequence_number: Option<i64>,
    
    // Delete 特有字段
    pub equality_field_ids: Option<SmallVec<[i32; 4]>>,
    pub referenced_data_file: Option<String>,
}
```

### ManifestEntry

```rust
pub struct ManifestEntry<F: ContentFile> {
    pub status: Status,                  // EXISTING(0), ADDED(1), DELETED(2)
    pub snapshot_id: Option<i64>,        // 添加该文件的快照 ID
    pub data_sequence_number: Option<i64>,
    pub file_sequence_number: Option<i64>,
    pub file: F,                         // DataFile 或 DeleteFile
}
```

## 内存优化策略

1. **FxHashMap** - 使用更快的哈希函数替代 std HashMap
2. **SmallVec** - 对于小数组（如 split_offsets）避免堆分配
3. **Option 字段** - 可选字段使用 Option 而不是空集合
4. **零拷贝** - Avro 解析时尽可能复用缓冲区

## 构建

```bash
# 开发构建
cargo build

# 发布构建（启用 LTO）
cargo build --release

# 运行测试
cargo test

# 运行基准测试
cargo bench
```

## 依赖

- `apache-avro` - Avro 文件读写
- `serde` / `serde_json` - JSON 序列化
- `rustc-hash` - 高性能哈希表
- `smallvec` - 栈上小数组
- `thiserror` - 错误处理

## 许可证

Apache License 2.0

## 贡献

欢迎贡献！请参阅 [CONTRIBUTING.md](../../CONTRIBUTING.md)。
