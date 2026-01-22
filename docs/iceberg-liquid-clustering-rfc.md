# Apache Iceberg Liquid Clustering 特性需求文档 (RFC)

## 文档信息

| 项目 | 内容 |
|------|------|
| 文档版本 | v1.0 |
| 创建日期 | 2026-01-22 |
| 状态 | Draft |
| 参考实现 | Delta Lake 3.1+ Liquid Clustering |

---

## 1. 执行摘要

### 1.1 概述

本文档提出在 Apache Iceberg 中实现 **Liquid Clustering**（液态聚类）特性，这是一种自适应的数据布局优化技术，旨在：

- 简化数据布局决策，优化查询性能
- 支持在不重写历史数据的情况下动态修改聚类列
- 提供增量聚类能力，降低维护成本
- 替代或增强现有的分区和排序策略

### 1.2 目标用户

- 拥有大规模数据表且需要频繁优化查询性能的数据工程师
- 需要灵活调整数据布局策略的数据架构师
- 希望降低表维护成本的数据平台运维人员

### 1.3 与现有方案对比

| 特性 | Iceberg 分区 | Iceberg Sort Order | Liquid Clustering |
|------|--------------|-------------------|-------------------|
| 数据组织 | 物理目录 | 文件内排序 | ZCube 聚类单元 |
| 列修改复杂度 | 高（需重写） | 中（新文件生效） | 低（增量处理） |
| 高基数列支持 | 差 | 中 | 优秀 |
| 多列联合优化 | 有限 | 有限 | 优秀（Hilbert曲线） |
| 维护成本 | 高 | 低 | 低 |
| 查询优化效果 | 好（匹配时） | 中 | 好 |

---

## 2. 功能需求

### 2.1 核心功能

#### 2.1.1 FR-001: 聚类列定义

**描述**：支持在表级别定义聚类列（Clustering Columns）

**需求详情**：

| 需求ID | 描述 | 优先级 |
|--------|------|--------|
| FR-001-1 | 支持在 CREATE TABLE 时通过 `CLUSTER BY` 子句定义聚类列 | P0 |
| FR-001-2 | 支持最多 4 个聚类列 | P0 |
| FR-001-3 | 聚类列必须是表 Schema 中已存在的列 | P0 |
| FR-001-4 | 聚类列必须是支持统计信息收集的数据类型 | P0 |
| FR-001-5 | 聚类列不能与分区列同时定义 | P0 |
| FR-001-6 | 支持嵌套列作为聚类列（如 `struct.field`） | P1 |

**SQL 语法**：

```sql
-- 创建表时指定聚类列
CREATE TABLE catalog.db.events (
    event_id BIGINT,
    event_time TIMESTAMP,
    user_id STRING,
    event_type STRING,
    payload STRING
) USING iceberg
CLUSTER BY (user_id, event_type);

-- CTAS 语句
CREATE TABLE catalog.db.events_copy
CLUSTER BY (user_id)
AS SELECT * FROM catalog.db.events;
```

#### 2.1.2 FR-002: 聚类列修改

**描述**：支持在表创建后动态修改聚类列

**需求详情**：

| 需求ID | 描述 | 优先级 |
|--------|------|--------|
| FR-002-1 | 支持通过 `ALTER TABLE ... CLUSTER BY` 修改聚类列 | P0 |
| FR-002-2 | 修改聚类列不需要重写历史数据 | P0 |
| FR-002-3 | 支持通过 `CLUSTER BY NONE` 禁用聚类 | P0 |
| FR-002-4 | 支持在无分区表上启用聚类 | P1 |
| FR-002-5 | 聚类列变更需记录在 Snapshot 历史中 | P0 |

**SQL 语法**：

```sql
-- 修改聚类列
ALTER TABLE catalog.db.events
CLUSTER BY (event_time, user_id);

-- 禁用聚类
ALTER TABLE catalog.db.events
CLUSTER BY NONE;

-- 在现有表上启用聚类
ALTER TABLE catalog.db.legacy_table
CLUSTER BY (col1, col2);
```

#### 2.1.3 FR-003: 触发聚类操作

**描述**：支持通过 OPTIMIZE 命令触发数据聚类

**需求详情**：

| 需求ID | 描述 | 优先级 |
|--------|------|--------|
| FR-003-1 | 执行 `OPTIMIZE` 时自动应用聚类策略 | P0 |
| FR-003-2 | 支持增量聚类（仅处理需要聚类的文件） | P0 |
| FR-003-3 | 支持 `OPTIMIZE ... FULL` 进行全表重聚类 | P1 |
| FR-003-4 | 聚类操作与现有 Compaction 流程集成 | P0 |
| FR-003-5 | 支持并行聚类任务执行 | P1 |
| FR-003-6 | 支持聚类进度监控和统计信息 | P1 |

**SQL 语法**：

```sql
-- 增量聚类（默认）
CALL catalog.system.rewrite_data_files(
    table => 'db.events',
    strategy => 'clustering'
);

-- 或简化语法（需要新增）
OPTIMIZE catalog.db.events;

-- 全表重聚类
OPTIMIZE catalog.db.events FULL;

-- 带过滤条件的聚类
CALL catalog.system.rewrite_data_files(
    table => 'db.events',
    strategy => 'clustering',
    where => 'event_time >= current_date - interval 7 days'
);
```

#### 2.1.4 FR-004: 查询聚类信息

**描述**：支持查询表的聚类配置和状态

**需求详情**：

| 需求ID | 描述 | 优先级 |
|--------|------|--------|
| FR-004-1 | 通过 `DESCRIBE TABLE` 显示聚类列 | P0 |
| FR-004-2 | 通过系统表查询聚类统计信息 | P1 |
| FR-004-3 | 支持查询聚类覆盖率（已聚类文件占比） | P2 |

**SQL 语法**：

```sql
-- 查看表详情
DESCRIBE TABLE EXTENDED catalog.db.events;

-- 查询聚类统计
SELECT * FROM catalog.db.events.clustering_stats;
```

### 2.2 数据写入行为

#### 2.2.1 FR-005: 写入时行为

| 需求ID | 描述 | 优先级 |
|--------|------|--------|
| FR-005-1 | 新写入的数据文件默认为"未聚类"状态 | P0 |
| FR-005-2 | 写入操作不被聚类配置阻塞 | P0 |
| FR-005-3 | 支持写入时可选的局部排序优化 | P2 |

### 2.3 查询优化

#### 2.3.1 FR-006: 数据跳过优化

| 需求ID | 描述 | 优先级 |
|--------|------|--------|
| FR-006-1 | 查询规划器利用聚类列的统计信息进行文件裁剪 | P0 |
| FR-006-2 | 支持多列联合过滤的优化 | P0 |
| FR-006-3 | 聚类统计信息与现有 min/max 统计集成 | P0 |

---

## 3. 技术设计

### 3.1 整体架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Iceberg Liquid Clustering Architecture               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                         API Layer                                │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐    │   │
│  │  │  SQL Parser  │  │  Spark API   │  │  Flink/Trino API   │    │   │
│  │  │ CLUSTER BY   │  │ clusterBy()  │  │    (Extensions)    │    │   │
│  │  └──────────────┘  └──────────────┘  └────────────────────┘    │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                                    ▼                                    │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      Core Module                                 │   │
│  │  ┌────────────────────┐  ┌────────────────────────────────┐    │   │
│  │  │  ClusteringSpec    │  │     ClusteringMetadata         │    │   │
│  │  │  - columns         │  │  - clusteringColumns           │    │   │
│  │  │  - algorithm       │  │  - currentZCubeId              │    │   │
│  │  │  - targetSize      │  │  - statistics                  │    │   │
│  │  └────────────────────┘  └────────────────────────────────┘    │   │
│  │                                                                  │   │
│  │  ┌────────────────────────────────────────────────────────┐    │   │
│  │  │              ClusteringManager                          │    │   │
│  │  │  - selectFilesForClustering()                          │    │   │
│  │  │  - groupFilesIntoZCubes()                              │    │   │
│  │  │  - executeClusteringJob()                              │    │   │
│  │  └────────────────────────────────────────────────────────┘    │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                                    ▼                                    │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    Clustering Engine                             │   │
│  │  ┌────────────────┐  ┌─────────────────┐  ┌────────────────┐   │   │
│  │  │ HilbertCurve   │  │   ZOrderCurve   │  │  RangePartition │   │   │
│  │  │ (default 2+col)│  │   (1 column)    │  │     ID          │   │   │
│  │  └────────────────┘  └─────────────────┘  └────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                                    ▼                                    │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    Storage Layer                                 │   │
│  │  ┌────────────────────────────────────────────────────────┐    │   │
│  │  │                  Manifest Files                         │    │   │
│  │  │  DataFile {                                            │    │   │
│  │  │    path, size, recordCount,                            │    │   │
│  │  │    + clusteringProvider: "liquid",  // NEW             │    │   │
│  │  │    + zCubeId: "uuid",               // NEW             │    │   │
│  │  │    + zCubeColumns: ["col1","col2"]  // NEW             │    │   │
│  │  │  }                                                      │    │   │
│  │  └────────────────────────────────────────────────────────┘    │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.2 元数据模型设计

#### 3.2.1 Table Metadata 扩展

**新增字段**：

```json
{
  "format-version": 2,
  "table-uuid": "...",
  "location": "...",
  "schema": {...},
  "partition-spec": [...],
  "sort-order": {...},
  
  "clustering-spec": {
    "spec-id": 1,
    "columns": [
      {"source-id": 3, "name": "user_id"},
      {"source-id": 5, "name": "event_type"}
    ],
    "algorithm": "hilbert",
    "target-zcube-size-bytes": 536870912,
    "min-zcube-size-bytes": 134217728
  },
  
  "clustering-spec-history": [
    {
      "spec-id": 0,
      "columns": [],
      "timestamp-ms": 1700000000000
    },
    {
      "spec-id": 1,
      "columns": [{"source-id": 3}, {"source-id": 5}],
      "timestamp-ms": 1700100000000
    }
  ],
  
  "properties": {
    "clustering.enabled": "true",
    "clustering.provider": "liquid"
  }
}
```

#### 3.2.2 DataFile 扩展

**Manifest Entry 扩展字段**：

| 字段名 | 类型 | 描述 | 必需 |
|--------|------|------|------|
| `clustering_provider` | string | 聚类实现标识（如 "liquid"） | 否 |
| `zcube_id` | string (UUID) | 所属 ZCube 的唯一标识 | 否 |
| `zcube_columns` | list&lt;string&gt; | 聚类时使用的列名 | 否 |
| `clustering_spec_id` | int | 聚类时的 spec ID | 否 |

**Avro Schema 扩展**：

```avro
{
  "type": "record",
  "name": "data_file",
  "fields": [
    // ... 现有字段 ...
    
    {"name": "clustering_provider", "type": ["null", "string"], "default": null},
    {"name": "zcube_id", "type": ["null", "string"], "default": null},
    {"name": "zcube_columns", "type": ["null", {"type": "array", "items": "string"}], "default": null},
    {"name": "clustering_spec_id", "type": ["null", "int"], "default": null}
  ]
}
```

#### 3.2.3 ClusteringSpec 对象

```java
public interface ClusteringSpec {
    int specId();
    List<ClusteringColumn> columns();
    String algorithm();  // "hilbert" | "zorder"
    long targetZCubeSizeBytes();
    long minZCubeSizeBytes();
    
    // 常量
    int MAX_CLUSTERING_COLUMNS = 4;
    String DEFAULT_ALGORITHM = "hilbert";
    long DEFAULT_TARGET_ZCUBE_SIZE = 512 * 1024 * 1024;  // 512MB
    long DEFAULT_MIN_ZCUBE_SIZE = 128 * 1024 * 1024;     // 128MB
}

public interface ClusteringColumn {
    int sourceId();        // Schema 中的列 ID
    String name();         // 列名
    Type type();           // 数据类型
}
```

### 3.3 核心算法设计

#### 3.3.1 ZCube 管理

**ZCube 定义**：

```java
public class ZCube {
    private final String zCubeId;              // UUID
    private final List<String> clusteringColumns;
    private final List<DataFile> files;
    private final long totalSizeBytes;
    private final long recordCount;
    
    public boolean isComplete(long targetSize) {
        return totalSizeBytes >= targetSize;
    }
    
    public boolean needsReclustering(
        List<String> currentColumns, 
        long minSize
    ) {
        return !clusteringColumns.equals(currentColumns) 
            || totalSizeBytes < minSize;
    }
}
```

#### 3.3.2 文件选择策略

```java
public class ClusteringFileSelector {
    
    /**
     * 选择需要聚类的文件
     * 
     * 选择规则：
     * 1. 未聚类的文件（clustering_provider = null）
     * 2. 使用不同聚类列的文件（zcube_columns != current_columns）
     * 3. 属于不完整 ZCube 的文件（ZCube size < min_size）
     * 4. 排除：属于完整 ZCube 且列匹配的文件
     */
    public List<DataFile> selectFilesForClustering(
        TableScan scan,
        ClusteringSpec spec,
        boolean isFull  // OPTIMIZE FULL 标志
    ) {
        List<DataFile> candidates = new ArrayList<>();
        
        for (FileScanTask task : scan.planFiles()) {
            DataFile file = task.file();
            
            if (isFull) {
                // FULL 模式：选择所有文件
                candidates.add(file);
            } else {
                // 增量模式：智能选择
                if (shouldIncludeFile(file, spec)) {
                    candidates.add(file);
                }
            }
        }
        
        return filterByZCubeCompleteness(candidates, spec);
    }
    
    private boolean shouldIncludeFile(DataFile file, ClusteringSpec spec) {
        // 1. 未聚类的文件
        if (file.clusteringProvider() == null) {
            return true;
        }
        
        // 2. 不同提供者的文件（默认不处理，除非 FULL）
        if (!LIQUID_PROVIDER.equals(file.clusteringProvider())) {
            return false;
        }
        
        // 3. 聚类列不匹配
        List<String> currentColumns = spec.columnNames();
        if (!currentColumns.equals(file.zCubeColumns())) {
            return false;  // 增量模式下跳过，FULL 模式下会被上层选中
        }
        
        return true;
    }
}
```

#### 3.3.3 多维聚类算法

**Hilbert 曲线实现**：

```java
public class HilbertClustering implements ClusteringAlgorithm {
    
    private final int numRanges;  // 范围分区数，默认 1024
    
    @Override
    public DataFrame cluster(
        DataFrame df,
        List<String> columns,
        int targetNumFiles
    ) {
        // 1. 计算每列的 Range Partition ID
        List<Column> rangeIdCols = columns.stream()
            .map(col -> rangePartitionId(df.col(col), numRanges))
            .collect(toList());
        
        // 2. 计算 Hilbert Index
        int numBits = calculateNumBits(numRanges);
        Column hilbertIndex = hilbertIndex(numBits, rangeIdCols);
        
        // 3. Range Partition 并排序
        return df
            .withColumn("_hilbert_key", hilbertIndex)
            .repartitionByRange(targetNumFiles, col("_hilbert_key"))
            .sortWithinPartitions("_hilbert_key")
            .drop("_hilbert_key");
    }
    
    /**
     * 计算 Range Partition ID
     * 将列值映射到 [0, numRanges) 的整数范围
     */
    private Column rangePartitionId(Column col, int numRanges) {
        // 使用采样获取近似分位数，然后计算 range ID
        return callUDF("range_partition_id", col, lit(numRanges));
    }
    
    /**
     * 计算多维 Hilbert 索引
     */
    private Column hilbertIndex(int numBits, List<Column> rangeIdCols) {
        return callUDF("hilbert_index", lit(numBits), array(rangeIdCols));
    }
}
```

**Z-Order 曲线实现（单列或备选）**：

```java
public class ZOrderClustering implements ClusteringAlgorithm {
    
    @Override
    public DataFrame cluster(
        DataFrame df,
        List<String> columns,
        int targetNumFiles
    ) {
        List<Column> rangeIdCols = columns.stream()
            .map(col -> rangePartitionId(df.col(col), numRanges))
            .collect(toList());
        
        // 交错位运算生成 Z-Value
        Column zValue = interleaveBits(rangeIdCols);
        
        return df
            .withColumn("_zorder_key", zValue)
            .repartitionByRange(targetNumFiles, col("_zorder_key"))
            .sortWithinPartitions("_zorder_key")
            .drop("_zorder_key");
    }
}
```

#### 3.3.4 增量聚类流程

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Incremental Clustering Flow                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐                                                   │
│  │   START      │                                                   │
│  └──────┬───────┘                                                   │
│         │                                                            │
│         ▼                                                            │
│  ┌──────────────────────────────────────┐                           │
│  │  1. Load current clustering spec     │                           │
│  │     - columns, algorithm, sizes      │                           │
│  └──────────────┬───────────────────────┘                           │
│                 │                                                    │
│                 ▼                                                    │
│  ┌──────────────────────────────────────┐                           │
│  │  2. Scan all data files              │                           │
│  │     - Group by ZCube ID              │                           │
│  │     - Calculate ZCube sizes          │                           │
│  └──────────────┬───────────────────────┘                           │
│                 │                                                    │
│                 ▼                                                    │
│  ┌──────────────────────────────────────┐                           │
│  │  3. Select candidate files           │                           │
│  │     ┌─────────────────────────────┐  │                           │
│  │     │ Include:                    │  │                           │
│  │     │ - Unclustered files         │  │                           │
│  │     │ - Files in small ZCubes     │  │                           │
│  │     │   (size < min_zcube_size)   │  │                           │
│  │     ├─────────────────────────────┤  │                           │
│  │     │ Exclude:                    │  │                           │
│  │     │ - Files in complete ZCubes  │  │                           │
│  │     │ - Different provider files  │  │                           │
│  │     │ - Single complete ZCube     │  │                           │
│  │     └─────────────────────────────┘  │                           │
│  └──────────────┬───────────────────────┘                           │
│                 │                                                    │
│                 ▼                                                    │
│  ┌──────────────────────────────────────┐                           │
│  │  4. Group files into bins            │                           │
│  │     - Bin size ≈ target_zcube_size   │                           │
│  │     - Each bin → one clustering job  │                           │
│  └──────────────┬───────────────────────┘                           │
│                 │                                                    │
│                 ▼                                                    │
│  ┌──────────────────────────────────────┐                           │
│  │  5. Execute clustering jobs          │                           │
│  │     For each bin:                    │                           │
│  │     a. Read files into DataFrame     │                           │
│  │     b. Apply Hilbert/ZOrder cluster  │                           │
│  │     c. Write new files with:         │                           │
│  │        - clustering_provider=liquid  │                           │
│  │        - zcube_id=new_uuid           │                           │
│  │        - zcube_columns=[...]         │                           │
│  └──────────────┬───────────────────────┘                           │
│                 │                                                    │
│                 ▼                                                    │
│  ┌──────────────────────────────────────┐                           │
│  │  6. Commit transaction               │                           │
│  │     - Delete old files               │                           │
│  │     - Add new clustered files        │                           │
│  │     - Update snapshot                │                           │
│  └──────────────┬───────────────────────┘                           │
│                 │                                                    │
│                 ▼                                                    │
│  ┌──────────────┐                                                   │
│  │     END      │                                                   │
│  └──────────────┘                                                   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 3.4 Spark 集成设计

#### 3.4.1 SparkActions 扩展

```java
public class ClusteringAction implements Action<ClusteringAction, ClusteringResult> {
    
    private final Table table;
    private final SparkSession spark;
    
    private boolean isFull = false;
    private Expression filter = Expressions.alwaysTrue();
    private int maxConcurrentJobs = 4;
    
    public ClusteringAction full() {
        this.isFull = true;
        return this;
    }
    
    public ClusteringAction filter(Expression expr) {
        this.filter = expr;
        return this;
    }
    
    @Override
    public ClusteringResult execute() {
        ClusteringSpec spec = table.clusteringSpec();
        if (spec == null || spec.columns().isEmpty()) {
            throw new ValidationException("Table has no clustering spec defined");
        }
        
        // 1. 选择文件
        List<DataFile> candidates = selectFiles(spec, isFull, filter);
        
        if (candidates.isEmpty()) {
            return ClusteringResult.empty();
        }
        
        // 2. 分组为 Bins
        List<List<DataFile>> bins = groupIntoBins(candidates, spec.targetZCubeSizeBytes());
        
        // 3. 并行执行聚类任务
        List<ClusteringJobResult> jobResults = bins.parallelStream()
            .map(bin -> executeClusteringJob(bin, spec))
            .collect(toList());
        
        // 4. 提交事务
        commitChanges(jobResults);
        
        return buildResult(jobResults);
    }
    
    private ClusteringJobResult executeClusteringJob(
        List<DataFile> files,
        ClusteringSpec spec
    ) {
        String newZCubeId = UUID.randomUUID().toString();
        
        // 读取数据
        DataFrame df = readFiles(files);
        
        // 应用聚类算法
        ClusteringAlgorithm algorithm = getAlgorithm(spec.algorithm());
        int targetNumFiles = calculateTargetFiles(files, spec.targetZCubeSizeBytes());
        DataFrame clusteredDf = algorithm.cluster(df, spec.columnNames(), targetNumFiles);
        
        // 写入新文件
        List<DataFile> newFiles = writeFiles(
            clusteredDf,
            newZCubeId,
            spec.specId(),
            spec.columnNames()
        );
        
        return new ClusteringJobResult(files, newFiles);
    }
}
```

#### 3.4.2 SQL 扩展

```java
// Spark SQL 扩展
public class IcebergClusteringExtensions implements SparkSessionExtensions {
    
    @Override
    public void apply(SparkSessionExtensions extensions) {
        // 注册 CLUSTER BY 语法解析
        extensions.injectParser((session, parser) -> 
            new IcebergClusteringSqlParser(parser));
        
        // 注册 OPTIMIZE 命令
        extensions.injectResolutionRule(session -> 
            new ResolveOptimizeCommand(session));
    }
}

// SQL 语法解析
public class IcebergClusteringSqlParser extends ParserInterface {
    
    // CREATE TABLE ... CLUSTER BY (col1, col2)
    // ALTER TABLE ... CLUSTER BY (col1, col2)
    // ALTER TABLE ... CLUSTER BY NONE
    // OPTIMIZE table [FULL]
}
```

### 3.5 查询优化集成

#### 3.5.1 数据跳过优化

```java
public class ClusteringAwareFilePruning implements Rule<LogicalPlan> {
    
    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (!(plan instanceof Filter)) {
            return plan;
        }
        
        Filter filter = (Filter) plan;
        Expression predicate = filter.condition();
        
        // 检查过滤条件是否涉及聚类列
        Set<String> clusteringColumns = getClusteringColumns(filter.child());
        Set<String> filterColumns = extractColumns(predicate);
        
        if (!clusteringColumns.isEmpty() && 
            !Collections.disjoint(clusteringColumns, filterColumns)) {
            // 启用增强的文件裁剪
            return applyClusteringAwarePruning(filter, clusteringColumns);
        }
        
        return plan;
    }
}
```

---

## 4. API 设计

### 4.1 Java API

```java
// Table 接口扩展
public interface Table {
    // 现有方法...
    
    /** 获取当前聚类规格 */
    ClusteringSpec clusteringSpec();
    
    /** 更新聚类规格 */
    UpdateClusteringSpec updateClusteringSpec();
}

// 更新聚类规格的 Builder
public interface UpdateClusteringSpec extends PendingUpdate<Table> {
    UpdateClusteringSpec clusterBy(String... columns);
    UpdateClusteringSpec clusterByNone();
    UpdateClusteringSpec algorithm(String algorithm);
    UpdateClusteringSpec targetZCubeSize(long bytes);
}

// 使用示例
Table table = catalog.loadTable(TableIdentifier.of("db", "events"));

// 设置聚类列
table.updateClusteringSpec()
    .clusterBy("user_id", "event_type")
    .algorithm("hilbert")
    .targetZCubeSize(512 * 1024 * 1024)
    .commit();

// 执行聚类
Actions.forTable(table)
    .clusteringAction()
    .filter(Expressions.greaterThan("event_time", "2024-01-01"))
    .execute();

// 全表重聚类
Actions.forTable(table)
    .clusteringAction()
    .full()
    .execute();
```

### 4.2 Spark DataFrame API

```scala
// Scala API
import org.apache.iceberg.spark.extensions._

// 创建聚类表
spark.sql("""
  CREATE TABLE catalog.db.events (
    event_id BIGINT,
    user_id STRING,
    event_type STRING
  ) USING iceberg
  CLUSTER BY (user_id, event_type)
""")

// 或使用 DataFrame API
df.writeTo("catalog.db.events")
  .clusterBy("user_id", "event_type")
  .create()

// 修改聚类列
spark.sql("ALTER TABLE catalog.db.events CLUSTER BY (event_type)")

// 触发聚类
spark.sql("OPTIMIZE catalog.db.events")

// 或使用 Procedure
spark.sql("""
  CALL catalog.system.rewrite_data_files(
    table => 'db.events',
    strategy => 'clustering'
  )
""")
```

### 4.3 REST Catalog API 扩展

```yaml
# 新增 API 端点

# 获取聚类规格
GET /v1/namespaces/{namespace}/tables/{table}/clustering-spec
Response:
  {
    "spec-id": 1,
    "columns": ["user_id", "event_type"],
    "algorithm": "hilbert",
    "target-zcube-size-bytes": 536870912
  }

# 更新聚类规格
POST /v1/namespaces/{namespace}/tables/{table}/clustering-spec
Request:
  {
    "columns": ["user_id", "event_type"],
    "algorithm": "hilbert"
  }
Response:
  {
    "spec-id": 2,
    "columns": ["user_id", "event_type"],
    "algorithm": "hilbert"
  }

# 触发聚类操作
POST /v1/namespaces/{namespace}/tables/{table}/clustering/execute
Request:
  {
    "full": false,
    "filter": "event_time > '2024-01-01'"
  }
Response:
  {
    "job-id": "clustering-job-123",
    "status": "RUNNING"
  }
```

---

## 5. 配置参数

### 5.1 表级配置

| 配置项 | 类型 | 默认值 | 描述 |
|--------|------|--------|------|
| `clustering.enabled` | boolean | false | 是否启用聚类 |
| `clustering.columns` | string | - | 聚类列，逗号分隔 |
| `clustering.algorithm` | string | hilbert | 聚类算法：hilbert/zorder |
| `clustering.target-zcube-size-bytes` | long | 536870912 (512MB) | 目标 ZCube 大小 |
| `clustering.min-zcube-size-bytes` | long | 134217728 (128MB) | 最小 ZCube 大小阈值 |
| `clustering.max-columns` | int | 4 | 最大聚类列数 |

### 5.2 Session 配置

| 配置项 | 类型 | 默认值 | 描述 |
|--------|------|--------|------|
| `spark.sql.iceberg.clustering.range-partitions` | int | 1024 | Range Partition 数量 |
| `spark.sql.iceberg.clustering.add-noise` | boolean | true | 是否添加随机噪声处理倾斜 |
| `spark.sql.iceberg.clustering.max-concurrent-jobs` | int | 4 | 最大并行聚类任务数 |
| `spark.sql.iceberg.clustering.sort-within-files` | boolean | true | 是否在文件内排序 |

---

## 6. 兼容性设计

### 6.1 向后兼容性

| 场景 | 行为 |
|------|------|
| 旧版 Reader 读取聚类表 | ✅ 正常读取，忽略聚类元数据 |
| 旧版 Writer 写入聚类表 | ✅ 正常写入，新文件为"未聚类"状态 |
| 新版读取非聚类表 | ✅ 正常读取 |
| 降级聚类规格 | ✅ 支持 CLUSTER BY NONE |

### 6.2 Format Version 兼容性

| Format Version | 支持情况 |
|----------------|----------|
| v1 | ❌ 不支持（需要扩展的 manifest 字段） |
| v2 | ✅ 支持 |
| v3 (future) | ✅ 支持 |

### 6.3 引擎兼容性

| 引擎 | 读取支持 | 写入支持 | 聚类操作支持 |
|------|----------|----------|--------------|
| Spark 3.3+ | ✅ | ✅ | ✅ |
| Flink 1.16+ | ✅ | ✅ | 🔶 (Roadmap) |
| Trino 400+ | ✅ | ✅ | 🔶 (Roadmap) |
| Presto | ✅ | ✅ | ❌ |
| Hive | ✅ | ✅ | ❌ |

---

## 7. 测试计划

### 7.1 单元测试

| 测试类别 | 测试项 |
|----------|--------|
| ClusteringSpec | 创建、序列化、反序列化、验证 |
| ClusteringColumn | 列解析、物理/逻辑名转换 |
| ZCube | 创建、合并、大小计算 |
| HilbertClustering | 索引计算、数据分布 |
| ZOrderClustering | 交错位运算、数据分布 |
| FileSelector | 文件选择逻辑、ZCube 过滤 |

### 7.2 集成测试

| 测试场景 | 描述 |
|----------|------|
| E2E_CREATE_CLUSTER | 创建聚类表，写入数据，验证元数据 |
| E2E_ALTER_CLUSTER | 修改聚类列，验证历史数据不变 |
| E2E_OPTIMIZE_INCREMENTAL | 增量聚类，验证只处理必要文件 |
| E2E_OPTIMIZE_FULL | 全表重聚类，验证所有文件更新 |
| E2E_QUERY_SKIPPING | 验证查询数据跳过效果 |
| E2E_CONCURRENT_WRITE | 并发写入时的聚类行为 |
| E2E_SCHEMA_EVOLUTION | Schema 演进对聚类的影响 |

### 7.3 性能测试

| 测试场景 | 指标 |
|----------|------|
| 聚类操作性能 | 吞吐量（GB/min），延迟 |
| 查询性能提升 | 有/无聚类的查询时间对比 |
| 数据跳过效率 | 文件裁剪比例 |
| 增量聚类效率 | 处理文件数与总文件数比例 |

---

## 8. 实施计划

### 8.1 里程碑

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Implementation Roadmap                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Phase 1: Foundation (8 weeks)                                       │
│  ├── Week 1-2: Metadata model design & implementation               │
│  │   - ClusteringSpec, ClusteringColumn classes                     │
│  │   - Table metadata serialization/deserialization                 │
│  │   - DataFile extension fields                                    │
│  ├── Week 3-4: Core clustering algorithms                           │
│  │   - RangePartitionId UDF                                         │
│  │   - HilbertClustering implementation                             │
│  │   - ZOrderClustering implementation                              │
│  ├── Week 5-6: File selection & ZCube management                    │
│  │   - ClusteringFileSelector                                       │
│  │   - ZCube grouping logic                                         │
│  │   - Incremental selection strategy                               │
│  └── Week 7-8: Basic Spark integration                              │
│      - ClusteringAction                                             │
│      - Basic SQL parser (CLUSTER BY)                                │
│      - Unit tests                                                   │
│                                                                      │
│  Phase 2: Full Spark Support (6 weeks)                              │
│  ├── Week 9-10: Complete SQL support                                │
│  │   - CREATE TABLE ... CLUSTER BY                                  │
│  │   - ALTER TABLE ... CLUSTER BY                                   │
│  │   - OPTIMIZE command                                             │
│  ├── Week 11-12: Query optimization                                 │
│  │   - ClusteringAwareFilePruning                                   │
│  │   - Statistics integration                                       │
│  └── Week 13-14: Integration tests & documentation                  │
│      - E2E tests                                                    │
│      - Performance benchmarks                                       │
│      - User documentation                                           │
│                                                                      │
│  Phase 3: Extended Engine Support (4 weeks)                         │
│  ├── Week 15-16: Flink integration                                  │
│  └── Week 17-18: Trino integration                                  │
│                                                                      │
│  Phase 4: Production Readiness (4 weeks)                            │
│  ├── Week 19-20: Performance optimization                           │
│  └── Week 21-22: GA release preparation                             │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 8.2 依赖关系

```
                    ┌──────────────────┐
                    │  Metadata Model  │
                    │  (ClusteringSpec)│
                    └────────┬─────────┘
                             │
            ┌────────────────┼────────────────┐
            │                │                │
            ▼                ▼                ▼
    ┌───────────────┐ ┌───────────────┐ ┌───────────────┐
    │   Clustering  │ │     File      │ │    ZCube      │
    │   Algorithms  │ │   Selection   │ │   Manager     │
    └───────┬───────┘ └───────┬───────┘ └───────┬───────┘
            │                 │                 │
            └────────────────┬┴─────────────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ ClusteringAction │
                    │  (Spark Action)  │
                    └────────┬─────────┘
                             │
            ┌────────────────┼────────────────┐
            │                │                │
            ▼                ▼                ▼
    ┌───────────────┐ ┌───────────────┐ ┌───────────────┐
    │  SQL Parser   │ │    Query      │ │    REST       │
    │   Extension   │ │  Optimizer    │ │    API        │
    └───────────────┘ └───────────────┘ └───────────────┘
```

---

## 9. 风险与缓解

| 风险 | 影响 | 可能性 | 缓解措施 |
|------|------|--------|----------|
| 元数据格式变更导致兼容性问题 | 高 | 中 | 使用可选字段，保持向后兼容 |
| 大表聚类操作耗时过长 | 中 | 高 | 支持增量聚类，并行执行，可中断 |
| 并发写入与聚类操作冲突 | 中 | 中 | 使用乐观并发控制，冲突重试 |
| 聚类效果不理想 | 中 | 中 | 提供统计信息，支持算法调优 |
| 跨引擎行为不一致 | 低 | 中 | 详细规范，兼容性测试矩阵 |

---

## 10. 附录

### 10.1 术语表

| 术语 | 定义 |
|------|------|
| Liquid Clustering | 自适应数据布局优化技术 |
| ZCube | 聚类单元，一组共享相同聚类属性的文件 |
| Clustering Column | 用于数据聚类的列 |
| Hilbert Curve | 一种保持局部性的空间填充曲线 |
| Z-Order Curve | 交错位编码的多维索引曲线 |
| Range Partition ID | 将列值映射到分区 ID 的函数 |
| Data Skipping | 基于统计信息跳过不相关文件的查询优化 |

### 10.2 参考资料

1. Delta Lake Liquid Clustering 实现
   - [Delta Lake Protocol - Clustered Table](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#clustered-table)
   - [Delta Lake Source Code](https://github.com/delta-io/delta)

2. 空间填充曲线
   - [Hilbert Curve Wikipedia](https://en.wikipedia.org/wiki/Hilbert_curve)
   - [Z-Order Curve Wikipedia](https://en.wikipedia.org/wiki/Z-order_curve)

3. Iceberg 文档
   - [Apache Iceberg Spec](https://iceberg.apache.org/spec/)
   - [Iceberg Table Maintenance](https://iceberg.apache.org/docs/latest/maintenance/)

### 10.3 与 Delta Lake 实现的对应关系

| Delta Lake 组件 | Iceberg 对应设计 |
|-----------------|------------------|
| `ClusteringTableFeature` | Table Property `clustering.enabled` |
| `ClusteringMetadataDomain` | `clustering-spec` in Table Metadata |
| `ClusteringColumn` | `ClusteringColumn` (相同设计) |
| `ZCube` / `ZCubeInfo` | `ZCube` (相同设计) |
| `AddFile.clusteringProvider` | `DataFile.clusteringProvider` |
| `AddFile.tags[ZCUBE_ID]` | `DataFile.zCubeId` |
| `OptimizeTableCommand` | `ClusteringAction` |
| `ClusteringStrategy` | `ClusteringAlgorithm` interface |
| `HilbertClustering` | `HilbertClustering` (相同算法) |
| `ZOrderClustering` | `ZOrderClustering` (相同算法) |

---

**文档结束**
