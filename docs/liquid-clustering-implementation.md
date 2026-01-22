# Apache Iceberg Liquid Clustering 实现文档

## 目录

- [1. 概述](#1-概述)
- [2. 架构设计](#2-架构设计)
- [3. API 模块实现](#3-api-模块实现)
- [4. Core 模块实现](#4-core-模块实现)
- [5. Spark 模块实现](#5-spark-模块实现)
- [6. 使用指南](#6-使用指南)
- [7. 配置参数](#7-配置参数)
- [8. 最佳实践](#8-最佳实践)
- [9. 与现有功能对比](#9-与现有功能对比)

---

## 1. 概述

### 1.1 什么是 Liquid Clustering

Liquid Clustering 是 Apache Iceberg 的一种自适应数据布局优化技术，借鉴了 Delta Lake 的设计理念。它提供了一种灵活、增量的数据组织方式，无需预定义分区策略即可优化查询性能。

### 1.2 核心特性

| 特性 | 描述 |
|------|------|
| **动态聚类列** | 支持运行时修改聚类列，无需重写历史数据 |
| **增量优化** | 只处理需要优化的数据文件，减少维护成本 |
| **多维聚类** | 使用 Hilbert/Z-Order 曲线实现多列数据聚类 |
| **ZCube 管理** | 将数据文件组织为逻辑聚类单元 |
| **透明查询优化** | 自动利用聚类统计信息进行文件裁剪 |

### 1.3 与传统分区的对比

| 特性 | 传统分区 | Liquid Clustering |
|------|----------|-------------------|
| 列选择时机 | 建表时确定 | 随时可变 |
| 列数量 | 通常1-2列 | 最多4列 |
| 修改成本 | 需要重写全表 | 增量生效 |
| 数据组织 | 按分区值 | 按空间填充曲线 |
| 小文件问题 | 高基数列易产生 | 自动合并 |

---

## 2. 架构设计

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Spark SQL Layer                              │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │ ANTLR Grammar (IcebergSqlExtensions.g4)                         ││
│  │ - ALTER TABLE ... CLUSTER BY                                     ││
│  │ - OPTIMIZE TABLE                                                 ││
│  └─────────────────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │ Logical Plans & Execution                                       ││
│  │ - SetClusteringColumns / SetClusteringColumnsExec               ││
│  │ - OptimizeTable / OptimizeTableExec                             ││
│  └─────────────────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │ Spark Actions                                                   ││
│  │ - SparkHilbertUDF / SparkHilbertFileRewriteRunner              ││
│  │ - RewriteDataFilesSparkAction.hilbert()                        ││
│  └─────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         Core Layer                                   │
│  ┌───────────────────┐  ┌───────────────────┐  ┌─────────────────┐ │
│  │  ClusteringSpec   │  │ ClusteringSpec    │  │ BaseUpdate      │ │
│  │  Parser           │  │ (API)             │  │ ClusteringSpec  │ │
│  └───────────────────┘  └───────────────────┘  └─────────────────┘ │
│  ┌───────────────────┐  ┌───────────────────┐  ┌─────────────────┐ │
│  │ TableMetadata     │  │ Table Properties  │  │ DataFile        │ │
│  │ Parser            │  │                   │  │ (Clustering)    │ │
│  └───────────────────┘  └───────────────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Storage Layer (Data Files)                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
│  │   ZCube 1   │  │   ZCube 2   │  │   ZCube 3   │  ...            │
│  │ (clustered) │  │ (clustered) │  │ (clustered) │                 │
│  └─────────────┘  └─────────────┘  └─────────────┘                 │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 核心概念

#### ZCube (聚类单元)

ZCube 是 Liquid Clustering 中的逻辑数据组织单元：

- **定义**: 一组具有相似聚类属性的数据文件
- **目标大小**: 默认 512MB
- **最小大小**: 默认 128MB
- **属性**: 包含 ZCube ID、聚类列、聚类规范 ID

#### 聚类算法

| 算法 | 描述 | 适用场景 |
|------|------|----------|
| **Hilbert** | Hilbert 空间填充曲线 | 2-4 维数据，更好的局部性保持 |
| **Z-Order** | Z-Order (Morton) 曲线 | 通用场景，与现有 Z-Order 实现兼容 |

### 2.3 数据流

```
[写入数据] → [新文件标记为 unclustered] → [OPTIMIZE 触发]
                                              │
                    ┌─────────────────────────┴─────────────────────────┐
                    │                                                    │
                    ▼                                                    ▼
            [增量聚类]                                            [完全重聚类]
                    │                                                    │
                    ▼                                                    ▼
        [选择 unclustered                                    [选择所有数据文件]
         或 spec 不匹配的文件]                                          │
                    │                                                    │
                    └─────────────────────────┬─────────────────────────┘
                                              │
                                              ▼
                              [按 Hilbert/Z-Order 曲线排序]
                                              │
                                              ▼
                              [重写为新的 clustered 文件]
                                              │
                                              ▼
                              [更新文件元数据 (ZCube ID 等)]
                                              │
                                              ▼
                              [提交事务]
```

---

## 3. API 模块实现

### 3.1 ClusteringSpec 接口

**文件**: `api/src/main/java/org/apache/iceberg/ClusteringSpec.java`

```java
public class ClusteringSpec implements Serializable {
    
    // 聚类算法枚举
    public enum Algorithm {
        HILBERT("hilbert"),  // Hilbert 空间填充曲线
        ZORDER("zorder");    // Z-Order 曲线
    }
    
    // 常量
    public static final int MAX_CLUSTERING_COLUMNS = 4;
    public static final Algorithm DEFAULT_ALGORITHM = Algorithm.HILBERT;
    public static final long DEFAULT_TARGET_ZCUBE_SIZE_BYTES = 512L * 1024 * 1024;
    public static final long DEFAULT_MIN_ZCUBE_SIZE_BYTES = 128L * 1024 * 1024;
    
    // 核心方法
    public int specId();                        // 获取规范 ID
    public List<ClusteringColumn> columns();    // 获取聚类列
    public Algorithm algorithm();               // 获取聚类算法
    public long targetZCubeSizeBytes();         // 获取目标 ZCube 大小
    public long minZCubeSizeBytes();            // 获取最小 ZCube 大小
    public boolean isClustered();               // 是否启用聚类
    
    // 静态工厂方法
    public static ClusteringSpec unclustered();
    public static Builder builderFor(Schema schema);
    public static ClusteringSpec fromProperties(Schema schema, Map<String, String> properties);
}
```

### 3.2 ClusteringColumn 类

**文件**: `api/src/main/java/org/apache/iceberg/ClusteringColumn.java`

```java
public class ClusteringColumn implements Serializable {
    private final int sourceId;    // Schema 中的字段 ID
    private final String name;     // 列名
    
    public ClusteringColumn(int sourceId, String name);
    public int sourceId();
    public String name();
}
```

### 3.3 UpdateClusteringSpec 接口

**文件**: `api/src/main/java/org/apache/iceberg/UpdateClusteringSpec.java`

```java
public interface UpdateClusteringSpec extends PendingUpdate<ClusteringSpec> {
    
    // 设置聚类列
    UpdateClusteringSpec clusterBy(String... columns);
    
    // 禁用聚类
    UpdateClusteringSpec clusterByNone();
    
    // 设置聚类算法
    UpdateClusteringSpec algorithm(String algorithm);
    
    // 设置目标 ZCube 大小
    UpdateClusteringSpec targetZCubeSize(long sizeBytes);
    
    // 设置最小 ZCube 大小
    UpdateClusteringSpec minZCubeSize(long sizeBytes);
}
```

### 3.4 Table 接口扩展

**文件**: `api/src/main/java/org/apache/iceberg/Table.java`

```java
public interface Table {
    // ... 现有方法 ...
    
    /**
     * 返回当前表的聚类规范
     */
    default ClusteringSpec clusteringSpec() {
        throw new UnsupportedOperationException("Clustering spec is not supported");
    }
    
    /**
     * 创建更新聚类规范的操作
     */
    default UpdateClusteringSpec updateClusteringSpec() {
        throw new UnsupportedOperationException("Updating clustering spec is not supported");
    }
}
```

### 3.5 DataFile/ContentFile 扩展

**文件**: `api/src/main/java/org/apache/iceberg/DataFile.java`

```java
public interface DataFile extends ContentFile<DataFile> {
    // ... 现有字段 ...
    
    // Liquid Clustering 字段
    Types.NestedField CLUSTERING_PROVIDER = optional(146, "clustering_provider", StringType.get());
    Types.NestedField ZCUBE_ID = optional(147, "zcube_id", StringType.get());
    Types.NestedField ZCUBE_COLUMNS = optional(148, "zcube_columns", ListType.ofRequired(149, StringType.get()));
    Types.NestedField CLUSTERING_SPEC_ID = optional(150, "clustering_spec_id", IntegerType.get());
}
```

**文件**: `api/src/main/java/org/apache/iceberg/ContentFile.java`

```java
public interface ContentFile<F> {
    // ... 现有方法 ...
    
    default String clusteringProvider() { return null; }
    default String zCubeId() { return null; }
    default List<String> zCubeColumns() { return null; }
    default Integer clusteringSpecId() { return null; }
}
```

---

## 4. Core 模块实现

### 4.1 ClusteringSpecParser

**文件**: `core/src/main/java/org/apache/iceberg/ClusteringSpecParser.java`

JSON 序列化格式：

```json
{
  "spec-id": 1,
  "columns": [
    {"source-id": 1, "name": "user_id"},
    {"source-id": 2, "name": "event_time"}
  ],
  "algorithm": "hilbert",
  "target-zcube-size-bytes": 536870912,
  "min-zcube-size-bytes": 134217728
}
```

### 4.2 TableMetadataParser 集成

**文件**: `core/src/main/java/org/apache/iceberg/TableMetadataParser.java`

```java
public class TableMetadataParser {
    // 新增字段
    static final String CLUSTERING_SPECS = "clustering-specs";
    static final String DEFAULT_CLUSTERING_SPEC_ID = "default-clustering-spec-id";
    
    // toJson() 方法中添加
    public static void toJson(TableMetadata metadata, JsonGenerator generator) {
        // ... 现有代码 ...
        
        // 写入聚类规范
        if (metadata.defaultClusteringSpecId() != ClusteringSpec.UNCLUSTERED_SPEC_ID
            || !metadata.clusteringSpecs().isEmpty()) {
            generator.writeNumberField(DEFAULT_CLUSTERING_SPEC_ID, metadata.defaultClusteringSpecId());
            generator.writeArrayFieldStart(CLUSTERING_SPECS);
            for (ClusteringSpec spec : metadata.clusteringSpecs()) {
                ClusteringSpecParser.toJson(spec, generator);
            }
            generator.writeEndArray();
        }
    }
}
```

### 4.3 BaseUpdateClusteringSpec 实现

**文件**: `core/src/main/java/org/apache/iceberg/BaseUpdateClusteringSpec.java`

```java
class BaseUpdateClusteringSpec implements UpdateClusteringSpec {
    private final TableOperations ops;
    private final Schema schema;
    
    private List<String> clusteringColumns = null;
    private String algorithm = null;
    private Long targetZCubeSizeBytes = null;
    private Long minZCubeSizeBytes = null;
    private boolean disableClustering = false;
    
    @Override
    public void commit() {
        UpdateProperties updateProperties = new PropertiesUpdate(ops);
        
        if (disableClustering) {
            updateProperties.set(TableProperties.CLUSTERING_ENABLED, "false");
            updateProperties.remove(TableProperties.CLUSTERING_COLUMNS);
        } else if (clusteringColumns != null) {
            updateProperties.set(TableProperties.CLUSTERING_ENABLED, "true");
            updateProperties.set(TableProperties.CLUSTERING_COLUMNS, 
                                 String.join(",", clusteringColumns));
            // ... 设置其他属性
        }
        
        updateProperties.commit();
    }
}
```

### 4.4 TableProperties 常量

**文件**: `core/src/main/java/org/apache/iceberg/TableProperties.java`

```java
public class TableProperties {
    // Liquid Clustering 配置
    public static final String CLUSTERING_ENABLED = "clustering.enabled";
    public static final boolean CLUSTERING_ENABLED_DEFAULT = false;
    
    public static final String CLUSTERING_COLUMNS = "clustering.columns";
    
    public static final String CLUSTERING_ALGORITHM = "clustering.algorithm";
    public static final String CLUSTERING_ALGORITHM_DEFAULT = "hilbert";
    
    public static final String CLUSTERING_TARGET_ZCUBE_SIZE_BYTES = "clustering.target-zcube-size-bytes";
    public static final long CLUSTERING_TARGET_ZCUBE_SIZE_BYTES_DEFAULT = 512L * 1024 * 1024;
    
    public static final String CLUSTERING_MIN_ZCUBE_SIZE_BYTES = "clustering.min-zcube-size-bytes";
    public static final long CLUSTERING_MIN_ZCUBE_SIZE_BYTES_DEFAULT = 128L * 1024 * 1024;
    
    public static final String CLUSTERING_MAX_COLUMNS = "clustering.max-columns";
    public static final int CLUSTERING_MAX_COLUMNS_DEFAULT = 4;
    
    public static final String CLUSTERING_PROVIDER = "clustering.provider";
    public static final String CLUSTERING_PROVIDER_LIQUID = "liquid";
}
```

### 4.5 BaseTable 实现

**文件**: `core/src/main/java/org/apache/iceberg/BaseTable.java`

```java
public class BaseTable implements Table, HasTableOperations, Serializable {
    
    @Override
    public ClusteringSpec clusteringSpec() {
        return ClusteringSpec.fromProperties(
            ops.current().schema(), 
            ops.current().properties()
        );
    }
    
    @Override
    public UpdateClusteringSpec updateClusteringSpec() {
        return new BaseUpdateClusteringSpec(ops);
    }
}
```

---

## 5. Spark 模块实现

### 5.1 ANTLR 语法扩展

**文件**: `spark/v4.0/spark-extensions/src/main/antlr/.../IcebergSqlExtensions.g4`

```antlr
statement
    : // ... 现有规则 ...
    | ALTER TABLE multipartIdentifier CLUSTER BY clusterBySpec        #setClusteringColumns
    | OPTIMIZE multipartIdentifier (FULL)?                            #optimizeTable
    ;

clusterBySpec
    : NONE                                                            #clusterByNone
    | '(' columns+=multipartIdentifier (',' columns+=multipartIdentifier)* ')' 
      (clusteringOptions)?                                            #clusterByColumns
    ;

clusteringOptions
    : WITH clusteringOption (clusteringOption)*
    ;

clusteringOption
    : ALGORITHM '=' algorithmName=identifier
    | TARGET SIZE number sizeUnit
    | MIN SIZE number sizeUnit
    ;

sizeUnit
    : BYTES | KB | MB | GB
    ;

// 关键字
CLUSTER: 'CLUSTER';
NONE: 'NONE';
OPTIMIZE: 'OPTIMIZE';
FULL: 'FULL';
ALGORITHM: 'ALGORITHM';
TARGET: 'TARGET';
MIN: 'MIN';
SIZE: 'SIZE';
BYTES: 'BYTES';
KB: 'KB';
MB: 'MB';
GB: 'GB';
```

### 5.2 逻辑计划

**SetClusteringColumns.scala**:
```scala
case class SetClusteringColumns(
    table: Seq[String],
    columns: Seq[String],
    options: ClusteringOptions) extends LeafCommand

case class ClusteringOptions(
    algorithm: Option[String] = None,
    targetSizeBytes: Option[Long] = None,
    minSizeBytes: Option[Long] = None)
```

**OptimizeTable.scala**:
```scala
case class OptimizeTable(
    table: Seq[String],
    isFull: Boolean) extends LeafCommand
```

### 5.3 物理执行计划

**SetClusteringColumnsExec.scala**:
```scala
case class SetClusteringColumnsExec(
    catalog: TableCatalog,
    ident: Identifier,
    columns: Seq[String],
    options: ClusteringOptions) extends LeafV2CommandExec {
    
    override protected def run(): Seq[InternalRow] = {
        // 更新 table properties
        val updates = table.updateProperties()
        if (columns.isEmpty) {
            updates.set(CLUSTERING_ENABLED, "false")
        } else {
            updates.set(CLUSTERING_ENABLED, "true")
            updates.set(CLUSTERING_COLUMNS, columns.mkString(","))
            // ... 设置其他选项
        }
        updates.commit()
        Nil
    }
}
```

**OptimizeTableExec.scala**:
```scala
case class OptimizeTableExec(
    catalog: TableCatalog,
    ident: Identifier,
    isFull: Boolean) extends LeafV2CommandExec {
    
    override protected def run(): Seq[InternalRow] = {
        val actions = SparkActions.get(session)
        val rewriteAction = actions.rewriteDataFiles(table)
        
        // 根据算法选择聚类策略
        algorithm match {
            case "hilbert" => rewriteAction.hilbert(clusteringColumns: _*)
            case "zorder"  => rewriteAction.zOrder(clusteringColumns: _*)
        }
        
        val result = rewriteAction.execute()
        // 返回统计信息
    }
}
```

### 5.4 SparkHilbertUDF

**文件**: `spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/actions/SparkHilbertUDF.java`

```java
class SparkHilbertUDF implements Serializable {
    
    // 将列值转换为有序字节数组
    Column sortedLexicographically(Column column, DataType type) {
        if (type instanceof IntegerType) {
            return intToOrderedBytesUDF().apply(column);
        } else if (type instanceof StringType) {
            return stringToOrderedBytesUDF().apply(column);
        }
        // ... 其他类型
    }
    
    // 交织位以生成 Hilbert 索引
    byte[] interleaveBits(Seq<byte[]> columnsBinary) {
        return ZOrderByteUtils.interleaveBits(columnsBinary, totalOutputBytes, outputBuffer.get());
    }
}
```

### 5.5 SparkHilbertFileRewriteRunner

**文件**: `spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/actions/SparkHilbertFileRewriteRunner.java`

```java
class SparkHilbertFileRewriteRunner extends SparkShufflingFileRewriteRunner {
    
    private static final String HILBERT_COLUMN = "ICEHILBERTVALUE";
    
    @Override
    protected Dataset<Row> sortedDF(Dataset<Row> df, Function<Dataset<Row>, Dataset<Row>> sortFunc) {
        // 1. 计算 Hilbert 值
        Dataset<Row> hilbertValueDF = df.withColumn(HILBERT_COLUMN, hilbertValue(df));
        // 2. 按 Hilbert 值排序
        Dataset<Row> sortedDF = sortFunc.apply(hilbertValueDF);
        // 3. 删除临时列
        return sortedDF.drop(HILBERT_COLUMN);
    }
    
    private Column hilbertValue(Dataset<Row> df) {
        SparkHilbertUDF hilbertUDF = new SparkHilbertUDF(
            hilbertColNames.size(), varLengthContribution, maxOutputSize);
        
        Column[] hilbertCols = hilbertColNames.stream()
            .map(col -> hilbertUDF.sortedLexicographically(df.col(col), colType))
            .toArray(Column[]::new);
        
        return hilbertUDF.interleaveBytes(array(hilbertCols));
    }
}
```

### 5.6 RewriteDataFilesSparkAction 扩展

**文件**: `spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteDataFilesSparkAction.java`

```java
public class RewriteDataFilesSparkAction {
    
    /**
     * 使用 Hilbert 曲线排序进行数据聚类
     */
    public RewriteDataFilesSparkAction hilbert(String... columnNames) {
        ensureRunnerNotSet();
        this.runner = new SparkHilbertFileRewriteRunner(spark(), table, Arrays.asList(columnNames));
        return this;
    }
}
```

---

## 6. 使用指南

### 6.1 Spark SQL 使用

#### 启用聚类

```sql
-- 设置聚类列（默认使用 Hilbert 算法）
ALTER TABLE catalog.db.events CLUSTER BY (user_id, event_time);

-- 指定算法和选项
ALTER TABLE catalog.db.events 
CLUSTER BY (user_id, event_time) 
WITH ALGORITHM = hilbert TARGET SIZE 512 MB;

-- 使用 Z-Order 算法
ALTER TABLE catalog.db.events 
CLUSTER BY (region, category) 
WITH ALGORITHM = zorder;
```

#### 执行优化

```sql
-- 增量优化（只处理 unclustered 文件）
OPTIMIZE catalog.db.events;

-- 完全重聚类（处理所有文件）
OPTIMIZE catalog.db.events FULL;
```

#### 禁用聚类

```sql
ALTER TABLE catalog.db.events CLUSTER BY NONE;
```

#### 查看聚类配置

```sql
-- 查看表属性
DESCRIBE EXTENDED catalog.db.events;

-- 或使用 SHOW TBLPROPERTIES
SHOW TBLPROPERTIES catalog.db.events;
```

### 6.2 Java API 使用

```java
import org.apache.iceberg.Table;
import org.apache.iceberg.ClusteringSpec;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

// 加载表
Catalog catalog = ...;
Table table = catalog.loadTable(TableIdentifier.of("db", "events"));

// 获取当前聚类规范
ClusteringSpec currentSpec = table.clusteringSpec();
System.out.println("Clustering enabled: " + currentSpec.isClustered());
System.out.println("Columns: " + currentSpec.columnNames());
System.out.println("Algorithm: " + currentSpec.algorithm());

// 启用聚类
table.updateClusteringSpec()
    .clusterBy("user_id", "event_time")
    .algorithm("hilbert")
    .targetZCubeSize(512 * 1024 * 1024)  // 512 MB
    .minZCubeSize(128 * 1024 * 1024)     // 128 MB
    .commit();

// 修改聚类列
table.updateClusteringSpec()
    .clusterBy("region", "category", "event_time")
    .commit();

// 禁用聚类
table.updateClusteringSpec()
    .clusterByNone()
    .commit();
```

### 6.3 Spark Actions API 使用

```java
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.actions.RewriteDataFiles;

SparkSession spark = ...;
Table table = ...;

// 使用 Hilbert 聚类优化
RewriteDataFiles.Result result = SparkActions.get(spark)
    .rewriteDataFiles(table)
    .hilbert("user_id", "event_time")
    .option("target-file-size-bytes", String.valueOf(128 * 1024 * 1024))
    .execute();

System.out.println("Rewritten files: " + result.rewrittenDataFilesCount());
System.out.println("Added files: " + result.addedDataFilesCount());

// 使用 Z-Order 聚类优化
SparkActions.get(spark)
    .rewriteDataFiles(table)
    .zOrder("user_id", "event_time")
    .execute();
```

---

## 7. 配置参数

### 7.1 Table Properties

| 属性 | 默认值 | 描述 |
|------|--------|------|
| `clustering.enabled` | `false` | 是否启用聚类 |
| `clustering.columns` | - | 聚类列，逗号分隔 |
| `clustering.algorithm` | `hilbert` | 聚类算法 (hilbert/zorder) |
| `clustering.target-zcube-size-bytes` | `536870912` (512MB) | 目标 ZCube 大小 |
| `clustering.min-zcube-size-bytes` | `134217728` (128MB) | 最小 ZCube 大小 |
| `clustering.max-columns` | `4` | 最大聚类列数 |
| `clustering.provider` | `liquid` | 聚类实现标识 |

### 7.2 Spark 配置

| 配置 | 默认值 | 描述 |
|------|--------|------|
| `spark.sql.iceberg.clustering.shuffle-partitions-per-file` | `1` | 每个输出文件的 shuffle 分区数 |
| `spark.sql.iceberg.clustering.var-length-contribution` | `8` | 变长类型贡献的字节数 |
| `spark.sql.iceberg.clustering.max-output-size` | `Integer.MAX_VALUE` | 交织字节的最大输出大小 |

### 7.3 配置示例

```sql
-- 创建表时设置聚类配置
CREATE TABLE catalog.db.events (
    event_id BIGINT,
    user_id BIGINT,
    event_time TIMESTAMP,
    event_type STRING,
    data STRING
) USING iceberg
TBLPROPERTIES (
    'clustering.enabled' = 'true',
    'clustering.columns' = 'user_id,event_time',
    'clustering.algorithm' = 'hilbert',
    'clustering.target-zcube-size-bytes' = '536870912'
);

-- 或者建表后配置
ALTER TABLE catalog.db.events SET TBLPROPERTIES (
    'clustering.enabled' = 'true',
    'clustering.columns' = 'user_id,event_time'
);
```

---

## 8. 最佳实践

### 8.1 聚类列选择

#### 推荐做法

1. **选择高过滤率的列**: 选择经常在 WHERE 子句中使用的列
2. **考虑列基数**: 中等基数的列效果最好
3. **列数量限制**: 建议 2-4 列，更多列会降低聚类效果
4. **避免高频更新列**: 频繁变化的列会导致大量重聚类

#### 列选择示例

```sql
-- 好的选择：查询经常按这些列过滤
ALTER TABLE events CLUSTER BY (user_id, event_date);

-- 不好的选择：过于细粒度的时间戳
ALTER TABLE events CLUSTER BY (event_timestamp_nanos);

-- 不好的选择：太多列
ALTER TABLE events CLUSTER BY (a, b, c, d);  -- 4列是上限
```

### 8.2 优化调度

#### 推荐策略

```sql
-- 1. 日常增量优化（处理新数据）
-- 每小时或每天运行
OPTIMIZE catalog.db.events;

-- 2. 周期性完全重聚类（整理碎片）
-- 每周或每月运行
OPTIMIZE catalog.db.events FULL;
```

#### 自动化调度示例

```python
# Airflow DAG 示例
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator

with DAG('iceberg_clustering_maintenance', schedule_interval='@hourly') as dag:
    
    # 增量优化
    incremental_optimize = SparkSqlOperator(
        task_id='incremental_optimize',
        sql='OPTIMIZE catalog.db.events',
        conn_id='spark_default'
    )
    
    # 每周完全重聚类
    full_recluster = SparkSqlOperator(
        task_id='full_recluster',
        sql='OPTIMIZE catalog.db.events FULL',
        conn_id='spark_default',
        trigger_rule='none_failed',
        # 只在周日运行
        execution_timeout=timedelta(hours=4)
    )
```

### 8.3 监控和调优

#### 监控指标

```sql
-- 查看文件统计
SELECT 
    COUNT(*) as total_files,
    SUM(file_size_in_bytes) as total_size,
    AVG(file_size_in_bytes) as avg_file_size,
    COUNT(DISTINCT zcube_id) as zcube_count
FROM catalog.db.events.files;

-- 查看 unclustered 文件比例
SELECT 
    COUNT(CASE WHEN clustering_provider IS NULL THEN 1 END) as unclustered,
    COUNT(CASE WHEN clustering_provider = 'liquid' THEN 1 END) as clustered
FROM catalog.db.events.files;
```

#### 调优建议

| 场景 | 建议 |
|------|------|
| 文件太小 | 增大 `target-zcube-size-bytes` |
| 优化太慢 | 减少聚类列数量 |
| 查询没有改善 | 检查聚类列是否与查询过滤条件匹配 |
| 频繁重聚类 | 检查是否有列被频繁更新 |

---

## 9. 与现有功能对比

### 9.1 Liquid Clustering vs 分区

| 方面 | Liquid Clustering | 传统分区 |
|------|-------------------|----------|
| **灵活性** | 高 - 随时可改 | 低 - 建表时确定 |
| **列数量** | 最多4列 | 通常1-2列 |
| **数据分布** | 均匀 (ZCube) | 依赖数据分布 |
| **小文件问题** | 自动处理 | 可能严重 |
| **维护成本** | 增量 OPTIMIZE | 可能需要重写 |
| **查询优化** | 统计裁剪 | 分区裁剪 |

### 9.2 Liquid Clustering vs Sort Order

| 方面 | Liquid Clustering | Sort Order |
|------|-------------------|------------|
| **多列支持** | 空间填充曲线 | 线性排序 |
| **查询模式** | 多维过滤 | 范围扫描 |
| **适用场景** | 点查询、多列过滤 | 范围查询 |
| **组合使用** | 可以与 Sort Order 共存 | - |

### 9.3 何时使用 Liquid Clustering

**推荐使用场景**:
- 查询经常按多个列过滤
- 不想预先确定分区策略
- 需要灵活调整数据组织
- 数据量大，需要增量优化

**不推荐场景**:
- 已有良好的分区策略且运行良好
- 数据量小，全表扫描可接受
- 查询模式非常固定

---

## 附录

### A. 文件清单

| 模块 | 文件 | 描述 |
|------|------|------|
| API | `ClusteringSpec.java` | 聚类规范接口 |
| API | `ClusteringColumn.java` | 聚类列类 |
| API | `UpdateClusteringSpec.java` | 更新聚类规范接口 |
| API | `Table.java` | Table 接口扩展 |
| API | `DataFile.java` | DataFile 接口扩展 |
| Core | `ClusteringSpecParser.java` | JSON 序列化 |
| Core | `BaseUpdateClusteringSpec.java` | UpdateClusteringSpec 实现 |
| Core | `TableMetadataParser.java` | 元数据解析器扩展 |
| Core | `TableProperties.java` | 属性常量 |
| Core | `BaseTable.java` | BaseTable 实现 |
| Spark | `IcebergSqlExtensions.g4` | ANTLR 语法 |
| Spark | `SetClusteringColumns.scala` | 逻辑计划 |
| Spark | `OptimizeTable.scala` | 逻辑计划 |
| Spark | `SetClusteringColumnsExec.scala` | 执行计划 |
| Spark | `OptimizeTableExec.scala` | 执行计划 |
| Spark | `SparkHilbertUDF.java` | Hilbert UDF |
| Spark | `SparkHilbertFileRewriteRunner.java` | 文件重写器 |
| Spark | `RewriteDataFilesSparkAction.java` | Action 扩展 |
| Spark | `ExtendedDataSourceV2Strategy.scala` | 策略扩展 |
| Spark | `IcebergSqlExtensionsAstBuilder.scala` | AST Builder |

### B. 版本支持

| Iceberg 版本 | Spark 版本 | 状态 |
|-------------|-----------|------|
| 1.x+ | Spark 4.0 | ✅ 完整支持 |
| 1.x+ | Spark 3.5 | 🔄 计划支持 |
| 1.x+ | Spark 3.4 | 🔄 计划支持 |

### C. 参考资料

- [Delta Lake Liquid Clustering](https://docs.databricks.com/delta/clustering.html)
- [Hilbert Curve Wikipedia](https://en.wikipedia.org/wiki/Hilbert_curve)
- [Z-Order Curve Wikipedia](https://en.wikipedia.org/wiki/Z-order_curve)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
