# CarrotQuant.Data 项目架构规范 (AI 专用)

## 1. 系统逻辑架构 (Control & Data Flow)

项目采用“元数据驱动”的流水线架构，确保下载逻辑精准、存储结构标准。

### 1.1 Gateway (接入层/外壳)
*   **职责**: 系统入口。
*   **控制流**: 接收 `main.py` (CLI) 或 `FastAPI` (REST) 的原始参数，封装为任务指令发送至 Service 层。

### 1.2 Service (业务服务层/大脑)
*   **SyncManager (总体调度)**: 协调内部各组件，串联规划、采集、落地与物理巡检。
*   **TaskPlanner (任务规划)**: 核心组件。
    1.  对比“用户请求”与“本地库存”（Metadata），计算下载补丁。
    2.  **增量逻辑**: 若本地 `last_sync_ts` 存在，则将该 symbol 的起始点设为 `last_sync_ts`。
*   **MetadataManager (元数据管理)**: 负责 `metadata.json` 的原子化保存与加载，为 Planner 提供决策依据。

### 1.3 Provider (驱动提供层/手脚)
*   **Source Drivers**: 插件化架构。`BaseProvider` 抽象类定义标准下载动作（`fetch`）。
    - **原子化下载**: 驱动接口强制单次仅处理**单支 Symbol**，确保任务编排层（Planner）可自由拆分与重试。
    - **路由分发**: 驱动内部解析 `table_id` 的业务段落（如 `kline`），自动分发至对应的私有抓取方法。
*   **Provider Manager**: 策略工厂模式。解析 `table_id` 的末尾字段（源标识），动态实例化并缓存对应的驱动。
*   **DataCleaner (数据清洗)**: 在驱动内部通过 `DataCleaner` 对原始数据进行“实时清洗”，强制补齐 `timestamp` (Int64) 及 ISO8601 `datetime` (String) 字段，并删除源特有字段（如 `code`）。

### 1.4 Storage (持久化存储层/资产库)
*   **StorageManager**: 负责数据落地。
*   **格式派生**: `CSVStorage`, `ParquetStorage` 实现不同的文件 IO。
*   **接口统一**: 通过 `write(table_id, df, mode="append")` 进行 Upsert 落地。在 `append` 模式下，内部自动执行“读取旧分片 -> 合并 -> unique(timestamp) -> 写入”。
*   **物理巡检**: 同步结束后由 `SyncManager` 触发物理扫描，更新 `metadata.json` 以反映磁盘真实状态。

## 2. Storage 层规范

### 命名规范 (Table ID)
所有存储表的 ID 遵循“由大到小”的层级命名逻辑，点号分段。
推荐格式：`{market}.{category}.{freq}.{adj}.{source}`
- `market`: 市场或项目标识
    - `ashare`: 专门指向 A 股个股
    - `aindex`: 专门指向 A 股指数
- `category`: 数据类别（如 `lhb`, `kline`）
- `freq`: 频率（如 `1d`, `5m`, `1m`）
- `adj`: 复权方式（如 `adj`, `qfq`, `hfq`, `raw`）
- `source`: 数据来源（如 `baostock`, `akshare`, `eastmoney`）

示例：`ashare.kline.1d.adj.baostock`, `ashare.lhb.eastmoney`
路径：`ashare.kline.1d.baostock` -> `storage_root/csv/ashare.kline.1d.baostock/`

### 存储路径模板
数据按格式、表名、年份分片存储，遵循 Hive 分区样式：
`storage_root/{format}/{table_id}/year={year}/{symbol}.{format}`

- `format`: 存储格式（`csv` 或 `parquet`）
- `table_id`: 见上述命名规范
- `year`: 数据年份（如 `year=2024`）
- `symbol`: 证券代码文件名（如 `sh.600000.csv`）

### 时间轴标准 (双时间轴协议)
存储层**强制要求**入库数据符合以下标准，这是物理层合并与分区的唯一凭据：
1.  **timestamp (核心列)**: 毫秒级时间戳 (Int64)。
    - **用途一**: 物理合并的唯一主键（去重）。
    - **用途二**: 分区定位的凭据（通过其计算 `year` 分区）。
2.  **datetime (可读列)**: 标准 Datetime 类型（在 CSV 中表现为 ISO8601 字符串）。
    - **用途**: 仅供人工查阅及其他跨语言工具读取，存储层逻辑不再依赖此列名。

### 核心逻辑要求
1.  **分区规则**: `year` 分区文件夹名称必须通过 `timestamp` 计算得出，严禁依赖字符串截取。
2.  **增量去重**: `append` 操作必须强制校验并使用 `timestamp` 数字列进行 `unique(subset=["timestamp"], keep="last")`。
3.  **职责明确**: 存储层不负责清洗任何 `date` 或 `time` 字段，若缺失 `timestamp` 列应拒绝入库。
4.  **IO 性能**: 必须使用 `polars` (pl)，确保 `timestamp` 列类型一致 (Int64)。

### 2.4 Metadata 元数据规范
元数据由 `SyncManager` 在同步完成后触发“Storage 元数据更新”自动生成，是该数据集本地物理状态的**真实映射**。
- **路径**: `storage_root/{format}/{table_id}/metadata.json`
- **属性释义**:
    - `total_bars`: 该目录下所有 CSV/Parquet 文件的行数物理总和。
    - `time_steps`: 全局去重后的 `timestamp` 时间点个数。
    - `symbol_count`: 物理存在的证券代码个数。

#### 元数据示例 (metadata.json)
```json
{
  "table_id": "ashare.kline.1d.adj.baostock",
  "category": "TimeSeries",
  "format": "csv",
  "global_stats": {
    "start_timestamp": 1704067200000,
    "end_timestamp": 1716163200000,
    "start_datetime": "2024-01-01T00:00:00.000",
    "end_datetime": "2024-05-20T00:00:00.000",
    "time_steps": 100, 
    "symbol_count": 5000,
    "total_bars": 485600 
  }
}
```

## 3. 命名与目录规范
- 模块路径统一采用 `app/...`。
- **Service 层**: 包含 `SyncManager`, `TaskPlanner`, `MetadataManager`。
- **Provider 层**: 包含 `ProviderManager`, `DataCleaner` 及各驱动实现。
- **Storage 层**: 包含 `CSVStorage`, `ParquetStorage` 等持久化实现。
- 抽象基类分别定义在各层的 `base.py` 中。
- 具体实现类命名为 `{Format}Storage`（如 `CSVStorage`）。

## 4. 强制约束
- **数据处理**: 禁止使用 `pandas`，统一使用 `polars` (pl)。
- **复权规则**: 量化交易系统**禁止使用前复权**。任何数据抓取接口仅支持 `raw` (不复权) 或 `adj` (后复权)。
- **字段要求**: 
    - `symbol` 为强制保留字段，各数据源原始字段（如 `code`）必须统一重命名为 `symbol`。
    - `timestamp` (Int64) 和 `datetime` (String) 为标准化输出的必选时间轴字段。
- **运行环境**: 所有执行指令必须带有 `uv run` 前缀。
- **语言**: 代码注释及开发文档必须使用中文。
