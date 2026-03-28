# CarrotQuant.Data 项目架构规范 (AI 专用)

## 1. 系统逻辑架构 (Control & Data Flow)

项目采用“元数据驱动”的流水线架构，确保下载逻辑精准、存储结构标准。

### 1.1 Gateway (接入层/外壳)
*   **职责**: 系统入口。
*   **控制流**: 接收 `main.py` (CLI) 或 `FastAPI` (REST) 的原始参数，封装为任务指令发送至 Service 层。

### 1.2 Service (业务服务层/大脑)
*   **SyncManager (总体调度)**: 协调内部各组件，串联规划、采集、落地与物理巡检。
    - **同步重构**: 引入“批处理聚合模式 (Batch Sync)”。按 `batch_size` 将 symbols 切分，将一个批次内所有股票的 DataFrame 在内存中通过 `pl.concat` 聚合为一张“大表”后，单次下移至 Storage 层落盘。此举旨在彻底解决 Parquet 等格式下的“写放大”性能灾难。
*   **TaskPlanner (任务规划)**: 核心组件。
    1.  对比“用户请求”与“本地库存”（Metadata），计算下载补丁。
    2.  **补丁逻辑**: 支持“向前补全”、“向后拓展”以及“全量覆盖”。系统计算缺失区间与本地数据的并集，并仅返回一个单段下载任务。支持 `force_refresh` 强制重载。
*   **MetadataManager (元数据管理)**: 负责 `metadata.json` 的原子化保存与加载，为 Planner 提供决策依据。

### 1.3 Provider (驱动提供层/手脚)
*   **Source Drivers**: 插件化架构。`BaseProvider` 抽象类定义标准下载动作（`fetch`）与能力发现。
    - **能力发现 (get_supported_tables)**: 驱动强制实现该接口以返回其支持的所有 `table_id` 列表。
    - **原子化下载**: 驱动接口强制单次仅处理**单支 Symbol**，确保任务编排层（Planner）可自由拆分与重试。
    - **路由分发**: 驱动内部通过 `get_supported_tables` 预校验并分发至私有抓取方法。
*   **Provider Manager**: 策略工厂模式。解析 `table_id` 的末尾字段（源标识），动态实例化并缓存对应的驱动。
*   **DataCleaner (数据清洗)**: 在驱动内部通过 `DataCleaner` 对原始数据进行“标准化清洗”，强制补齐 `timestamp` (Int64) 及 ISO8601 `datetime` (String) 字段。

### 1.4 Storage (持久化存储层/资产库)
*   **StorageManager**: 负责数据落地。
*   **格式派生**: `CSVStorage` (单代码单文件), `ParquetStorage` (月度聚合大表)。
*   **接口统一**: 提供两个独立的写入接口：
    - `write_series(table_id, df, mode="append")`: 处理时间序列数据 (TS)，按 symbol 和 year 分区
    - `write_event(table_id, df, mode="append")`: 处理事件数据 (EV)，按 year 单文件布局，执行全行去重
*   **去重与排序**: 
    - TS 数据：基于 `[symbol, timestamp]` 复合主键去重 (`keep="last"`)
    - EV 数据：执行全行去重
    - 所有数据按动态排序逻辑处理：若包含 `symbol` 列则按 `["timestamp", "symbol"]` 排序，否则仅按 `["timestamp"]` 排序
*   **原子写入**: 所有存储写操作必须遵循"先写 `.tmp` 临时文件，成功后通过 `os.replace` 替换原文件"的原子化原则，确保物理安全。
*   **物理巡检**: 同步结束后由 `SyncManager` 针对每个格式触发独立的物理扫描，更新对应的 `metadata.json`。
*   **动态列校验**: EV 数据支持动态列校验，若 `symbol` 列缺失则自动回退到仅基于 `timestamp` 的排序与统计逻辑，确保宏观数据（如利率、指数成分变动）能正常落地。

## 2. Gateway 层 (并发防御与多模运行)

### 2.1 CLI 模式 (Typer)
*   **入口**: `app/gateway/cli.py`
*   **功能**: 支持多表、多格式批量启动同步。参数支持 `--tables`, `--formats`, `--start`, `--force` 等。

### 2.2 API 模式 (FastAPI)
*   **入口**: `app/gateway/api.py`
*   **并发防御 (Task Locking)**:
    - **ACTIVE_SYNC_TASKS**: 内存中的 `set` 集合，记录当前正在同步的 `table_id`。
    - **冲突拦截**: 当新请求到达时，若 `table_id` 已在集合中，立即返回 `409 Conflict`，防止对同一物理目录的并发写入冲突。
    - **异步执行**: 使用 `BackgroundTasks` 异步下行，任务结束后在 `finally` 块中释放锁。
*   **路由**:
    - `POST /api/v1/sync`: 触发同步任务。
    - `GET /api/v1/tasks`: 查询活跃任务。
    - `GET /api/v1/data/{table_id}`: 数据查询标准化网关。

## 3. Storage 层规范

### 命名规范 (Table ID)
所有存储表的 ID 遵循"由大到小"的层级命名逻辑，点号分段。
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
数据按格式、表名、年份分片存储，遵循 Hive 分区样式。根据数据类别（TS/EV）采用不同的物理布局：

#### TimeSeries (TS) 数据布局
1. **CSV (按代码分片)**:
   `storage_root/csv/{table_id}/year={year}/{symbol}.csv`
2. **Parquet (月度大表聚合)**:
   `storage_root/parquet/{table_id}/year={year}/{year}-{month}.parquet`

#### Event (EV) 数据布局
1. **CSV (年份单文件)**:
   `storage_root/csv/{table_id}/year={year}/data.csv`
2. **Parquet (年份单文件)**:
   `storage_root/parquet/{table_id}/year={year}/data.parquet`

**路径参数说明**:
- `format`: 存储格式（`csv` 或 `parquet`）
- `table_id`: 见上述命名规范
- `year`: 数据年份（如 `year=2024`）
- `month`: 数据月份（如 `01` 到 `12`）
- `symbol`: 证券代码文件名（如 `sh.600000.csv`）

### 写入接口规范
存储层提供两个独立的写入接口，根据数据类别调用：

1. **write_series(table_id, df, mode="append")**: 处理时间序列数据
   - 分区策略：按 `symbol` 和 `year` 分区
   - 去重策略：基于 `[symbol, timestamp]` 复合主键去重
   - 排序策略：按 `["timestamp", "symbol"]` 升序排序
   - 强制要求：必须包含 `symbol` 和 `timestamp` 列

2. **write_event(table_id, df, mode="append")**: 处理事件数据
   - 分区策略：按 `year` 单文件布局
   - 去重策略：全行去重（非 symbol+timestamp）
   - 排序策略：动态排序逻辑
     - 若包含 `symbol` 列：按 `["timestamp", "symbol"]` 排序
     - 若不含 `symbol` 列：按 `["timestamp"]` 排序
   - 列探测：自动检查 `df` 是否包含 `symbol` 列，支持无 `symbol` 列的宏观数据

### 时间轴标准 (双时间轴协议)
存储层**强制要求**入库数据符合以下标准，这是物理层合并与分区的唯一凭据：
1.  **timestamp (核心列)**: 毫秒级时间戳 (Int64)。
    - **用途一**: 物理合并的复合主键之一。
    - **用途二**: 分区定位的凭据（通过其计算 `year` 和 `month` 分区）。
2.  **symbol (核心列)**: 证券代码 (String)。
    - **用途**: 物理合并的复合主键之一。
3.  **datetime (可读列)**: 标准 Datetime 类型（在 CSV 中表现为 ISO8601 字符串）。

### 核心逻辑要求
1.  **物理主键**: 全系统去重与排序标准统一为 `["symbol", "timestamp"]`。
2.  **分子区规则**: 分区路径必须通过 `timestamp` 计算得出。
3.  **增量去重**: `append` 操作必须强制校验并使用 `unique(subset=["symbol", "timestamp"], keep="last")`。
4.  **原子落盘**: 严禁直接 `write`，必须通过 `.tmp` 中转 + `os.replace`。
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

**TimeSeries 类型 (category: "TS")**:
```json
{
  "table_id": "ashare.kline.1d.adj.baostock",
  "category": "TS",
  "format": "csv",
  "schema": {
    "symbol": "Utf8",
    "datetime": "Utf8",
    "timestamp": "Int64",
    "open": "Float64",
    "high": "Float64",
    "low": "Float64",
    "close": "Float64",
    "volume": "Float64",
    "amount": "Float64"
  },
  "statistics": {
    "start_timestamp": 1704067200000,
    "end_timestamp": 1716163200000,
    "start_datetime": "2024-01-01T00:00:00.000+08:00",
    "end_datetime": "2024-05-20T00:00:00.000+08:00",
    "total_bars": 485600,
    "symbol_count": 5000,
    "time_steps": 100
  }
}
```

**Event 类型 (category: "EV")**:
```json
{
  "table_id": "ashare.adj_factor.baostock",
  "category": "EV",
  "format": "csv",
  "schema": {
    "symbol": "Utf8",
    "datetime": "Utf8",
    "timestamp": "Int64",
    "back_adj_factor": "Float64"
  },
  "statistics": {
    "start_timestamp": 1104537600000,
    "end_timestamp": 1716163200000,
    "start_datetime": "2005-01-01T00:00:00.000+08:00",
    "end_datetime": "2024-05-20T00:00:00.000+08:00",
    "total_bars": 15000
  }
}
```

**注意**: 复权因子数据仅保留后复权因子 (`back_adj_factor`)，前复权因子 (`foreAdjustFactor`) 和其他复权因子 (`adjustFactor`) 已被物理剔除，以防止回溯特性影响增量同步水位线的安全性。

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
- **时区处理**: 
    - **双时区驱动机制**: 系统引入 `source_tz` 和 `display_tz` 两个时区参数。
    - `source_tz`: 用于将原始挂钟时间对齐到 UTC 0。必须使用 IANA 时区名称（如 "Asia/Shanghai", "UTC", "America/New_York"）。
    - `display_tz`: 用于生成 datetime 显示列（带偏移量的 ISO8601）。必须使用 IANA 时区名称。
    - **默认值**: 针对 A 股环境，两者默认均为 "Asia/Shanghai"。
    - **实现规范**: 
        1. 必须使用 Python 3.9+ 的 `zoneinfo` 库处理时区，严禁手动计算时区偏移。
        2. `datetime` 字符串必须以 `+HH:MM` 或 `-HH:MM` 结尾（如 `2024-01-01T08:00:00.000+08:00`）。
        3. 严禁使用不带偏移量的本地时间。
    - **验证要求**: 
        - 跨时区验证：如果 `source_tz="UTC"` 且 `display_tz="Asia/Shanghai"`，北京时间 8 点的数据生成的 timestamp 应为 0（UTC 0点）。
        - 夏令时验证：使用 `America/New_York` 测试 6 月份数据，验证 datetime 后缀应自动为 `-04:00`。
- **无损降级**: 当 EV 数据没有 `symbol` 列时，系统必须能够静默处理，严禁抛出 `ColumnNotFoundError`。
- **物理布局**: 
    - TS: `storage_root/{format}/{table_id}/year=2024/{symbol}.{ext}`
    - EV: `storage_root/{format}/{table_id}/year=2024/data.{ext}`
- **去重一致性**: EV 模式强制全行去重，这在没有 `symbol` 的宏观数据（如：每天只有一行全市场统计）下依然有效。
- **元数据巡检适配**: 
    - `get_all_symbols`: 若为 EV 目录且文件内无 `symbol` 列，应返回空列表。
    - `get_total_bars`: 维持通配符扫描汇总逻辑。
- **运行环境**: 所有执行指令必须带有 `uv run` 前缀。
- **语言**: 代码注释及开发文档必须使用中文。

## 5. 空数据防御逻辑 (Empty Data Defense Logic)

为降低无效 IO 并确保元数据与磁盘物理状态的绝对一致，系统实施了以下三个层次的防御：

### 5.1 驱动骨架补齐 (Provider Layer)
*   **行为**: 当 `fetch` 未能抓取到任何数据或发生错误时，禁止返回裸的空 `pl.DataFrame()`。
*   **实现**: 驱动必须构造一个带有完整列定义的 DataFrame（基于 `fields`），并强制通过 `DataCleaner` 进行标准化处理。
*   **价值**: 确保下游 `SyncManager` 即使在无数据时也能捕捉到正确的 Schema，并保证 `timestamp` 等核心列始终存在，避免清洗逻辑报错。

### 5.2 存储写入拦截 (Storage Layer)
*   **行为**: `write` 方法在执行任何磁盘操作前（如 `mkdir`, `.tmp` 文件创建）必须先行判定 `df.is_empty()`。
*   **实现**: 若为空则立即 `return`。
*   **价值**: 彻底杜绝空表在磁盘上产生任何“文件夹残留”。

### 5.3 元数据静默机制 (Service Layer)
*   **核心原则**: "物理真实性优先"。`metadata.json` 必须是磁盘有效数据的映射，禁止为"空文件夹"盖章。
*   **具体场景**:
    1.  **初次同步无数据**: 若磁盘物理巡检 `total_bars == 0` 且本地无元数据，则不创建任何文件。
    2.  **增量同步无新增**: 若本次同步未产生实际数据下载（`data_written == False`）且本地已有元数据，且非 `force_refresh` 模式，则跳过 `metadata.json` 的更新（保持 mtime 不变）。
    3.  **价值**: 保证元数据的时间轴描述与物理磁盘文件同步，避免 Planner 基于虚空的元数据做出错误规划。

## 6. 单元测试文件结构

```
tests/
├── unit/                          # 单元测试
│   ├── test_utils_time.py        # 时区工具
│   ├── test_storage_csv.py       # CSV 存储
│   ├── test_storage_parquet.py   # Parquet 存储
│   ├── test_storage_factory.py   # 存储工厂
│   ├── test_storage_merger.py    # 数据合并
│   ├── test_service_planner.py   # 任务规划
│   ├── test_service_batch_sync.py # 批量同步
│   └── test_sync_and_metadata.py # 元数据同步
├── integration/                   # 集成测试
│   ├── test_provider_baostock.py # Baostock 驱动
│   ├── test_sync_full_flow.py    # 全链路同步
│   ├── test_sync_defense.py      # 空数据防御
│   └── test_sync_multi_storage.py # 多存储格式
└── gateway/                       # 网关测试
    ├── test_api_locking.py       # API 并发锁
    └── test_api_query.py         # API 查询
```

**测试运行命令**:
```bash
uv run pytest tests/ -v
uv run pytest tests/unit/test_utils_time.py -v
uv run pytest tests/unit/test_utils_time.py::test_ts_to_iso_summer_time -v
```

