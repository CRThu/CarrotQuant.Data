# CarrotQuant.Data 项目架构规范 (AI 专用)

## 1. 架构定义 (四层架构)

项目采用清晰的四层架构，各层之间职责解耦：

1.  **Gateways (网关层)**: 负责与外部数据源（如 AkShare, Baostock, Eastmoney）进行通信，将原始数据转换为系统内部模型。
2.  **Storage (存储层)**: 负责数据的持久化与读取。支持多种存储格式（CSV, Parquet），具备分区（Hive-style）及去重逻辑。
3.  **Models (模型层)**: 定义全局通用的数据结构、枚举及 Pydantic 模式。
4.  **Services (服务层)**: 业务逻辑层，组合 Gateways 和 Storage 完成复杂的数据采集、清洗及入库流程。

## 2. Storage 层规范

### 命名规范 (Table ID)
所有存储表的 ID 必须遵循以下格式：
`{market}.{freq}.{adj}.{source}`
- `market`: 市场标识（如 `ashare`, `hkstock`）
- `freq`: 频率（如 `1d`, `5m`, `1m`）
- `adj`: 复权方式（如 `adj`, `qfq`, `hfq`, `none`）
- `source`: 数据来源（如 `baostock`, `akshare`）

示例：`ashare.1d.adj.baostock`

### 存储路径模板
数据按格式、表名、年份分片存储，遵循 Hive 分区样式：
`storage_root/{format}/{table_id}/year={year}/{symbol}.{format}`

- `format`: 存储格式（`csv` 或 `parquet`）
- `table_id`: 见上述命名规范
- `year`: 数据年份（如 `year=2024`）
- `symbol`: 证券代码文件名（如 `sh.600000.csv`）

### 核心逻辑要求
1.  **分片存储**: 写入时必须按 `symbol` 和 `year` 分片，避免单个文件过大。
2.  **增量去重**: `append` 操作必须包含去重逻辑，默认以 `date` 字段作为唯一标识。
3.  **IO 性能**: 必须使用 `polars` (pl) 进行数据处理，强制使用 `read_csv`/`write_csv` 或 `read_parquet`/`write_parquet` 以保证性能。

## 3. 命名与目录规范
- 模块路径统一采用 `backend/app/...` 或 `src/carrotquant_data/...`（当前演进中）。
- 抽象基类定义在 `storage/base.py`。
- 具体实现类命名为 `{Format}Storage`（如 `CSVStorage`）。

## 4. 强制约束
- **数据处理**: 禁止使用 `pandas`，统一使用 `polars`。
- **运行环境**: 所有执行指令必须带有 `uv run` 前缀。
- **语言**: 代码注释及开发文档必须使用中文。
