# CarrotQuant.Data 项目架构规范 (AI 专用)

## 1. 架构定义 (四层架构)

项目采用清晰的四层架构，各层之间职责解耦：

1.  **Gateways (网关层)**: 负责与外部数据源（如 AkShare, Baostock, Eastmoney）进行通信，将原始数据转换为系统内部模型。
2.  **Storage (存储层)**: 负责数据的持久化与读取。支持多种存储格式（CSV, Parquet），具备分区（Hive-style）及去重逻辑。
3.  **Models (模型层)**: 定义全局通用的数据结构、枚举及 Pydantic 模式。
4.  **Services (服务层)**: 业务逻辑层，组合 Gateways 和 Storage 完成复杂的数据采集、清洗及入库流程。

## 2. Storage 层规范

### 命名规范 (Table ID)
所有存储表的 ID 遵循“由大到小”的层级命名逻辑，点号分段。
推荐格式：`{market}.{category}.{freq}.{adj}.{source}`
- `market`: 市场标识（如 `ashare`, `hkstock`）
- `category`: 数据类别（如 `lhb`, `kline`）
- `freq`: 频率（如 `1d`, `5m`, `1m`）
- `adj`: 复权方式（如 `adj`, `qfq`, `hfq`, `none`）
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

## 3. 命名与目录规范
- 模块路径统一采用 `backend/app/...` 或 `src/carrotquant_data/...`（当前演进中）。
- 抽象基类定义在 `storage/base.py`。
- 具体实现类命名为 `{Format}Storage`（如 `CSVStorage`）。

## 4. 强制约束
- **数据处理**: 禁止使用 `pandas`，统一使用 `polars`。
- **运行环境**: 所有执行指令必须带有 `uv run` 前缀。
- **语言**: 代码注释及开发文档必须使用中文。
