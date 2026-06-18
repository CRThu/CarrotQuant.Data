# CarrotQuant.Data 存储架构规范 (AI 专用)

本文档旨在为 AI Agent 提供关于存储布局、数据格式、元数据定义的权威指南。请在进行存储相关开发或维护时严格遵守。

## 1. 存储核心设计原则
*   **格式**: 统一使用 `CSV` (按需存储) 和 `Parquet` (性能存储)。
*   **引擎**: 全系统强制使用 `polars` (pl)，严禁使用 `pandas`。
*   **类型安全**: 禁止 Polars 自动类型推断。所有读取操作必须通过 `metadata.json` 的 `schema` 进行显式 `cast`。
*   **原子落盘**: 遵循“先写 `.tmp`，后 `os.replace`”原则。
*   **数据流**: 遵循“双时间轴协议” (`timestamp` Int64, `symbol` String, `datetime` String)。

## 2. 数据分类与布局 (Hive 分区)

### 2.1 TimeSeries (TS) - 时间序列
*   **定义**: 具备明显 `symbol` 维度和 `timestamp` 维度的数据（如 K 线、行情）。
*   **CSV 布局**: `storage_root/csv/{table_id}/year={yyyy}/{symbol}.csv`
*   **Parquet 布局**: `storage_root/parquet/{table_id}/year={yyyy}/data.parquet`
*   **去重策略**: `subset=["symbol", "timestamp"]`，`keep="last"`。
*   **排序策略**: 
    *   CSV: `["timestamp"]`
    *   Parquet: `["symbol", "timestamp"]` (Symbol-First)

### 2.2 Event (EV) - 事件数据
*   **定义**: 宏观数据、调整因子或缺乏 `symbol` 维度的全市场数据。
*   **CSV 布局**: `storage_root/csv/{table_id}/year={yyyy}/data.csv`
*   **Parquet 布局**: `storage_root/parquet/{table_id}/year={yyyy}/data.parquet`
*   **去重策略**: 全行去重。
*   **排序策略**: `["timestamp", "symbol"]` (Time-First)。

## 3. 存储路径规则 (Path Mapping)

若需手动构造路径或进行物理扫描，请遵循以下布局：

### Table ID 命名规范
存储表的 ID 遵循“由大到小”的层级命名逻辑，通过点号 (`.`) 分段，以实现组件的动态解析与路由。
**规范格式**: `{market}.{category}.[sub_category/freq/adj].{source}`

* **market (第一段)**: 市场或项目标识（如 `ashare`, `aindex`）
* **category**: 数据类别（如 `lhb`, `kline`, `adj_factor`）
* **中间段落**: 由具体驱动解析（如频率 `1d`, `5m`；复权 `adj`, `raw`）
* **source (最后一段)**: 数据来源（如 `baostock`, `akshare`, `eastmoney`）

### 物理路径布局
* **TimeSeries (TS):**
    * CSV: `{storage_root}/csv/{table_id}/year={yyyy}/{symbol}.csv`
    * Parquet: `{storage_root}/parquet/{table_id}/year={yyyy}/data.parquet`

* **Event (EV):**
    * CSV: `{storage_root}/csv/{table_id}/year={yyyy}/data.csv`
    * Parquet: `{storage_root}/parquet/{table_id}/year={yyyy}/data.parquet`

## 4. 元数据规范 (metadata.json)

每个数据集都必须在其存储根目录下拥有一个 `metadata.json`，它是该数据集的“法律证明”。

### 4.1 结构示例
```json
{
  "table_id": "ashare.kline.1d.adj.baostock",
  "category": "timeseries",
  "format": "csv",
  "partition": "symbol",
  "layout": "hive",
  "schema": {
    "symbol": "String",
    "datetime": "String",
    "timestamp": "Int64",
    "open": "Float64",
    "high": "Float64",
    "low": "Float64",
    "close": "Float64",
    "volume": "Float64",
    "amount": "Float64"
  }
}
```

### 4.2 核心字段说明
*   `table_id`: 数据表唯一标识 (`{market}.{category}.[sub_category/freq/adj].{source}`)。
*   `category`: `timeseries` 或 `event`。
*   `format`: `csv` 或 `parquet`。
*   `schema`: 类型映射表，Value 必须为 `String`, `Int64`, `Float64`, `Boolean`, `Date`, `Datetime`。
*   `statistics` (可选): 物理巡检产出的状态描述，仅用于分析用途，系统不再依赖此字段进行路径调度。

## 5. 类型转换字典
在读取数据时，Agent 应使用以下映射逻辑：

```python
type_map = {
    "Int64": pl.Int64,
    "Float64": pl.Float64,
    "String": pl.String,
    "Boolean": pl.Boolean,
    "Date": pl.Date,
    "Datetime": pl.Datetime
}
```

## 6. 开发禁忌
1.  **禁止使用前复权**: 复权仅限 `raw` 或 `adj` (后复权)。
2.  **禁止跳过元数据**: 若发现 `metadata.json` 缺失，必须抛出 `RuntimeError`，严禁通过猜测推断 Schema。
3.  **禁止自动推断**: 读取操作必须显式传参 `schema` 或在 `read_parquet` 后执行 `cast`。
4.  **禁止直接 Append**: `write` 操作需进行去重处理 (`DataMerger`)。
