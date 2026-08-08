# CarrotQuant.Data (cqdata) Python SDK 指南

本文档提供 `cqdata` Python SDK 的全量 API 说明与常用代码示例。

---

## 1. SDK 设计原则与快速入门

`cqdata` SDK 为量化研究员与 Python 开发者提供极简、类型安全、高性能的数据接入能力。

### 核心特性
1. **统一读取 (`cqdata.read`)**：一个函数读取 K 线时序与板块事件数据，自动智能路由处理分支。
2. **统一探查 (`cqdata.list_tables`)**：平铺返回所有数据表清单及 `category` 分类标记。
3. **原生 Polars 高级性能**：基础返回类型均为 `polars.DataFrame`，原生支持内存投影、快速过滤与链式表达式处理。

---

## 2. API 规范与函数清单

```python
import cqdata
```

| 函数 | 类型签名 | 说明 |
| :--- | :--- | :--- |
| **`read(...)`** | `(table_id, symbols=None, start_date=None, end_date=None, columns=None, format="auto", storage_root=None) -> pl.DataFrame` | 统一切片读取金融数据（自动按 `table_id` 智能路由） |
| **`list_tables(...)`** | `(format="auto", storage_root=None) -> List[Dict[str, str]]` | 列出本地所有数据表及其 `category` 分类（`timeseries` 或 `event`） |
| **`list_formats(...)`** | `(table_id, storage_root=None) -> List[str]` | 查询某数据表在本地已拥有的物理存储格式 |
| **`list_symbols(...)`** | `(table_id, format="auto", storage_root=None) -> List[str]` | 查询某数据表在本地已下载的股票/指数代码列表 |
| **`get_time_range(...)`** | `(table_id, format="auto", storage_root=None) -> Tuple[str, str]` | 获取某数据表的全局时间跨度 `(start_datetime, end_datetime)` |
| **`get_schema(...)`** | `(table_id, format="auto", storage_root=None) -> Dict[str, str]` | 获取某数据表的字段定义字典 `(列名 -> 类型名)` |
| **`get_row_count(...)`** | `(table_id, format="auto", storage_root=None) -> int` | 获取某数据表在物理存储中的记录条数 |
| **`sync(...)`** | `(table_ids, formats="parquet", start_date=None, end_date=None, force_refresh=False, batch_size=100, symbol_limit=None)` | 触发底层全自动同步引擎 |
| **`configure(...)`** | `(storage_root=None, config_file=None)` | 全局配置 `storage_root` 等 Settings 参数 |

---

## 3. 常见用法示例

### 3.1 统一读取数据 (`cqdata.read`)

```python
import cqdata

# 1. 读取日 K 线 (后复权) 数据
df_kline = cqdata.read(
    table_id="ashare.kline.1d.adj.baostock",
    symbols=["sh.600000", "sz.000001"],
    start_date="2024-01-01",
    end_date="2024-06-30",
    columns=["timestamp", "datetime", "symbol", "close", "volume"]
)
print(df_kline)

# 2. 读取板块成分股事件数据
df_concept = cqdata.read(
    table_id="ashare.concept.eastmoney",
    columns=["board_code", "board_name", "symbol", "stock_name"]
)
print(df_concept)
```

### 3.2 查看本地数据表总览 (`cqdata.list_tables`)

```python
import cqdata

tables = cqdata.list_tables()
print(tables)
# 输出示例:
# [
#   {"table_id": "ashare.kline.1d.raw.baostock", "category": "timeseries"},
#   {"table_id": "ashare.concept.eastmoney", "category": "event"}
# ]
```

### 3.3 转换为 Pandas DataFrame

由于返回对象为标准的 Polars DataFrame，直接调用 `.to_pandas()` 即可：

```python
import cqdata

df_pandas = cqdata.read("ashare.kline.1d.raw.baostock").to_pandas()
print(type(df_pandas))  # <class 'pandas.core.frame.DataFrame'>
```

---

## 4. 异常处理规约

SDK 中的读取与探查操作均遵循标准的 Python 异常体系：
- **`FileNotFoundError`**: 指定的 `table_id` 本地未查找到对应的 `metadata.json` 或物理存储文件。
- **`ValueError`**: 输入的参数非法（如不支持的格式 `format="invalid"`）。
