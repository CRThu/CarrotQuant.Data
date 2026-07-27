# CarrotQuant.Data (cqdata) Python SDK 示例代码与文档

本目录提供了 `cqdata` SDK 最常用功能的模块化、极简 Python 示例代码，方便直接复制使用或作为开发参考。

---

## 示例清单

| 文件名 | 说明 | 重点 API |
| :--- | :--- | :--- |
| [`01_quickstart.py`](01_quickstart.py) | 快速上手基础示例 | `cqdata.sync`, `cqdata.read_series` |
| [`02_sync_data.py`](02_sync_data.py) | 数据同步与调度示例 | `cqdata.sync` (支持不同存储格式/全量/增量) |
| [`03_read_series.py`](03_read_series.py) | 时序数据切片读取 | `read_series` (**支持不复权 raw 与 后复权 adj**) |
| [`04_read_events.py`](04_read_events.py) | 事件/静态数据切片读取 | `read_events` (概念板块/龙虎榜等) |
| [`05_export_pandas.py`](05_export_pandas.py) | Pandas DataFrame 转码 | `read_series` / `read_events` (`as_pandas=True`) |
| [`06_metadata_inspection.py`](06_metadata_inspection.py) | 元数据与存储盘点示例 | `cqdata.list_series_tables`, `get_time_range`, `get_schema` 等 |

---

## 运行方法

在项目根目录下，直接使用 `uv` 运行对应的脚本即可：

```bash
uv run python examples/01_quickstart.py
uv run python examples/02_sync_data.py
uv run python examples/03_read_series.py
uv run python examples/04_read_events.py
uv run python examples/05_export_pandas.py
uv run python examples/06_metadata_inspection.py
```

