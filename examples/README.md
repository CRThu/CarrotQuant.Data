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

---

## 💡 通达信 (TDX) 同步特别说明

通达信驱动支持 **Local 离线包** 与 **Online 在线 TCP** 两种同步模式：

> [!TIP]
> **最佳推荐实践**：
> 1. **历史全量日线**：推荐运行 `uv run scripts/download_tdx.py` 或 `cqdata tdx download` 先下载通达信官方全量日线 `vipdoc` 离线包极速初始化。
> 2. **盘后增量/分钟线**：日常通过 `cqdata.sync(table_ids=["ashare.kline.1d.raw.tdx"])` 在线拉取最新增量或分钟线，系统将自动去重与合并。

