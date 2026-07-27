"""
CarrotQuant.Data (cqdata)

轻量级、模块化的本地金融数据同步与管理工具。

对外暴露的统一 Python SDK 高效 API:
- read_series: 切片读取 K 线/分笔等时间序列数据
- read_events: 切片读取板块/龙虎榜等事件数据
- list_series_tables: 列出本地已有的时序表
- list_event_tables: 列出本地已有的事件表
- list_formats: 查看某表已有的存储格式
- list_symbols: 查看某表已有的股票/指数代码列表
- get_time_range: 获取某表的时间起止跨度
- get_schema: 获取某表的字段列定义与类型
- get_row_count: 获取某表物理存储记录条数
- sync: 触发数据全自动同步
"""

from cqdata.entrypoints import (
    read_series,
    read_events,
    list_series_tables,
    list_event_tables,
    list_formats,
    list_symbols,
    get_time_range,
    get_schema,
    get_row_count,
    sync,
    configure,
    set_config,
    get_config
)

__version__ = "0.1.1"

__all__ = [
    "read_series",
    "read_events",
    "list_series_tables",
    "list_event_tables",
    "list_formats",
    "list_symbols",
    "get_time_range",
    "get_schema",
    "get_row_count",
    "sync",
    "configure",
    "set_config",
    "get_config",
    "__version__"
]
