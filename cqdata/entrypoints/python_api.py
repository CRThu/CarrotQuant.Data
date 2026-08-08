"""
cqdata/entrypoints/python_api.py

Python SDK API 接入面模块。
为 Python 量化脚本与交互环境提供干净、专一、直观的高阶函数调用。
"""

from typing import List, Tuple, Dict, Union, Optional
from pathlib import Path
import polars as pl

from cqdata.service import data_reader as dr
from cqdata.service import metadata_reader as mr
from cqdata.service import sync_manager as sm
from cqdata.config import settings
from cqdata.service.metadata_manager import MetadataManager


def read(
    table_id: str,
    symbols: Optional[Union[str, List[str]]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    columns: Optional[Union[str, List[str]]] = None,
    format: str = "auto"
) -> pl.DataFrame:
    """
    统一数据切片读取入口 (直接读取本地元数据记载的 category 智能路由，无元数据直接报错)

    Args:
        table_id: 数据集 ID (例如 'ashare.kline.1d.raw.baostock' 或 'ashare.dragon_tiger.eastmoney')
        symbols: 证券代码或代码列表
        start_date: 起始日期 ('YYYY-MM-DD')
        end_date: 结束日期 ('YYYY-MM-DD')
        columns: 选挑字段清单 (例如 ['timestamp', 'close', 'volume'])
        format: 存储格式 ('auto', 'parquet', 'csv')

    Returns:
        pl.DataFrame
    """
    # 1. 自动解析生效的存储格式 (parquet 或 csv)
    formats = mr.list_formats(table_id)
    if not formats:
        raise FileNotFoundError(f"No storage found for table_id '{table_id}'. Please sync data first.")

    real_fmt = formats[0] if format == "auto" else format

    # 2. 直接读取物理元数据记载的真实 category (元数据不存在自动抛出 FileNotFoundError)
    meta = MetadataManager(settings.storage_path).load(table_id, real_fmt)
    category = meta.get("category", "timeseries")

    # 3. 路由调用切片读取器
    if category == "event":
        return dr.read_events(
            table_id=table_id,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            columns=columns,
            format=real_fmt
        )
    else:
        return dr.read_series(
            table_id=table_id,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            columns=columns,
            format=real_fmt
        )


def list_tables(format: str = "auto") -> List[Dict[str, str]]:
    """
    列出本地已存在的所有数据表清单及分类信息 (平铺对象列表)

    Returns:
        List[Dict[str, str]]: [{'table_id': '...', 'category': 'timeseries'|'event'}, ...]
    """
    series_tables = mr.list_series_tables(format=format)
    event_tables = mr.list_event_tables(format=format)

    tables = [{"table_id": tid, "category": "timeseries"} for tid in series_tables]
    tables.extend([{"table_id": tid, "category": "event"} for tid in event_tables])
    return tables


def list_formats(table_id: str) -> List[str]:
    """获取指定表在本地存在的所有存储格式"""
    return mr.list_formats(table_id=table_id)


def list_symbols(table_id: str, format: str = "auto") -> List[str]:
    """获取指定表在本地已存储的所有 symbol 股票/指数代码列表"""
    return mr.list_symbols(table_id=table_id, format=format)


def get_time_range(table_id: str, format: str = "auto") -> Tuple[str, str]:
    """获取指定表的全局起止 ISO 时间字符串 tuple (start_datetime, end_datetime)"""
    return mr.get_time_range(table_id=table_id, format=format)


def get_schema(table_id: str, format: str = "auto") -> Dict[str, str]:
    """获取指定表在元数据中的字段 Schema 映射字典 (列名 -> 类型名)"""
    return mr.get_schema(table_id=table_id, format=format)


def get_row_count(table_id: str, format: str = "auto") -> int:
    """获取指定表在物理存储中的总记录行数/条目数"""
    return mr.get_row_count(table_id=table_id, format=format)


def sync(
    table_ids: Union[List[str], str],
    formats: Union[List[str], str] = "parquet",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    force_refresh: bool = False,
    batch_size: int = 100,
    symbol_limit: Optional[int] = None,
    provider_kwargs: Optional[dict] = None
):
    """
    触发全自动化同步闭环的快捷函数
    
    Args:
        table_ids: 表 ID 或表 ID 列表
        formats: 落地格式 ('parquet', 'csv', 或 ['parquet', 'csv'])
        start_date: 起始日期 ('YYYY-MM-DD')
        end_date: 结束日期 ('YYYY-MM-DD')
        force_refresh: 是否强制全量覆盖刷新
        batch_size: 批处理数量
        symbol_limit: 限制处理的证券数
        provider_kwargs: 传递给 Provider 的选项字典
    """
    return sm.sync(
        table_ids=table_ids,
        formats=formats,
        start_date=start_date,
        end_date=end_date,
        force_refresh=force_refresh,
        batch_size=batch_size,
        symbol_limit=symbol_limit,
        provider_kwargs=provider_kwargs
    )


def configure(config_path: Union[str, Path]):
    """
    全局加载 YAML 配置文件。
    可以在 import cqdata 后在代码入口处显式调用。

    示例:
        import cqdata
        cqdata.configure("./config.yaml")
    """
    from cqdata.config import settings
    return settings.configure(config_path=config_path)
