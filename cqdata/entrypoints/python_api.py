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


def read_series(
    table_id: str,
    symbols: Optional[Union[str, List[str]]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    columns: Optional[Union[str, List[str]]] = None,
    format: str = "auto",
    storage_root: Optional[Union[str, Path]] = None
) -> pl.DataFrame:
    """
    切片读取【时间序列 (TimeSeries)】数据 (如 K 线、分笔)
    
    Args:
        table_id: 数据集 ID (例如 'ashare.kline.1d.raw.baostock')
        symbols: 证券代码或代码列表 ('sh.600000' 或 ['sh.600000', 'sz.000001'])
        start_date: 起始日期 ('YYYY-MM-DD')
        end_date: 结束日期 ('YYYY-MM-DD')
        columns: 选挑字段清单 (例如 ['timestamp', 'close', 'volume'])
        format: 存储格式 ('auto', 'parquet', 'csv')
        storage_root: 自定义存储根目录
        
    Returns:
        pl.DataFrame
    """
    return dr.read_series(
        table_id=table_id,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
        format=format,
        storage_root=storage_root
    )


def read_events(
    table_id: str,
    symbols: Optional[Union[str, List[str]]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    columns: Optional[Union[str, List[str]]] = None,
    format: str = "auto",
    storage_root: Optional[Union[str, Path]] = None
) -> pl.DataFrame:
    """
    切片读取【事件/静态 (Event)】数据 (如板块成分股、龙虎榜、机构交易)
    
    Args:
        table_id: 数据集 ID (例如 'ashare.dragon_tiger.eastmoney')
        symbols: 证券代码或代码列表
        start_date: 起始日期
        end_date: 结束日期
        columns: 选挑字段清单
        format: 存储格式 ('auto', 'parquet', 'csv')
        storage_root: 自定义存储根目录
        
    Returns:
        pl.DataFrame
    """
    return dr.read_events(
        table_id=table_id,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
        format=format,
        storage_root=storage_root
    )


def list_series_tables(format: str = "auto", storage_root: Optional[Union[str, Path]] = None) -> List[str]:
    """列出本地已存在的所有【时间序列 (TimeSeries)】数据表 ID"""
    return mr.list_series_tables(format=format, storage_root=storage_root)


def list_event_tables(format: str = "auto", storage_root: Optional[Union[str, Path]] = None) -> List[str]:
    """列出本地已存在的所有【事件/静态 (Event)】数据表 ID"""
    return mr.list_event_tables(format=format, storage_root=storage_root)


def list_formats(table_id: str, storage_root: Optional[Union[str, Path]] = None) -> List[str]:
    """获取指定表在本地存在的所有存储格式"""
    return mr.list_formats(table_id=table_id, storage_root=storage_root)


def list_symbols(table_id: str, format: str = "auto", storage_root: Optional[Union[str, Path]] = None) -> List[str]:
    """获取指定表在本地已存储的所有 symbol 股票/指数代码列表"""
    return mr.list_symbols(table_id=table_id, format=format, storage_root=storage_root)


def get_time_range(table_id: str, format: str = "auto", storage_root: Optional[Union[str, Path]] = None) -> Tuple[str, str]:
    """获取指定表的全局起止 ISO 时间字符串 tuple (start_datetime, end_datetime)"""
    return mr.get_time_range(table_id=table_id, format=format, storage_root=storage_root)


def get_schema(table_id: str, format: str = "auto", storage_root: Optional[Union[str, Path]] = None) -> Dict[str, str]:
    """获取指定表在元数据中的字段 Schema 映射字典 (列名 -> 类型名)"""
    return mr.get_schema(table_id=table_id, format=format, storage_root=storage_root)


def get_row_count(table_id: str, format: str = "auto", storage_root: Optional[Union[str, Path]] = None) -> int:
    """获取指定表在物理存储中的总记录行数/条目数"""
    return mr.get_row_count(table_id=table_id, format=format, storage_root=storage_root)


def sync(
    table_ids: Union[List[str], str],
    formats: Union[List[str], str] = "parquet",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    force_refresh: bool = False,
    batch_size: int = 100,
    symbol_limit: Optional[int] = None,
    storage_root: Optional[str] = None,
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
        storage_root: 自定义存储根目录
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
        storage_root=storage_root,
        provider_kwargs=provider_kwargs
    )


def configure(
    storage_root: Optional[Union[str, Path]] = None,
    config_file: Optional[Union[str, Path]] = None,
    **kwargs
):
    """
    全局配置 CarrotQuant.Data 运行参数。
    可以在 import cqdata 后在代码入口处显式调用。

    示例:
        import cqdata
        cqdata.configure(storage_root="/path/to/my_storage")
    """
    from cqdata.config import settings
    return settings.configure(storage_root=storage_root, config_file=config_file, **kwargs)


# 别名导出
set_config = configure


def get_config():
    """获取当前全局 Settings 实例"""
    from cqdata.config import settings
    return settings

