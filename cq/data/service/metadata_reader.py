"""
cqdata/service/metadata_reader.py

元数据探查与资源发现服务模块。
提供开箱即用的本地已持久化数据表、格式、股票代码列表、时间范围与 Schema 查询功能。
"""

from pathlib import Path
from typing import List, Tuple, Dict, Optional, Union

from cq.data.config import settings
from cq.data.service.metadata_manager import MetadataManager
from cq.data.provider.provider_manager import ProviderManager
from cq.data.storage.storage_factory import StorageFactory
from cq.data.utils.time_utils import ts_to_iso


class MetadataReader:
    """
    元数据只读查询服务类
    用于探测本地物理存储结构与元数据信息
    """

    def __init__(self, data_dir: Optional[Union[str, Path]] = None):
        """
        初始化 MetadataReader
        
        Args:
            data_dir: 数据存储根目录 (未指定时默认使用 settings.data_dir)
        """
        self.data_dir = Path(data_dir) if data_dir else Path(settings.data_dir)
        self.meta_mgr = MetadataManager(str(self.data_dir))

    def _determine_format(self, table_id: str, requested_format: str = "auto") -> str:
        """根据本地磁盘文件存在情况智能判断实际存储格式"""
        if requested_format != "auto":
            return requested_format.lower()

        # 检查 parquet
        parquet_dir = self.data_dir / "parquet" / table_id
        if parquet_dir.exists() and (self.meta_mgr._get_metadata_path(table_id, "parquet").exists() or any(parquet_dir.glob("**/*"))):
            return "parquet"

        # 检查 csv
        csv_dir = self.data_dir / "csv" / table_id
        if csv_dir.exists() and (self.meta_mgr._get_metadata_path(table_id, "csv").exists() or any(csv_dir.glob("**/*"))):
            return "csv"

        return "parquet"

    def _get_table_category(self, table_id: str, fmt: str) -> str:
        """获知表的数据分类 ('timeseries' 或 'event')，优先读取本地元数据"""
        meta = self.meta_mgr.load(table_id, fmt)
        if "category" in meta:
            return meta["category"]

        try:
            provider = ProviderManager().get_provider(table_id)
            return provider.get_table_category(table_id)
        except Exception:
            return "timeseries"

    def list_formats(self, table_id: str) -> List[str]:
        """
        获取指定数据表在本地物理存储中存在的所有格式
        
        Args:
            table_id: 表 ID (例如 'ashare.kline.1d.raw.baostock')
            
        Returns:
            List[str]: 本地存在的格式列表，例如 ['parquet', 'csv']
        """
        formats = []
        for fmt in ["parquet", "csv"]:
            fmt_dir = self.data_dir / fmt / table_id
            if fmt_dir.exists() and (self.meta_mgr._get_metadata_path(table_id, fmt).exists() or any(fmt_dir.glob("**/*"))):
                formats.append(fmt)
        return formats

    def list_series_tables(self, format: str = "auto") -> List[str]:
        """
        列出本地已下载/存在的所有【时间序列 (TimeSeries)】数据表
        
        Args:
            format: 指定格式 ('auto', 'parquet', 'csv')
            
        Returns:
            List[str]: 时序表 ID 列表
        """
        return self._list_tables_by_category(category="timeseries", requested_format=format)

    def list_event_tables(self, format: str = "auto") -> List[str]:
        """
        列出本地已下载/存在的所有【事件/静态 (Event)】数据表
        
        Args:
            format: 指定格式 ('auto', 'parquet', 'csv')
            
        Returns:
            List[str]: 事件表 ID 列表
        """
        return self._list_tables_by_category(category="event", requested_format=format)

    def _list_tables_by_category(self, category: str, requested_format: str = "auto") -> List[str]:
        """内部方法：按 category 扫描本地表 ID"""
        target_formats = ["parquet", "csv"] if requested_format == "auto" else [requested_format.lower()]
        found_tables = set()

        for fmt in target_formats:
            fmt_dir = self.data_dir / fmt
            if not fmt_dir.exists():
                continue

            for p in fmt_dir.iterdir():
                if p.is_dir():
                    table_id = p.name
                    # 校验 category
                    table_cat = self._get_table_category(table_id, fmt)
                    if table_cat == category:
                        found_tables.add(table_id)

        return sorted(list(found_tables))

    def list_symbols(self, table_id: str, format: str = "auto") -> List[str]:
        """
        获取指定数据表在本地已存储的所有 symbol 股票/指数代码列表
        
        Args:
            table_id: 表 ID
            format: 存储格式 ('auto', 'parquet', 'csv')
            
        Returns:
            List[str]: 唯一代码列表
        """
        fmt = self._determine_format(table_id, format)
        category = self._get_table_category(table_id, fmt)
        storage = StorageFactory.get_storage(
            storage_format=fmt,
            data_dir=str(self.data_dir),
            category=category
        )
        return storage.get_all_symbols(table_id)

    def get_time_range(self, table_id: str, format: str = "auto") -> Tuple[str, str]:
        """
        获取指定表的全局起始与结束 ISO 时间字符串 tuple (start_datetime, end_datetime)
        用于辅助用户进行时间切片过滤
        
        Args:
            table_id: 表 ID
            format: 存储格式 ('auto', 'parquet', 'csv')
            
        Returns:
            Tuple[str, str]: 起止 ISO 时间，如 ('2020-01-01T15:00:00.000+08:00', '2024-06-30T15:00:00.000+08:00')
                             若无有效时间戳返回 ('', '')
        """
        fmt = self._determine_format(table_id, format)
        meta = self.meta_mgr.load(table_id, fmt)
        stats = meta.get("statistics", {})

        start_dt = stats.get("start_datetime")
        end_dt = stats.get("end_datetime")

        if start_dt and end_dt:
            return (start_dt, end_dt)

        # 降级：从 Storage 获取最小最大时间戳
        category = self._get_table_category(table_id, fmt)
        storage = StorageFactory.get_storage(
            storage_format=fmt,
            data_dir=str(self.data_dir),
            category=category
        )
        min_ts, max_ts = storage.get_global_time_range(table_id)
        if min_ts == 0 and max_ts == 0:
            return ("", "")
        return (ts_to_iso(min_ts), ts_to_iso(max_ts))

    def get_schema(self, table_id: str, format: str = "auto") -> Dict[str, str]:
        """
        获取指定表在元数据中的字段 Schema 映射字典 (列名 -> 类型名)
        用于辅助用户在 read_series / read_events 中传入 columns 参数精确定向过滤
        
        Args:
            table_id: 表 ID
            format: 存储格式 ('auto', 'parquet', 'csv')
            
        Returns:
            Dict[str, str]: 例如 {"symbol": "String", "timestamp": "Int64", "close": "Float64"}
        """
        fmt = self._determine_format(table_id, format)
        meta = self.meta_mgr.load(table_id, fmt)
        return meta.get("schema", {})

    def get_row_count(self, table_id: str, format: str = "auto") -> int:
        """
        获取指定表在物理存储中的总记录行数/条目数
        
        Args:
            table_id: 表 ID
            format: 存储格式 ('auto', 'parquet', 'csv')
            
        Returns:
            int: 物理记录总行数
        """
        fmt = self._determine_format(table_id, format)
        meta = self.meta_mgr.load(table_id, fmt)
        stats = meta.get("statistics", {})

        if "total_bars" in stats:
            return stats["total_bars"]

        # 降级从 Storage 实时极速统计
        category = self._get_table_category(table_id, fmt)
        storage = StorageFactory.get_storage(
            storage_format=fmt,
            data_dir=str(self.data_dir),
            category=category
        )
        return storage.get_total_bars(table_id)


# 快捷导出的单例/包装函数
def list_series_tables(format: str = "auto", data_dir: Optional[Union[str, Path]] = None) -> List[str]:
    """列出本地所有【时间序列 (TimeSeries)】表 ID"""
    mr = MetadataReader(data_dir=data_dir)
    return mr.list_series_tables(format=format)


def list_event_tables(format: str = "auto", data_dir: Optional[Union[str, Path]] = None) -> List[str]:
    """列出本地所有【事件/静态 (Event)】表 ID"""
    mr = MetadataReader(data_dir=data_dir)
    return mr.list_event_tables(format=format)


def list_formats(table_id: str, data_dir: Optional[Union[str, Path]] = None) -> List[str]:
    """列出某表本地存在的所有存储格式"""
    mr = MetadataReader(data_dir=data_dir)
    return mr.list_formats(table_id=table_id)


def list_symbols(table_id: str, format: str = "auto", data_dir: Optional[Union[str, Path]] = None) -> List[str]:
    """列出某表已下载的股票/指数代码列表"""
    mr = MetadataReader(data_dir=data_dir)
    return mr.list_symbols(table_id=table_id, format=format)


def get_time_range(table_id: str, format: str = "auto", data_dir: Optional[Union[str, Path]] = None) -> Tuple[str, str]:
    """获取某表的起止 ISO 时间 tuple"""
    mr = MetadataReader(data_dir=data_dir)
    return mr.get_time_range(table_id=table_id, format=format)


def get_schema(table_id: str, format: str = "auto", data_dir: Optional[Union[str, Path]] = None) -> Dict[str, str]:
    """获取某表字段名称及数据类型字典"""
    mr = MetadataReader(data_dir=data_dir)
    return mr.get_schema(table_id=table_id, format=format)


def get_row_count(table_id: str, format: str = "auto", data_dir: Optional[Union[str, Path]] = None) -> int:
    """获取某表在物理存储中的总记录行数"""
    mr = MetadataReader(data_dir=data_dir)
    return mr.get_row_count(table_id=table_id, format=format)
