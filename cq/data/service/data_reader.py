"""
cqdata/service/data_reader.py

高级数据读取服务模块。
为量化研究员提供显式、高性能的 Python 数据查询接口。
包含 read_series (时序切片) 与 read_events (事件切片)，支持按列投影 columns、多代码 symbols 过滤、时间切片与 Polars/Pandas 自动转换。
"""

from pathlib import Path
from typing import List, Union, Optional
import polars as pl

from cq.data.config import settings
from cq.data.service.metadata_manager import MetadataManager
from cq.data.provider.provider_manager import ProviderManager
from cq.data.storage.storage_factory import StorageFactory
from cq.data.utils.time_utils import parse_date_to_ts, align_to_day_end


class DataReader:
    """
    数据读取核心服务类
    处理跨年份切片、按列投影、条件下推与动态类型对齐
    """

    def __init__(self, data_dir: Optional[Union[str, Path]] = None):
        """
        初始化 DataReader
        
        Args:
            data_dir: 自定义数据存储根目录，未指定时使用全局 settings.data_dir
        """
        self.data_dir = Path(data_dir) if data_dir else Path(settings.data_dir)
        self.meta_mgr = MetadataManager(str(self.data_dir))

    def _determine_format(self, table_id: str, requested_format: str) -> str:
        """确定实际使用的存储格式。如果 requested_format 为 'auto'，优先选 parquet，其次选 csv。"""
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
        """从元数据或 ProviderManager 推断数据集类别 (timeseries 或 event)"""
        metadata = self.meta_mgr.load(table_id, fmt)
        if "category" in metadata:
            return metadata["category"]

        try:
            provider = ProviderManager().get_provider(table_id)
            return provider.get_table_category(table_id)
        except Exception:
            return "timeseries"

    def _apply_column_selection(self, df: pl.DataFrame, columns: Optional[Union[str, List[str]]]) -> pl.DataFrame:
        """按需进行列投影选择 (只选择指定的列返回)"""
        if df.is_empty() or not columns:
            return df

        col_list = [columns] if isinstance(columns, str) else columns
        # 仅保留 DataFrame 中真正存在的列，避免 KeyError
        valid_cols = [c for c in col_list if c in df.columns]
        if not valid_cols:
            return df

        return df.select(valid_cols)

    def read_series(
        self,
        table_id: str,
        symbols: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        columns: Optional[Union[str, List[str]]] = None,
        format: str = "auto"
    ):
        """
        读取【时间序列 (TimeSeries)】数据
        
        Args:
            table_id: 数据表 ID (如 'ashare.kline.1d.raw.baostock')
            symbols: 证券代码或代码列表 ('sh.600000' 或 ['sh.600000', 'sz.000001'])
            start_date: 起始日期字符串 (格式 'YYYY-MM-DD' 或 ISO)
            end_date: 结束日期字符串 (格式 'YYYY-MM-DD' 或 ISO)
            columns: 列挑选清单 (如 ['timestamp', 'close', 'volume'])，降低开销
            format: 存储格式 ('auto', 'parquet', 'csv')
            
        Returns:
            pl.DataFrame: 查询切片后的数据
        """
        return self._read_data(
            table_id=table_id,
            target_category="timeseries",
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            columns=columns,
            format=format
        )

    def read_events(
        self,
        table_id: str,
        symbols: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        columns: Optional[Union[str, List[str]]] = None,
        format: str = "auto"
    ):
        """
        读取【事件/静态 (Event)】数据
        
        Args:
            table_id: 数据表 ID (如 'ashare.dragon_tiger.eastmoney')
            symbols: 证券代码或代码列表
            start_date: 起始日期字符串
            end_date: 结束日期字符串
            columns: 列挑选清单
            format: 存储格式 ('auto', 'parquet', 'csv')
            
        Returns:
            pl.DataFrame: 查询切片后的数据
        """
        return self._read_data(
            table_id=table_id,
            target_category="event",
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            columns=columns,
            format=format
        )

    def _read_data(
        self,
        table_id: str,
        target_category: str,
        symbols: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        columns: Optional[Union[str, List[str]]] = None,
        format: str = "auto"
    ):
        """底层统一通用切片与加载核心实现"""
        # 1. 规范化 symbols 参数
        if isinstance(symbols, str):
            symbol_list = [symbols]
        elif isinstance(symbols, list):
            symbol_list = symbols
        else:
            symbol_list = None

        # 2. 确认物理存储格式与 Category
        fmt = self._determine_format(table_id, format)
        category = self._get_table_category(table_id, fmt)

        # 3. 解析时间区间毫秒戳
        start_ts = parse_date_to_ts(start_date) if start_date else None
        end_ts = align_to_day_end(parse_date_to_ts(end_date)) if end_date else None

        # 4. 获取 Storage Manager 实例
        storage = StorageFactory.get_storage(
            storage_format=fmt,
            data_dir=str(self.data_dir),
            category=category
        )

        # 5. 决定涉及的年份
        years = None
        if start_date and end_date:
            try:
                start_year = int(start_date[:4])
                end_year = int(end_date[:4])
                years = list(range(start_year, end_year + 1))
            except ValueError:
                years = None

        # 如果无法从日期推导年份，扫描磁盘获取存在数据的年份
        if years is None:
            base_dir = self.data_dir / fmt / table_id
            if base_dir.exists():
                year_dirs = [d.name for d in base_dir.glob("year=*") if d.is_dir()]
                found_years = []
                for yd in year_dirs:
                    try:
                        found_years.append(int(yd.split("=")[1]))
                    except (IndexError, ValueError):
                        pass
                years = sorted(found_years)
            else:
                years = []

        dfs: List[pl.DataFrame] = []

        # 6. 读取分支处理
        if category == "timeseries":
            for y in years:
                if symbol_list:
                    for sym in symbol_list:
                        df_part = storage.read_series(table_id, sym, y)
                        if not df_part.is_empty():
                            dfs.append(df_part)
                else:
                    if fmt == "parquet":
                        path = storage._get_series_path(table_id, y)
                        if path.exists():
                            df_part = storage._read_with_schema(table_id, path)
                            if not df_part.is_empty():
                                dfs.append(df_part)
                    else: # CSV
                        all_syms = storage.get_all_symbols(table_id)
                        for sym in all_syms:
                            df_part = storage.read_series(table_id, sym, y)
                            if not df_part.is_empty():
                                dfs.append(df_part)
        else: # Event
            if storage._is_flat_event(table_id):
                df_part = storage.read_event(table_id)
                if not df_part.is_empty():
                    dfs.append(df_part)
            else:
                for y in years:
                    df_part = storage.read_event(table_id, y)
                    if not df_part.is_empty():
                        dfs.append(df_part)

        # 7. 合并 DataFrame
        if not dfs:
            result_df = pl.DataFrame()
        else:
            result_df = pl.concat(dfs, how="vertical_relaxed")

        # 8. 内存精确过滤与排序
        if not result_df.is_empty():
            # timestamp 时间范围过滤
            if "timestamp" in result_df.columns:
                if start_ts is not None:
                    result_df = result_df.filter(pl.col("timestamp") >= start_ts)
                if end_ts is not None:
                    result_df = result_df.filter(pl.col("timestamp") <= end_ts)

            # symbol 筛选
            if symbol_list and "symbol" in result_df.columns:
                result_df = result_df.filter(pl.col("symbol").is_in(symbol_list))

            # 排序
            if category == "timeseries" and "timestamp" in result_df.columns and "symbol" in result_df.columns:
                result_df = result_df.sort(["timestamp", "symbol"])
            elif "timestamp" in result_df.columns:
                result_df = result_df.sort("timestamp")

            # 按需列挑选 (columns)
            result_df = self._apply_column_selection(result_df, columns)

        return result_df


# 快捷工具 API 函数
def read_series(
    table_id: str,
    symbols: Optional[Union[str, List[str]]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    columns: Optional[Union[str, List[str]]] = None,
    format: str = "auto",
    data_dir: Optional[Union[str, Path]] = None
):
    """读取时间序列数据的快捷函数"""
    reader = DataReader(data_dir=data_dir)
    return reader.read_series(
        table_id=table_id,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
        format=format
    )


def read_events(
    table_id: str,
    symbols: Optional[Union[str, List[str]]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    columns: Optional[Union[str, List[str]]] = None,
    format: str = "auto",
    data_dir: Optional[Union[str, Path]] = None
):
    """读取事件/静态数据的快捷函数"""
    reader = DataReader(data_dir=data_dir)
    return reader.read_events(
        table_id=table_id,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
        format=format
    )
