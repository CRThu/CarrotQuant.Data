"""
cqdata/entrypoints/accessors/ashare.py

A 股个股数据 OOP 访问类与命名空间实现。
"""

from typing import List, Optional, Union
import polars as pl

from cqdata.entrypoints.accessors.base import _BaseTable, DefaultConfig


class AShareKline(_BaseTable):
    """A 股个股 K 线快捷访问类"""
    _PREFIX = "ashare.kline"
    _FALLBACK_SOURCE = "baostock"

    def get(
        self,
        freq: str = "1d",
        adj: str = "raw",
        symbols: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        columns: Optional[Union[str, List[str]]] = None,
        source: Optional[str] = None,
        format: Optional[str] = None
    ) -> pl.DataFrame:
        """
        读取 A 股个股 K 线数据。

        Args:
            freq: K 线频率，默认 '1d' (可选 '1d', '5m', '1m')
            adj: 复权方式，默认 'raw' (可选 'raw', 'adj')
            symbols: 代码或代码列表 (如 'sh.600000')
            start_date: 起始日期 ('YYYY-MM-DD')
            end_date: 结束日期 ('YYYY-MM-DD')
            columns: 选挑字段列表
            source: 指定数据源 ('baostock', 'tdx' 等)
            format: 存储格式 ('parquet', 'csv', 'auto')

        Returns:
            pl.DataFrame
        """
        resolved_source, resolved_format = self._resolve_source_format(source, format)
        table_id = f"{self._PREFIX}.{freq}.{adj}.{resolved_source}"
        return self._read_table(
            table_id=table_id,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            columns=columns,
            format=resolved_format
        )


class AShareAdjFactor(_BaseTable):
    """A 股复权因子快捷访问类"""
    _PREFIX = "ashare.adj_factor"
    _FALLBACK_SOURCE = "baostock"

    def get(
        self,
        symbols: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        columns: Optional[Union[str, List[str]]] = None,
        source: Optional[str] = None,
        format: Optional[str] = None
    ) -> pl.DataFrame:
        resolved_source, resolved_format = self._resolve_source_format(source, format)
        table_id = f"{self._PREFIX}.{resolved_source}"
        return self._read_table(table_id, symbols, start_date, end_date, columns, resolved_format)


class AShareConcept(_BaseTable):
    """A 股概念板块成分股快捷访问类"""
    _PREFIX = "ashare.concept"
    _FALLBACK_SOURCE = "eastmoney"

    def get(
        self,
        symbols: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        columns: Optional[Union[str, List[str]]] = None,
        source: Optional[str] = None,
        format: Optional[str] = None
    ) -> pl.DataFrame:
        resolved_source, resolved_format = self._resolve_source_format(source, format)
        table_id = f"{self._PREFIX}.{resolved_source}"
        return self._read_table(table_id, symbols, start_date, end_date, columns, resolved_format)


class AShareIndustry(_BaseTable):
    """A 股行业板块成分股快捷访问类"""
    _PREFIX = "ashare.industry"
    _FALLBACK_SOURCE = "eastmoney"

    def get(
        self,
        symbols: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        columns: Optional[Union[str, List[str]]] = None,
        source: Optional[str] = None,
        format: Optional[str] = None
    ) -> pl.DataFrame:
        resolved_source, resolved_format = self._resolve_source_format(source, format)
        table_id = f"{self._PREFIX}.{resolved_source}"
        return self._read_table(table_id, symbols, start_date, end_date, columns, resolved_format)


class AShareDragonTiger(_BaseTable):
    """A 股龙虎榜快捷访问类"""
    _PREFIX = "ashare.dragon_tiger"
    _FALLBACK_SOURCE = "eastmoney"

    def get(
        self,
        symbols: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        columns: Optional[Union[str, List[str]]] = None,
        source: Optional[str] = None,
        format: Optional[str] = None
    ) -> pl.DataFrame:
        resolved_source, resolved_format = self._resolve_source_format(source, format)
        table_id = f"{self._PREFIX}.{resolved_source}"
        return self._read_table(table_id, symbols, start_date, end_date, columns, resolved_format)


class AShareInstTrade(_BaseTable):
    """A 股机构交易每日统计快捷访问类"""
    _PREFIX = "ashare.inst_trade"
    _FALLBACK_SOURCE = "eastmoney"

    def get(
        self,
        symbols: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        columns: Optional[Union[str, List[str]]] = None,
        source: Optional[str] = None,
        format: Optional[str] = None
    ) -> pl.DataFrame:
        resolved_source, resolved_format = self._resolve_source_format(source, format)
        table_id = f"{self._PREFIX}.{resolved_source}"
        return self._read_table(table_id, symbols, start_date, end_date, columns, resolved_format)


class AShare:
    """A 股数据命名空间类"""

    def __init__(self, parent_default: DefaultConfig):
        self.default = DefaultConfig(parent=parent_default)
        self.kline = AShareKline(parent_default=self.default)
        self.adj_factor = AShareAdjFactor(parent_default=self.default)
        self.concept = AShareConcept(parent_default=self.default)
        self.industry = AShareIndustry(parent_default=self.default)
        self.dragon_tiger = AShareDragonTiger(parent_default=self.default)
        self.inst_trade = AShareInstTrade(parent_default=self.default)
