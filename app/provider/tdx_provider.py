"""通达信数据源驱动实现。

两种数据获取模式:
  1. local:  读取本地 vipdoc 目录 (由 download_tdx.py 脚本下载解压)
  2. online: 通过 tdxpy TCP 在线获取

支持的 table_id:
  - ashare.kline.1d.raw.tdx   (TS) A股个股日线
  - ashare.kline.5m.raw.tdx   (TS) A股个股5分钟线
  - ashare.kline.1m.raw.tdx   (TS) A股个股1分钟线
  - aindex.kline.1d.raw.tdx   (TS) 指数日线
  - aindex.kline.5m.raw.tdx   (TS) 指数5分钟线
  - aindex.kline.1m.raw.tdx   (TS) 指数1分钟线

仅支持 raw (不复权) 数据，复权需求请使用 Baostock。
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import polars as pl
from loguru import logger

from app.provider.base import BaseProvider
from app.provider.data_cleaner import DataCleaner
from app.provider.tdx_utils import (
    discover_tdx_symbols_from_local,
    read_tdx_file_from_local,
    tdx_code_to_standard,
    standard_to_tdx_code,
    fetch_bars_online,
    fetch_stock_list_online,
    _empty_kline_df,
)
from app.utils.time_utils import ts_to_str


class TDXProvider(BaseProvider):
    """通达信数据源驱动，支持 local (vipdoc) 和 online (tdxpy TCP) 两种模式。"""

    _SUPPORTED_TABLE_MAP: dict[str, str] = {
        "ashare.kline.1d.raw.tdx": "timeseries",
        "ashare.kline.5m.raw.tdx": "timeseries",
        "ashare.kline.1m.raw.tdx": "timeseries",
        "aindex.kline.1d.raw.tdx": "timeseries",
        "aindex.kline.5m.raw.tdx": "timeseries",
        "aindex.kline.1m.raw.tdx": "timeseries",
    }

    def __init__(
        self,
        mode: str = "online",
        vipdoc_dir: str = r"C:\new_tdx\vipdoc",
    ):
        """初始化 TDX Provider。

        Args:
            mode: 数据获取模式 ("local" / "online")
            vipdoc_dir: vipdoc 目录路径 (local 模式使用)
        """
        self._mode = mode
        self._vipdoc_dir = Path(vipdoc_dir)

    def get_supported_tables(self) -> list[str]:
        return list(self._SUPPORTED_TABLE_MAP.keys())

    def get_table_category(self, table_id: str) -> str:
        if table_id not in self._SUPPORTED_TABLE_MAP:
            raise ValueError(f"Table '{table_id}' is not supported by TDXProvider.")
        return self._SUPPORTED_TABLE_MAP[table_id]

    def get_sort_keys(self, table_id: str) -> list[str]:
        return ["timestamp"]

    def get_all_symbols(self, table_id: str) -> list[str]:
        if table_id not in self.get_supported_tables():
            raise ValueError(f"Table '{table_id}' is not supported by TDXProvider.")
        prefix = table_id.split('.')[0]

        if self._mode == "online":
            return self._get_all_symbols_online(prefix)
        return self._get_all_symbols_local(prefix)

    def _get_all_symbols_local(self, prefix: str) -> list[str]:
        raw = discover_tdx_symbols_from_local(self._vipdoc_dir)

        if prefix == "aindex":
            return [tdx_code_to_standard(c) for c in raw if c[2:].startswith(('000', '399'))]
        sh_symbols = [c for c in raw if c[:2] == 'sh' and c[2:].startswith('6')]
        sz_symbols = [c for c in raw if c[:2] == 'sz' and c[2:].startswith(('0', '3'))]
        bj_symbols = [tdx_code_to_standard(c) for c in raw if c[2:].startswith(('920', '83', '87', '43'))]
        symbols = sorted([tdx_code_to_standard(c) for c in sh_symbols + sz_symbols] + bj_symbols)
        logger.info(f"Discovered {len(symbols)} symbols (local)")
        return symbols

    def _get_all_symbols_online(self, prefix: str) -> list[str]:
        if prefix == "aindex":
            sh = fetch_stock_list_online(market="sh")
            sz = fetch_stock_list_online(market="sz")
            return sorted([tdx_code_to_standard(c) for c in sh + sz if c[2:].startswith(('000', '399'))])
        sh = fetch_stock_list_online(market="sh")
        sz = fetch_stock_list_online(market="sz")
        sh_symbols = [c for c in sh if c[2:].startswith('6')]
        sz_symbols = [c for c in sz if c[2:].startswith(('0', '3'))]
        symbols = sorted([tdx_code_to_standard(c) for c in sh_symbols + sz_symbols])
        logger.info(f"Discovered {len(symbols)} symbols (online, sh+sz)")
        return symbols

    def fetch(
        self, table_id: str, symbol: str, start_date: Any, end_date: Any, **kwargs
    ) -> pl.DataFrame:
        if table_id not in self.get_supported_tables():
            raise ValueError(f"Table '{table_id}' is not supported by TDXProvider.")

        if isinstance(start_date, int):
            start_date = ts_to_str(start_date)
        if isinstance(end_date, int):
            end_date = ts_to_str(end_date)
        if start_date is None:
            start_date = "2020-01-01"
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        parts = table_id.split('.')
        freq = parts[2] if len(parts) > 2 else '1d'

        if self._mode == "online":
            return self._fetch_online(table_id, symbol, freq, start_date, end_date)
        return self._fetch_local(table_id, symbol, freq, start_date, end_date)

    def _fetch_online(
        self, table_id: str, symbol: str, freq: str, start_date: str, end_date: str
    ) -> pl.DataFrame:
        df = fetch_bars_online(symbol, freq=freq, start_date=start_date, end_date=end_date, table_id=table_id)
        if df.is_empty():
            return _empty_kline_df()
        is_minute = freq in ("5m", "1m")
        if is_minute:
            result = DataCleaner.standardize(
                df, "datetime", time_fmt="%Y-%m-%d %H:%M",
                source_tz="Asia/Shanghai", display_tz="Asia/Shanghai",
            )
        else:
            result = DataCleaner.standardize(
                df, "date", time_fmt="%Y-%m-%d",
                source_tz="Asia/Shanghai", display_tz="Asia/Shanghai",
                time_shift_hours=15,
            )
        if "volume" in result.columns and result.schema["volume"] == pl.Int64:
            result = result.with_columns(pl.col("volume").cast(pl.Float64))
        return result

    def _fetch_local(
        self, table_id: str, symbol: str, freq: str,
        start_date: str, end_date: str
    ) -> pl.DataFrame:
        tdx_code = standard_to_tdx_code(symbol)
        records = read_tdx_file_from_local(self._vipdoc_dir, tdx_code, freq=freq)

        if not records:
            return _empty_kline_df()

        if freq == '1d':
            df = pl.DataFrame(records, schema={
                "date": pl.String,
                "open": pl.Float64, "high": pl.Float64,
                "low": pl.Float64, "close": pl.Float64,
                "volume": pl.Float64, "amount": pl.Float64,
            })
            df = df.with_columns(pl.lit(symbol).alias("symbol"))
            df = df.filter(
                (pl.col("date") >= start_date) & (pl.col("date") <= end_date)
            )
            return DataCleaner.standardize(
                df, "date", time_fmt="%Y-%m-%d",
                source_tz="Asia/Shanghai", display_tz="Asia/Shanghai",
                time_shift_hours=15,
            )
        else:
            df = pl.DataFrame(records, schema={
                "datetime": pl.String, "date": pl.String, "time": pl.String,
                "open": pl.Float64, "high": pl.Float64,
                "low": pl.Float64, "close": pl.Float64,
                "volume": pl.Float64, "amount": pl.Float64,
            })
            df = df.with_columns(pl.lit(symbol).alias("symbol"))
            df = df.filter(
                (pl.col("date") >= start_date) & (pl.col("date") <= end_date)
            )
            return DataCleaner.standardize(
                df, "datetime", time_fmt="%Y-%m-%d %H:%M:%S",
                source_tz="Asia/Shanghai", display_tz="Asia/Shanghai",
            )
