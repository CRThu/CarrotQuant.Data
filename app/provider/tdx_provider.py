"""通达信数据源驱动实现。

三种数据导入模式:
  1. zip:     下载 hsjday.zip 并解析 (仅日线有ZIP)
  2. local:   读取本地通达信安装目录的 vipdoc 文件夹
  3. online:  通过 tdxpy TCP 在线获取

支持的 table_id:
  - ashare.kline.1d.tdx   (TS) A股个股日线
  - ashare.kline.5m.tdx   (TS) A股个股5分钟线
  - ashare.kline.1m.tdx   (TS) A股个股1分钟线
  - aindex.kline.1d.tdx   (TS) 指数日线
  - aindex.kline.5m.tdx   (TS) 指数5分钟线
  - aindex.kline.1m.tdx   (TS) 指数1分钟线

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
    TDX_DAILY_URL,
    TDX_MIN5_URL,
    download_tdx_file,
    discover_tdx_symbols,
    read_tdx_file_from_zip,
    discover_tdx_symbols_from_local,
    read_tdx_file_from_local,
    tdx_code_to_standard,
    standard_to_tdx_code,
    fetch_bars_online,
    fetch_stock_list_online,
)
from app.utils.time_utils import ts_to_str


class TDXProvider(BaseProvider):
    """通达信数据源驱动，支持三种导入模式: zip / local / online。"""

    _SUPPORTED_TABLE_MAP: dict[str, str] = {
        "ashare.kline.1d.tdx": "timeseries",
        "ashare.kline.5m.tdx": "timeseries",
        "ashare.kline.1m.tdx": "timeseries",
        "aindex.kline.1d.tdx": "timeseries",
        "aindex.kline.5m.tdx": "timeseries",
        "aindex.kline.1m.tdx": "timeseries",
    }

    def __init__(
        self,
        data_dir: Optional[str] = None,
        mode: str = "zip",
        vipdoc_dir: Optional[str] = None,
    ):
        """初始化 TDX Provider。

        Args:
            data_dir: ZIP 下载目录 (zip 模式)，默认 ./tdx_data
            mode: 数据获取模式 ("zip" / "local" / "online")
            vipdoc_dir: 本地通达信 vipdoc 目录路径 (仅 local 模式)
        """
        self._data_dir = Path(data_dir) if data_dir else Path("./tdx_data")
        self._mode = mode
        self._vipdoc_dir = Path(vipdoc_dir) if vipdoc_dir else None
        self._daily_zip: Optional[Path] = None
        self._min5_zip: Optional[Path] = None

    def _ensure_daily_zip(self) -> Path:
        if self._daily_zip is None or not self._daily_zip.exists():
            self._daily_zip = download_tdx_file(TDX_DAILY_URL, str(self._data_dir))
        return self._daily_zip

    def _ensure_min5_zip(self) -> Path:
        if self._min5_zip is None or not self._min5_zip.exists():
            self._min5_zip = download_tdx_file(TDX_MIN5_URL, str(self._data_dir))
        return self._min5_zip

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

        if self._mode == "local":
            return self._get_all_symbols_local(prefix)
        elif self._mode == "online":
            return self._get_all_symbols_online(prefix)
        else:
            return self._get_all_symbols_zip(prefix)

    def _get_all_symbols_zip(self, prefix: str) -> list[str]:
        try:
            zip_path = self._ensure_daily_zip()
            if prefix == "aindex":
                raw = discover_tdx_symbols(zip_path, market="sh")
                return [tdx_code_to_standard(c) for c in raw if c[2:].startswith('000')]
            raw = discover_tdx_symbols(zip_path, market="sh") + \
                  discover_tdx_symbols(zip_path, market="sz")
            symbols = [tdx_code_to_standard(c) for c in raw if c[2:].startswith(('6', '0', '3'))]
            logger.info(f"Discovered {len(symbols)} symbols (zip)")
            return symbols
        except Exception as e:
            logger.warning(f"ZIP不可用，回退在线: {e}")
            return self._get_all_symbols_online(prefix)

    def _get_all_symbols_local(self, prefix: str) -> list[str]:
        if not self._vipdoc_dir:
            raise ValueError("local 模式需要指定 vipdoc_dir 参数")
        raw = discover_tdx_symbols_from_local(self._vipdoc_dir)
        if prefix == "aindex":
            return [tdx_code_to_standard(c) for c in raw if c[2:].startswith('000')]
        symbols = [tdx_code_to_standard(c) for c in raw if c[2:].startswith(('6', '0', '3'))]
        logger.info(f"Discovered {len(symbols)} symbols (local)")
        return symbols

    def _get_all_symbols_online(self, prefix: str) -> list[str]:
        if prefix == "aindex":
            raw = fetch_stock_list_online(market="sh")
            return [tdx_code_to_standard(c) for c in raw if c[2:].startswith('000')]
        sh = fetch_stock_list_online(market="sh")
        sz = fetch_stock_list_online(market="sz")
        symbols = [tdx_code_to_standard(c) for c in sh + sz if c[2:].startswith(('6', '0', '3'))]
        logger.info(f"Discovered {len(symbols)} symbols (online)")
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
            return self._fetch_online(symbol, freq, start_date, end_date)
        else:
            return self._fetch_offline(table_id, symbol, freq, start_date, end_date)

    def _fetch_online(
        self, symbol: str, freq: str, start_date: str, end_date: str
    ) -> pl.DataFrame:
        df = fetch_bars_online(symbol, freq=freq, start_date=start_date, end_date=end_date)
        if df.is_empty():
            return self._empty_kline_df()
        result = DataCleaner.standardize(
            df, "date", time_fmt="%Y-%m-%d",
            source_tz="Asia/Shanghai", display_tz="Asia/Shanghai",
            time_shift_hours=15,
        )
        if "volume" in result.columns and result.schema["volume"] == pl.Int64:
            result = result.with_columns(pl.col("volume").cast(pl.Float64))
        return result

    def _fetch_offline(
        self, table_id: str, symbol: str, freq: str,
        start_date: str, end_date: str
    ) -> pl.DataFrame:
        tdx_code = standard_to_tdx_code(symbol)

        if self._mode == "local":
            if not self._vipdoc_dir:
                return self._empty_kline_df()
            records = read_tdx_file_from_local(self._vipdoc_dir, tdx_code, freq=freq)
        else:  # zip
            if freq == '1d':
                zip_path = self._ensure_daily_zip()
            elif freq in ('5m', '1m'):
                zip_path = self._ensure_min5_zip()
            else:
                return self._empty_kline_df()
            records = read_tdx_file_from_zip(zip_path, tdx_code, freq=freq if freq != '1d' else '1d')

        if not records:
            return self._empty_kline_df()

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

    def _empty_kline_df(self) -> pl.DataFrame:
        return pl.DataFrame({
            "symbol": pl.Series([], dtype=pl.String),
            "datetime": pl.Series([], dtype=pl.String),
            "timestamp": pl.Series([], dtype=pl.Int64),
            "open": pl.Series([], dtype=pl.Float64),
            "high": pl.Series([], dtype=pl.Float64),
            "low": pl.Series([], dtype=pl.Float64),
            "close": pl.Series([], dtype=pl.Float64),
            "volume": pl.Series([], dtype=pl.Float64),
            "amount": pl.Series([], dtype=pl.Float64),
        })
