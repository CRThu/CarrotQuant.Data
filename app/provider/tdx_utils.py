"""通达信数据工具。

两种数据获取模式:
  1. local:  读取本地通达信 vipdoc 目录 (由 download_tdx.py 脚本下载解压)
  2. online: 通过 tdxpy TCP 在线获取 (日线全历史, 5m~2年, 1m~5月)
"""

from pathlib import Path

import polars as pl
from loguru import logger
from tdxpy.hq import TdxHq_API
from tdxpy.reader import TdxDailyBarReader, TdxLCMinBarReader

# tdxpy category 映射
_CATEGORY_MAP = {
    "1d": 4,
    "5m": 0,
    "1m": 7,
    "15m": 1,
    "30m": 2,
    "60m": 3,
}

# tdxpy 市场映射
_MARKET_MAP = {
    "sh": 1,
    "sz": 0,
    "bj": 2,
}

# 单次最大获取量
_MAX_BARS_PER_REQUEST = 800

# TDX 服务器列表
_TDX_SERVERS = [
    ("218.75.126.9", 7709),
    ("119.147.212.81", 7709),
    ("115.238.56.198", 7709),
    ("124.160.88.183", 7709),
]

# vipdoc 频率子目录映射
_FREQ_TO_SUBDIR = {
    "1d": "lday",
    "5m": "minline",
    "1m": "minline",
    "15m": "minline",
    "30m": "minline",
    "60m": "minline",
}

# vipdoc 分钟线文件后缀映射
_FREQ_TO_EXT = {
    "1d": ".day",
    "5m": ".lc5",
    "1m": ".lc1",
    "15m": ".lc1",
    "30m": ".lc1",
    "60m": ".lc1",
}

# tdxpy reader 实例 (全局单例)
_daily_reader = TdxDailyBarReader()
_lc_min_reader = TdxLCMinBarReader()


def _connect_tdx_api() -> TdxHq_API:
    """连接 TDX 服务器，失败时抛出 RuntimeError。"""
    api = TdxHq_API()
    for ip, port in _TDX_SERVERS:
        try:
            if api.connect(ip, port):
                return api
        except Exception:
            continue
    raise RuntimeError("TDX 服务器连接失败，请检查网络或稍后重试")


# -----------------------------------------------------------------------
# 代码格式转换
# -----------------------------------------------------------------------

def tdx_code_to_standard(tdx_code: str) -> str:
    """通达信格式 → 标准格式 (sh600000 → sh.600000)。"""
    if len(tdx_code) >= 2:
        return f"{tdx_code[:2]}.{tdx_code[2:]}"
    return tdx_code


def standard_to_tdx_code(standard_code: str) -> str:
    """标准格式 → 通达信格式 (sh.600000 → sh600000)。"""
    return standard_code.replace('.', '')


# -----------------------------------------------------------------------
# 本地 vipdoc 目录读取
# -----------------------------------------------------------------------

def discover_tdx_symbols_from_local(vipdoc_dir: Path, market: str = "all") -> list[str]:
    """从本地 vipdoc lday 目录发现所有证券代码。"""
    symbols = set()
    lday_dir = vipdoc_dir / "lday"

    if not lday_dir.exists():
        for market_dir in ["sh", "sz", "bj"]:
            mlday = vipdoc_dir / market_dir / "lday"
            if mlday.exists():
                for f in mlday.iterdir():
                    if f.suffix == ".day":
                        code = f.stem
                        if market != "all" and not code.startswith(market):
                            continue
                        symbols.add(code)
    else:
        for f in lday_dir.iterdir():
            if f.suffix == ".day":
                code = f.stem
                if market != "all" and not code.startswith(market):
                    continue
                symbols.add(code)

    return sorted(symbols)


def read_tdx_file_from_local(
    vipdoc_dir: Path, tdx_code: str, freq: str = "1d"
) -> list[dict]:
    """从本地 vipdoc 目录读取指定证券的数据。

    使用 tdxpy reader 解析二进制文件，返回标准记录列表。
    """
    subdir = _FREQ_TO_SUBDIR.get(freq)
    ext = _FREQ_TO_EXT.get(freq)
    if not subdir or not ext:
        raise ValueError(f"不支持的频率: {freq}")

    market = tdx_code[:2]
    file_path = vipdoc_dir / market / subdir / f"{tdx_code}{ext}"

    if not file_path.exists():
        logger.warning(f"文件不存在: {file_path}")
        return []

    try:
        if freq == "1d":
            pdf = _daily_reader.get_df(str(file_path))
        else:
            pdf = _lc_min_reader.get_df(str(file_path))

        if pdf.empty:
            return []

        records = []
        for idx, row in pdf.iterrows():
            if freq == "1d":
                dt_str = idx.strftime("%Y-%m-%d")
                records.append({
                    "date": dt_str,
                    "open": round(float(row["open"]), 4),
                    "high": round(float(row["high"]), 4),
                    "low": round(float(row["low"]), 4),
                    "close": round(float(row["close"]), 4),
                    "volume": float(row["volume"]),
                    "amount": round(float(row["amount"]), 2),
                })
            else:
                dt_str = idx.strftime("%Y-%m-%d %H:%M:%S")
                date_part = idx.strftime("%Y-%m-%d")
                records.append({
                    "datetime": dt_str,
                    "date": date_part,
                    "time": idx.strftime("%H:%M:%S"),
                    "open": round(float(row["open"]), 4),
                    "high": round(float(row["high"]), 4),
                    "low": round(float(row["low"]), 4),
                    "close": round(float(row["close"]), 4),
                    "volume": float(row["volume"]),
                    "amount": round(float(row["amount"]), 2),
                })

        return records

    except Exception as e:
        logger.warning(f"解析失败: {file_path}, {e}")
        return []


# -----------------------------------------------------------------------
# 在线获取 (tdxpy TCP)
# -----------------------------------------------------------------------

def fetch_bars_online(
    symbol: str,
    freq: str = "1d",
    start_date: str = None,
    end_date: str = None,
    table_id: str = "",
) -> pl.DataFrame:
    """通过 TDX TCP 在线获取 K 线数据 (支持 offset 回溯)。"""
    tdx_code = standard_to_tdx_code(symbol)
    pure_code = tdx_code[2:]
    market_str = tdx_code[:2]
    market = _MARKET_MAP.get(market_str)
    if market is None:
        raise ValueError(f"未知市场前缀: {market_str}")

    category = _CATEGORY_MAP.get(freq)
    if category is None:
        raise ValueError(f"不支持的频率: {freq}")

    is_index = table_id.startswith("aindex") or pure_code.startswith(("000", "399"))
    fetch_market = 1 if pure_code.startswith("000") else market

    api = _connect_tdx_api()
    try:
        all_records = []
        offset = 0

        while True:
            if is_index:
                data = api.get_index_bars(category, fetch_market, pure_code, offset, _MAX_BARS_PER_REQUEST)
            else:
                data = api.get_security_bars(category, fetch_market, pure_code, offset, _MAX_BARS_PER_REQUEST)

            if not data:
                break

            for row in data:
                dt_str = row["datetime"]
                date_part = dt_str[:10]
                all_records.append({
                    "date": date_part,
                    "open": round(float(row["open"]), 4),
                    "high": round(float(row["high"]), 4),
                    "low": round(float(row["low"]), 4),
                    "close": round(float(row["close"]), 4),
                    "volume": int(row["vol"]),
                    "amount": round(float(row["amount"]), 2),
                })

            if start_date and len(data) > 0:
                earliest = data[0]["datetime"][:10]
                if earliest <= start_date:
                    break

            if len(data) < _MAX_BARS_PER_REQUEST:
                break

            offset += _MAX_BARS_PER_REQUEST

    finally:
        api.disconnect()

    if not all_records:
        return _empty_kline_df()

    df = pl.DataFrame(all_records, schema={
        "date": pl.String,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume": pl.Int64,
        "amount": pl.Float64,
    })

    df = df.with_columns(pl.lit(symbol).alias("symbol"))

    if start_date:
        df = df.filter(pl.col("date") >= start_date)
    if end_date:
        df = df.filter(pl.col("date") <= end_date)

    return df


def fetch_stock_list_online(market: str = "sh") -> list[str]:
    """通过 TDX TCP 获取股票列表。"""
    market_code = _MARKET_MAP.get(market)
    if market_code is None:
        raise ValueError(f"未知市场: {market}")

    api = _connect_tdx_api()
    try:
        all_codes = []
        offset = 0
        while True:
            stocks = api.get_security_list(market_code, offset)
            if not stocks:
                break
            for row in stocks:
                code = str(row["code"]).strip()
                all_codes.append(f"{market}{code}")
            if len(stocks) < 1000:
                break
            offset += 1000
    finally:
        api.disconnect()

    return sorted(set(all_codes))


def _empty_kline_df() -> pl.DataFrame:
    """返回标准 schema 的空 K 线 DataFrame。"""
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
