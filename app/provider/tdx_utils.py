"""通达信数据下载与解析工具。

三种数据导入模式:
  1. zip:     下载 hsjday.zip / hsmin5.zip 并解析
  2. local:   读取本地通达信安装目录的 vipdoc 文件夹
  3. online:  通过 tdxpy TCP 在线获取 (日线全历史, 5m~2年, 1m~5月)
"""

import os
import struct
import zipfile
from pathlib import Path
from typing import Optional
from urllib.request import urlretrieve

import polars as pl
from loguru import logger
from tdxpy.hq import TdxHq_API

# TDX 数据源 URL
TDX_DAILY_URL = "https://data.tdx.com.cn/vipdoc/hsjday.zip"
TDX_MIN1_URL = "https://data.tdx.com.cn/vipdoc/hsmin1.zip"
TDX_MIN5_URL = "https://data.tdx.com.cn/vipdoc/hsmin5.zip"

# TDX 二进制记录大小
_DAY_RECORD_SIZE = 32
_MIN1_RECORD_SIZE = 32
_MIN5_RECORD_SIZE = 32

# mootdx category 映射
_CATEGORY_MAP = {
    "1d": 4,   # 日线
    "5m": 0,   # 5分钟线
    "1m": 7,   # 1分钟线
    "15m": 1,  # 15分钟线
    "30m": 2,  # 30分钟线
    "60m": 3,  # 60分钟线
}

# mootdx 市场映射
_MARKET_MAP = {
    "sh": 1,
    "sz": 0,
}

# 单次最大获取量
_MAX_BARS_PER_REQUEST = 800


def download_tdx_file(url: str, dest_dir: str) -> Path:
    """下载 TDX 数据文件到指定目录。

    Args:
        url: 下载地址
        dest_dir: 目标目录

    Returns:
        下载文件的路径
    """
    dest_path = Path(dest_dir)
    dest_path.mkdir(parents=True, exist_ok=True)
    filename = url.split("/")[-1]
    file_path = dest_path / filename

    if file_path.exists():
        logger.info(f"TDX 文件已存在: {file_path}")
        return file_path

    logger.info(f"正在下载 TDX 数据: {url}")
    urlretrieve(url, str(file_path))
    logger.info(f"下载完成: {file_path}")
    return file_path


def parse_tdx_day_data(data: bytes) -> list[dict]:
    """解析通达信日线二进制数据。

    TDX 日线格式 (32字节/条):
    - 4 bytes: 日期 (YYYYMMDD, little-endian uint32)
    - 4 bytes: 开盘价 * 100 (little-endian uint32)
    - 4 bytes: 最高价 * 100 (little-endian uint32)
    - 4 bytes: 最低价 * 100 (little-endian uint32)
    - 4 bytes: 收盘价 * 100 (little-endian uint32)
    - 4 bytes: 成交额 (little-endian float32)
    - 4 bytes: 成交量 (little-endian uint32)
    - 4 bytes: 保留 (little-endian uint32)

    Args:
        data: 二进制数据

    Returns:
        解析后的记录列表
    """
    records = []
    num_records = len(data) // _DAY_RECORD_SIZE

    for i in range(num_records):
        offset = i * _DAY_RECORD_SIZE
        record = data[offset:offset + _DAY_RECORD_SIZE]

        date_int = struct.unpack('<I', record[0:4])[0]
        year = date_int // 10000
        month = (date_int % 10000) // 100
        day = date_int % 100

        open_price = struct.unpack('<I', record[4:8])[0] / 100.0
        high_price = struct.unpack('<I', record[8:12])[0] / 100.0
        low_price = struct.unpack('<I', record[12:16])[0] / 100.0
        close_price = struct.unpack('<I', record[16:20])[0] / 100.0
        amount = struct.unpack('<f', record[20:24])[0]
        volume = struct.unpack('<I', record[24:28])[0]

        records.append({
            'date': f"{year:04d}-{month:02d}-{day:02d}",
            'open': round(open_price, 4),
            'high': round(high_price, 4),
            'low': round(low_price, 4),
            'close': round(close_price, 4),
            'volume': volume,
            'amount': round(amount, 2),
        })

    return records


def parse_tdx_min_data(data: bytes, freq: int = 1) -> list[dict]:
    """解析通达信分钟线二进制数据。

    TDX 分钟线格式 (32字节/条):
    - 4 bytes: 日期 (YYYYMMDD, little-endian uint32)
    - 4 bytes: 时间 (HHMMSS, little-endian uint32)
    - 4 bytes: 开盘价 * 100 (little-endian uint32)
    - 4 bytes: 最高价 * 100 (little-endian uint32)
    - 4 bytes: 最低价 * 100 (little-endian uint32)
    - 4 bytes: 收盘价 * 100 (little-endian uint32)
    - 4 bytes: 成交额 (little-endian float32)
    - 4 bytes: 成交量 (little-endian uint32)

    Args:
        data: 二进制数据
        freq: 频率 (1=1分钟, 5=5分钟)

    Returns:
        解析后的记录列表
    """
    records = []
    num_records = len(data) // _MIN1_RECORD_SIZE

    for i in range(num_records):
        offset = i * _MIN1_RECORD_SIZE
        record = data[offset:offset + _MIN1_RECORD_SIZE]

        date_int = struct.unpack('<I', record[0:4])[0]
        time_int = struct.unpack('<I', record[4:8])[0]

        year = date_int // 10000
        month = (date_int % 10000) // 100
        day = date_int % 100

        hour = time_int // 10000
        minute = (time_int % 10000) // 100
        second = time_int % 100

        open_price = struct.unpack('<I', record[8:12])[0] / 100.0
        high_price = struct.unpack('<I', record[12:16])[0] / 100.0
        low_price = struct.unpack('<I', record[16:20])[0] / 100.0
        close_price = struct.unpack('<I', record[20:24])[0] / 100.0
        amount = struct.unpack('<f', record[24:28])[0]
        volume = struct.unpack('<I', record[28:32])[0]

        records.append({
            'datetime': f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}",
            'date': f"{year:04d}-{month:02d}-{day:02d}",
            'time': f"{hour:02d}:{minute:02d}:{second:02d}",
            'open': round(open_price, 4),
            'high': round(high_price, 4),
            'low': round(low_price, 4),
            'close': round(close_price, 4),
            'volume': volume,
            'amount': round(amount, 2),
        })

    return records


def discover_tdx_symbols(zip_path: Path, market: str = "all") -> list[str]:
    """从 ZIP 文件中发现所有证券代码。

    Args:
        zip_path: ZIP 文件路径
        market: 市场过滤 ("sh", "sz", "bj", "all")

    Returns:
        证券代码列表 (格式: sh600000)
    """
    symbols = set()

    with zipfile.ZipFile(zip_path, 'r') as zf:
        for name in zf.namelist():
            if not name.endswith('.day'):
                continue

            # 解析文件名: sh/lday/sh600000.day -> sh600000
            parts = name.split('/')
            if len(parts) >= 2:
                filename = parts[-1]
                code = filename.replace('.day', '')

                # 市场过滤
                if market != "all":
                    if not code.startswith(market):
                        continue

                symbols.add(code)

    return sorted(symbols)


def read_tdx_file_from_zip(zip_path: Path, tdx_code: str, freq: str = "1d") -> list[dict]:
    """从 ZIP 文件中读取指定证券的数据。

    Args:
        zip_path: ZIP 文件路径
        tdx_code: 通达信格式代码 (如 sh600000)
        freq: 频率 ("1d", "1m", "5m")

    Returns:
        解析后的记录列表
    """
    # 构建 ZIP 内路径
    if freq == "1d":
        zip_internal = f"{tdx_code[:2]}/lday/{tdx_code}.day"
    elif freq == "1m":
        zip_internal = f"{tdx_code[:2]}/min1/{tdx_code}.min1"
    elif freq == "5m":
        zip_internal = f"{tdx_code[:2]}/min5/{tdx_code}.min5"
    else:
        raise ValueError(f"不支持的频率: {freq}")

    with zipfile.ZipFile(zip_path, 'r') as zf:
        if zip_internal not in zf.namelist():
            logger.warning(f"文件不存在: {zip_internal}")
            return []

        data = zf.read(zip_internal)

    if freq == "1d":
        return parse_tdx_day_data(data)
    else:
        freq_num = int(freq.replace('m', ''))
        return parse_tdx_min_data(data, freq=freq_num)


def tdx_code_to_standard(tdx_code: str) -> str:
    """将通达信格式代码转换为标准格式。

    Args:
        tdx_code: 通达信格式代码 (如 sh600000)

    Returns:
        标准格式代码 (如 sh.600000)
    """
    if len(tdx_code) >= 2:
        prefix = tdx_code[:2]
        code = tdx_code[2:]
        return f"{prefix}.{code}"
    return tdx_code


def standard_to_tdx_code(standard_code: str) -> str:
    """将标准格式代码转换为通达信格式。

    Args:
        standard_code: 标准格式代码 (如 sh.600000)

    Returns:
        通达信格式代码 (如 sh600000)
    """
    return standard_code.replace('.', '')


# -----------------------------------------------------------------------
# 本地通达信 vipdoc 目录读取
# -----------------------------------------------------------------------

# 本地 vipdoc 目录结构:
#   vipdoc/
#     sh/lday/sh600000.day    # 上证日线
#     sh/lday/sh000001.day    # 上证指数日线
#     sz/lday/sz000001.day    # 深证日线
#     sh/min5/sh600000.day    # 上证5分钟线
#     sz/min5/sz000001.day    # 深证5分钟线
#     sh/min1/sh600000.day    # 上证1分钟线

_FREQ_TO_SUBDIR = {
    "1d": "lday",
    "5m": "min5",
    "1m": "min1",
    "15m": "min15",
    "30m": "min30",
    "60m": "min60",
}

_FREQ_TO_EXT = {
    "1d": ".day",
    "5m": ".day",
    "1m": ".day",
    "15m": ".day",
    "30m": ".day",
    "60m": ".day",
}


def discover_tdx_symbols_from_local(vipdoc_dir: Path, market: str = "all") -> list[str]:
    """从本地通达信 vipdoc 目录发现所有证券代码。

    Args:
        vipdoc_dir: vipdoc 目录路径 (如 C:/new_tdx/vipdoc)
        market: 市场过滤 ("sh", "sz", "bj", "all")

    Returns:
        证券代码列表 (格式: sh600000)
    """
    symbols = set()
    lday_dir = vipdoc_dir / "lday"

    if not lday_dir.exists():
        # 尝试 sh/lday 和 sz/lday 结构
        for market_dir in ["sh", "sz", "bj"]:
            mlday = vipdoc_dir / market_dir / "lday"
            if mlday.exists():
                for f in mlday.iterdir():
                    if f.suffix == ".day":
                        code = f.stem  # sh600000
                        if market != "all" and not code.startswith(market):
                            continue
                        symbols.add(code)
    else:
        # 平铺结构: lday/sh600000.day
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
    """从本地通达信 vipdoc 目录读取指定证券的数据。

    Args:
        vipdoc_dir: vipdoc 目录路径
        tdx_code: 通达信格式代码 (如 sh600000)
        freq: 频率 ("1d", "5m", "1m")

    Returns:
        解析后的记录列表
    """
    subdir = _FREQ_TO_SUBDIR.get(freq)
    if not subdir:
        raise ValueError(f"不支持的频率: {freq}")

    # 构建文件路径: vipdoc/sh/lday/sh600000.day
    market = tdx_code[:2]
    file_path = vipdoc_dir / market / subdir / f"{tdx_code}.day"

    if not file_path.exists():
        logger.warning(f"文件不存在: {file_path}")
        return []

    data = file_path.read_bytes()

    if freq == "1d":
        return parse_tdx_day_data(data)
    else:
        freq_num = int(freq.replace('m', ''))
        return parse_tdx_min_data(data, freq=freq_num)


# -----------------------------------------------------------------------
# 在线下载 (mootdx TCP)
# -----------------------------------------------------------------------

def fetch_bars_online(
    symbol: str,
    freq: str = "1d",
    start_date: str = None,
    end_date: str = None,
) -> pl.DataFrame:
    """通过 TDX TCP 在线获取 K 线数据 (支持增量偏移)。

    支持的频率: 1m, 5m, 1d
    日线可通过 offset 无限回溯到 IPO 日期。

    Args:
        symbol: 标准格式代码 (如 sh.600000)
        freq: 频率 ("1d", "5m", "1m")
        start_date: 起始日期 (YYYY-MM-DD)，None 则获取全部
        end_date: 结束日期 (YYYY-MM-DD)，None 则到最新

    Returns:
        标准化后的 Polars DataFrame
    """
    tdx_code = standard_to_tdx_code(symbol)
    market_str = tdx_code[:2]
    market = _MARKET_MAP.get(market_str)
    if market is None:
        raise ValueError(f"未知市场前缀: {market_str}")

    category = _CATEGORY_MAP.get(freq)
    if category is None:
        raise ValueError(f"不支持的频率: {freq}")

    # 指数代码 (000xxx) 强制使用上海市场
    pure_code = tdx_code[2:]
    is_index = pure_code.startswith("000")
    fetch_market = 1 if is_index else market

    api = TdxHq_API()
    # 连接服务器
    connected = False
    for ip, port in [("218.75.126.9", 7709), ("119.147.212.81", 7709),
                      ("115.238.56.198", 7709), ("124.160.88.183", 7709)]:
        try:
            if api.connect(ip, port):
                connected = True
                break
        except Exception:
            continue

    if not connected:
        logger.warning("TDX 服务器连接失败")
        return _empty_online_df()

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

            # 转换为标准记录
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

            # 检查是否已到起始日期
            if start_date and len(data) > 0:
                earliest = data[0]["datetime"][:10]
                if earliest <= start_date:
                    break

            # 不足一批说明已到头
            if len(data) < _MAX_BARS_PER_REQUEST:
                break

            offset += _MAX_BARS_PER_REQUEST

    finally:
        api.disconnect()

    if not all_records:
        return _empty_online_df()

    df = pl.DataFrame(all_records, schema={
        "date": pl.String,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume": pl.Int64,
        "amount": pl.Float64,
    })

    # 添加 symbol 列
    df = df.with_columns(pl.lit(symbol).alias("symbol"))

    # 日期过滤
    if start_date:
        df = df.filter(pl.col("date") >= start_date)
    if end_date:
        df = df.filter(pl.col("date") <= end_date)

    return df


def fetch_stock_list_online(market: str = "sh") -> list[str]:
    """通过 TDX TCP 获取股票列表。

    Args:
        market: 市场 ("sh" 或 "sz")

    Returns:
        通达信格式代码列表 (如 ["sh600000", "sh600001", ...])
    """
    market_code = _MARKET_MAP.get(market)
    if market_code is None:
        raise ValueError(f"未知市场: {market}")

    api = TdxHq_API()
    connected = False
    for ip, port in [("218.75.126.9", 7709), ("119.147.212.81", 7709),
                      ("115.238.56.198", 7709), ("124.160.88.183", 7709)]:
        try:
            if api.connect(ip, port):
                connected = True
                break
        except Exception:
            continue

    if not connected:
        logger.warning("TDX 服务器连接失败")
        return []

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


def _empty_online_df() -> pl.DataFrame:
    """返回空的在线数据 DataFrame。"""
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
