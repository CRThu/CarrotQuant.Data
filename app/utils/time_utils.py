from datetime import datetime, timezone, timedelta
from typing import Any

def ts_to_str(ts_ms: int, fmt: str = "%Y-%m-%d") -> str:
    """
    通用毫秒戳转字符串工具。
    支持自定义 format。默认使用 UTC+8。
    """
    if not ts_ms:
        return ""
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone(timedelta(hours=8)))
        # 针对毫秒的处理：若 format 包含 %f，截断到 3 位
        result = dt.strftime(fmt)
        if ".%f" in fmt:
            return result[:-3]
        return result
    except (ValueError, OSError, OverflowError):
        return ""

def ts_to_iso(ts_ms: int) -> str:
    """将毫秒戳转换为 ISO8601 字符串 (YYYY-MM-DDTHH:MM:SS.mmm)"""
    return ts_to_str(ts_ms, fmt="%Y-%m-%dT%H:%M:%S.%f")

def parse_date_to_ts(date_val: Any) -> int:
    """
    通用日期解析：支持 yyyy-mm-dd 字符串或毫秒级整数。
    统一返回 UTC+8 毫秒戳。
    """
    if isinstance(date_val, int):
        return date_val
    if isinstance(date_val, str):
        # 假定 yyyy-mm-dd
        dt = datetime.strptime(date_val, "%Y-%m-%d").replace(tzinfo=timezone(timedelta(hours=8)))
        return int(dt.timestamp() * 1000)
    raise ValueError(f"Unsupported date format: {date_val}")
