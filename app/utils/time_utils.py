from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

def ts_to_str(ts_ms: int, fmt: str = "%Y-%m-%d", display_tz: str = "Asia/Shanghai") -> str:
    """
    通用毫秒戳转字符串工具。
    支持自定义 format 和动态时区。
    
    Args:
        ts_ms: UTC 0 毫秒戳
        fmt: 时间格式字符串
        display_tz: 目标时区名称 (IANA 格式，默认 Asia/Shanghai)
    """
    if not ts_ms:
        return ""
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=ZoneInfo(display_tz))
        # 针对毫秒的处理：若 format 包含 %f，截断到 3 位
        result = dt.strftime(fmt)
        if ".%f" in fmt:
            return result[:-3]
        return result
    except (ValueError, OSError, OverflowError):
        return ""

def ts_to_iso(ts_ms: int, display_tz: str = "Asia/Shanghai") -> str:
    """
    将 UTC 0 毫秒戳转换为 ISO8601 字符串，包含时区偏移。
    
    Args:
        ts_ms: UTC 0 毫秒戳
        display_tz: 目标时区名称 (IANA 格式，默认 Asia/Shanghai)
        
    Returns:
        ISO8601 字符串，如 "2024-01-01T08:00:00.000+08:00"
    """
    if not ts_ms:
        return ""
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=ZoneInfo(display_tz))
        # 生成带时区偏移的 ISO 字符串
        iso_str = dt.isoformat(timespec='milliseconds')
        return iso_str
    except (ValueError, OSError, OverflowError):
        return ""

def parse_date_to_ts(date_val: Any, source_tz: str = "Asia/Shanghai") -> int:
    """
    通用日期解析：支持 yyyy-mm-dd 字符串或毫秒级整数。
    将输入日期视为指定时区的挂钟时间，返回对应的 UTC 0 毫秒戳。
    
    Args:
        date_val: 日期值 (yyyy-mm-dd 字符串或毫秒级整数)
        source_tz: 输入日期的时区名称 (IANA 格式，默认 Asia/Shanghai)
        
    Returns:
        UTC 0 毫秒戳
    """
    if isinstance(date_val, int):
        return date_val
    if isinstance(date_val, str):
        # 假定 yyyy-mm-dd，视为指定时区的挂钟时间
        dt = datetime.strptime(date_val, "%Y-%m-%d").replace(tzinfo=ZoneInfo(source_tz))
        return int(dt.timestamp() * 1000)
    raise ValueError(f"Unsupported date format: {date_val}")