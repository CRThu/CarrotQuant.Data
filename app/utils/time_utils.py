from datetime import datetime, timezone, timedelta

def ts_to_iso(ts_ms: int) -> str:
    """将毫秒戳转换为 ISO8601 字符串 (UTC+8 示例)"""
    if not ts_ms:
        return ""
    # 假设由于项目针对 A 股，默认使用 UTC+8
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone(timedelta(hours=8)))
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    except (ValueError, OSError, OverflowError):
        return ""
