import pytest
from datetime import datetime, timezone, timedelta
from app.utils.time_utils import parse_date_to_ts, ts_to_iso, ts_to_str

def test_parse_date_to_ts_utc8():
    """
    测试 parse_date_to_ts 是否准确生成 UTC+8 的毫秒戳
    """
    # 测试 2024-01-01 的解析
    ts = parse_date_to_ts("2024-01-01")
    
    # 验证返回的是整数毫秒戳
    assert isinstance(ts, int)
    
    # 验证时区正确：UTC+8 的 2024-01-01 00:00:00
    expected_dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=8)))
    expected_ts = int(expected_dt.timestamp() * 1000)
    
    assert ts == expected_ts, f"预期 {expected_ts}，实际 {ts}"
    
    # 验证转换回日期时间是否正确
    actual_dt = datetime.fromtimestamp(ts / 1000, tz=timezone(timedelta(hours=8)))
    assert actual_dt.year == 2024
    assert actual_dt.month == 1
    assert actual_dt.day == 1
    assert actual_dt.hour == 0
    assert actual_dt.minute == 0
    assert actual_dt.second == 0

def test_parse_date_to_ts_integer_passthrough():
    """
    测试整数输入直接返回（透传）
    """
    test_ts = 1704067200000  # 2024-01-01 00:00:00 UTC+8
    result = parse_date_to_ts(test_ts)
    assert result == test_ts

def test_parse_date_to_ts_invalid_format():
    """
    测试无效格式抛出 ValueError
    """
    with pytest.raises(ValueError):
        parse_date_to_ts("2024/01/01")  # 错误的格式
    
    with pytest.raises(ValueError):
        parse_date_to_ts("invalid-date")

def test_ts_to_iso_format():
    """
    测试 ts_to_iso 的格式是否符合 YYYY-MM-DDTHH:MM:SS.mmm
    """
    # 1704067200000 毫秒戳 = UTC 2024-01-01 00:00:00 = UTC+8 2024-01-01 08:00:00
    ts = 1704067200000
    iso_str = ts_to_iso(ts)
    
    # 验证格式（UTC+8 时区）
    assert iso_str == "2024-01-01T08:00:00.000", f"预期 '2024-01-01T08:00:00.000'，实际 '{iso_str}'"
    
    # 验证包含 T 分隔符
    assert "T" in iso_str
    # 验证包含毫秒部分
    assert "." in iso_str
    # 验证毫秒部分为3位
    assert len(iso_str.split(".")[-1]) == 3

def test_ts_to_iso_with_milliseconds():
    """
    测试带毫秒的时间戳转换
    """
    # 2024-01-01 12:30:45.123 UTC+8
    dt = datetime(2024, 1, 1, 12, 30, 45, 123000, tzinfo=timezone(timedelta(hours=8)))
    ts = int(dt.timestamp() * 1000)
    
    iso_str = ts_to_iso(ts)
    assert iso_str == "2024-01-01T12:30:45.123"

def test_ts_to_str_custom_format():
    """
    测试 ts_to_str 自定义格式
    """
    ts = 1704067200000  # 2024-01-01 00:00:00 UTC+8
    
    # 测试日期格式
    date_str = ts_to_str(ts, fmt="%Y-%m-%d")
    assert date_str == "2024-01-01"
    
    # 测试日期时间格式（UTC+8 时区）
    datetime_str = ts_to_str(ts, fmt="%Y-%m-%d %H:%M:%S")
    assert datetime_str == "2024-01-01 08:00:00"
    
    # 测试中文格式
    cn_str = ts_to_str(ts, fmt="%Y年%m月%d日")
    assert cn_str == "2024年01月01日"

def test_ts_to_str_empty_input():
    """
    测试空输入返回空字符串
    """
    assert ts_to_str(0) == ""
    assert ts_to_str(None) == ""