import os
import sys
import polars as pl
from loguru import logger

# 将当前根目录加入系统路径
sys.path.append(os.getcwd())

from app.provider.manager import ProviderManager

def test_baostock_provider_fetch():
    """
    测试 BaostockProvider 的数据采集与标准化，验证重命名与清理逻辑
    """
    manager = ProviderManager()
    symbol = "sz.000001"
    
    # --- 测试 1: 日线数据 ---
    table_id_day = "ashare.kline.1d.raw.baostock"
    provider = manager.get_provider(table_id_day)
    df_day = provider.fetch(table_id_day, symbol, "2024-01-01", "2024-01-05")
    
    logger.info("Daily Kline Check:")
    assert not df_day.is_empty()
    assert "symbol" in df_day.columns
    assert "pe_ttm" in df_day.columns, "peTTM should be renamed to pe_ttm"
    
    # 验证列顺序：symbol, datetime, timestamp 应该在最前面
    front_cols = df_day.columns[:3]
    assert "symbol" == front_cols[0]
    assert "datetime" == front_cols[1]
    assert "timestamp" == front_cols[2]
    
    # 打印并校验日线所有字段
    expected_day_fields = [
        "symbol", "datetime", "timestamp", "open", "high", "low", "close", 
        "preclose", "volume", "amount", "adjustflag", "turnover_rate", 
        "trade_status", "change_pct", "pe_ttm", "pb_mrq", "ps_ttm", "pcf_ncf_ttm", "is_st"
    ]
    for field in expected_day_fields:
        assert field in df_day.columns, f"Daily kline missing field: {field}"
        
    print(f"Daily Total Fields ({len(df_day.columns)}): {df_day.columns}")
    print(df_day.head(2))

    # --- 测试 2: 5分钟线数据 ---
    table_id_min = "ashare.kline.5m.raw.baostock"
    df_min = provider.fetch(table_id_min, symbol, "2024-01-05", "2024-01-05")
    
    logger.info("5min Kline Check:")
    assert not df_min.is_empty()
    assert "symbol" in df_min.columns
    
    # 验证列顺序
    front_cols_min = df_min.columns[:3]
    assert "symbol" == front_cols_min[0]
    assert "datetime" == front_cols_min[1]
    assert "timestamp" == front_cols_min[2]
    
    # 验证 datetime 是否包含毫秒/时间部分 (YYYY-MM-DDTHH:MM:SS.sss)
    assert "T" in df_min["datetime"][0]
    logger.info(f"5min fetch test passed. Rows: {len(df_min)}")
    print(df_min.head())
    print("Test Passed: Successfully fetched and standardized both daily and 5min data.")
    
    # 验证冗余列删除
    assert "date" not in df_min.columns, "Redundant 'date' column should be removed for minute data"
    assert "time" not in df_min.columns, "Original 'time' column should be removed after standardization"
    
    print(f"5min Fields: {df_min.columns}")
    print(df_min.head(2))
    
    print("\nTest Passed: All renaming and cleaning logic verified.")

if __name__ == "__main__":
    test_baostock_provider_fetch()
