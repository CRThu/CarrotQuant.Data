import pytest
import polars as pl
from loguru import logger
from app.provider.provider_manager import ProviderManager

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
        "preclose", "volume", "amount", "adjust_flag", "turnover_rate", 
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

def test_baostock_provider_adj_factor():
    """
    测试 BaostockProvider 的复权因子数据采集与标准化，验证 Event 数据逻辑
    """
    manager = ProviderManager()
    symbol = "sz.000001"
    
    table_id = "ashare.adj_factor.baostock"
    provider = manager.get_provider(table_id)
    
    # 测试复权因子数据
    df = provider.fetch(table_id, symbol, "2024-01-01", "2024-12-31")
    
    logger.info("Adj Factor Check:")
    
    # 验证返回数据非空
    assert not df.is_empty(), "Adj factor data should not be empty"
    
    # 验证核心列存在
    assert "symbol" in df.columns
    assert "timestamp" in df.columns
    assert "datetime" in df.columns
    
    # 验证复权因子列存在且类型为 Float64
    assert "back_adjust_factor" in df.columns, "back_adjust_factor should exist"
    assert df.schema["back_adjust_factor"] == pl.Float64, "back_adjust_factor should be Float64"
    
    # 验证 fore_adjust_factor 和 adjust_factor 也存在
    assert "fore_adjust_factor" in df.columns
    assert "adjust_factor" in df.columns
    assert df.schema["fore_adjust_factor"] == pl.Float64
    assert df.schema["adjust_factor"] == pl.Float64
    
    # 验证 datetime 列格式正确（应为 YYYY-MM-DDT00:00:00.000+08:00）
    # Event 数据的 timestamp 是 dividOperateDate 的 00:00:00 UTC+8 对应的毫秒戳
    # 经过 DataCleaner.standardize 处理后，datetime 应该显示原始日期的 00:00:00+08:00
    if not df.is_empty():
        first_dt = df["datetime"][0]
        logger.info(f"First datetime = {first_dt}")
        # 验证 datetime 格式正确：包含 T00:00:00 和 +08:00
        assert "T00:00:00" in first_dt, "datetime should contain 00:00:00 time"
        assert "+08:00" in first_dt, "datetime should have +08:00 timezone suffix"
    
    # 验证列顺序：symbol, datetime, timestamp 应该在最前面
    front_cols = df.columns[:3]
    assert "symbol" == front_cols[0]
    assert "datetime" == front_cols[1]
    assert "timestamp" == front_cols[2]
    
    print(f"Adj Factor Total Fields ({len(df.columns)}): {df.columns}")
    print(df.head(3))
    
    print("\nTest Passed: Adj factor (Event) fetch and standardization verified.")

if __name__ == "__main__":
    test_baostock_provider_fetch()
    test_baostock_provider_adj_factor()
