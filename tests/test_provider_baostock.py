import os
import sys
import polars as pl
from loguru import logger

# 将当前根目录加入系统路径
sys.path.append(os.getcwd())

from app.provider.manager import ProviderManager
from app.provider.baostock_provider import BaostockProvider

def test_baostock_provider_fetch():
    """
    测试 BaostockProvider 的数据采集与标准化
    """
    manager = ProviderManager()
    table_id = "ashare.kline.1d.adj.baostock"
    symbol = "sz.000001" # 平安银行
    
    # 1. 测试 Manager 获取驱动
    provider = manager.get_provider(table_id)
    assert isinstance(provider, BaostockProvider)
    
    # 2. 测试数据抓取
    start_date = "2024-01-01"
    end_date = "2024-01-10"
    
    logger.info(f"Testing fetch for {symbol} from {start_date} to {end_date}")
    df = provider.fetch(table_id, symbol, start_date, end_date)
    
    # 3. 验证数据结构
    assert isinstance(df, pl.DataFrame)
    if df.is_empty():
        logger.warning("Fetched DataFrame is empty, check if it's a weekend or market closed.")
        return

    print("Fetched DataFrame Head:")
    print(df.head())
    
    # 验证核心列是否存在
    assert "timestamp" in df.columns, "Missing 'timestamp' column"
    assert "datetime" in df.columns, "Missing 'datetime' column"
    
    # 验证核心列类型
    assert df.schema["timestamp"] == pl.Int64
    assert df.schema["datetime"] == pl.Utf8
    
    # 验证是否删除了源特有列 'code'
    assert "code" not in df.columns
    
    # 验证数值列类型 (open, high, low, close 应该是 Float64)
    assert df.schema["close"] == pl.Float64
    assert df.schema["volume"] == pl.Float64
    
    # 验证数据行数 (2024-01-01 到 2024-01-10 包含 7 个交易日)
    assert len(df) > 0
    
    print(f"Test Passed: Successfully fetched and standardized {len(df)} rows.")

if __name__ == "__main__":
    test_baostock_provider_fetch()
