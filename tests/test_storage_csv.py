import os
import sys
from pathlib import Path
import polars as pl
import shutil

# 将当前根目录加入系统路径
sys.path.append(os.getcwd())

from app.storage.csv_storage import CSVStorage

def test_csv_storage_timestamp_merge():
    test_root = "test_storage_root"
    if os.path.exists(test_root):
        shutil.rmtree(test_root)
    
    # 场景 1: 标准 1d K线存储 (直接使用默认构造)
    storage = CSVStorage(storage_root=f"{test_root}/csv")
    table_id = "ashare.kline.1d.baostock"
    symbol = "sh.600000"
    
    # 模拟已标准化的数据 (datetime + timestamp)
    df1 = pl.DataFrame({
        "datetime": ["2024-01-01T00:00:00.000", "2024-01-02T00:00:00.000"],
        "timestamp": [1704067200000, 1704153600000],
        "symbol": [symbol] * 2,
        "close": [10.0, 10.1]
    })
    
    print("--- 第一次写入 ---")
    storage.append(table_id, df1)
    
    # 场景 2: 包含重复 timestamp 的增量数据
    df2 = pl.DataFrame({
        "datetime": ["2024-01-02T00:00:00.000", "2024-01-03T00:00:00.000"],
        "timestamp": [1704153600000, 1704240000000],
        "symbol": [symbol] * 2,
        "close": [99.9, 10.2] # 修改 01-02 的值测试 keep="last"
    })
    
    print("--- 第二次增量 (基于 timestamp 去重) ---")
    storage.append(table_id, df2)
    
    # 验证
    read_df = storage.read(table_id, symbol, 2024)
    print(f"合并后数据量: {len(read_df)}")
    
    assert len(read_df) == 3, "数据去重失败"
    assert read_df.filter(pl.col("timestamp") == 1704153600000)["close"][0] == 99.9, "未保留最新的增量数据"
    
    # 场景 3: 跨年数据写入测试
    df_cross_year = pl.DataFrame({
        "datetime": ["2024-12-31T23:59:59.000", "2025-01-01T00:00:00.000"],
        "timestamp": [1735689599000, 1735689600000],
        "symbol": [symbol] * 2,
        "close": [10.5, 10.6]
    })
    
    print("--- 写入跨年数据 (2024 + 2025) ---")
    storage.append(table_id, df_cross_year)
    
    # 验证 2025 年的目录和文件是否存在
    path_2025 = Path(test_root) / "csv" / table_id / "year=2025" / f"{symbol}.csv"
    assert path_2025.exists(), "跨年分区失败：2025 年目录未生成"
    
    # 验证 2024 年的数据是否增量合并成功
    read_2024 = storage.read(table_id, symbol, 2024)
    assert 1735689599000 in read_2024["timestamp"].to_list(), "2024 年跨年数据合并丢失"

    print("测试通过！跨年 Hive 分区验证成功。")

def test_csv_storage_metadata_stats():
    test_root = "test_metadata_root"
    if os.path.exists(test_root):
        shutil.rmtree(test_root)
        
    storage = CSVStorage(storage_root=f"{test_root}/csv")
    table_id = "test.metadata.stats"
    
    # 写入 symbol A (2024, 2025)
    df_a = pl.DataFrame({
        "timestamp": [1704067200000, 1735689600000], 
        "datetime": ["2024-01-01T00:00:00.000", "2025-01-01T00:00:00.000"],
        "symbol": ["sh.600000"] * 2,
        "close": [10.0, 11.0]
    })
    # 写入 symbol B (2024)
    df_b = pl.DataFrame({
        "timestamp": [1704153600000],
        "datetime": ["2024-01-02T00:00:00.000"],
        "symbol": ["sz.000001"],
        "close": [20.0]
    })
    
    storage.append(table_id, df_a)
    storage.append(table_id, df_b)
    
    print("--- 验证 Storage 元数据统计接口 ---")
    symbols = storage.get_all_symbols(table_id)
    assert len(symbols) == 2
    assert "sh.600000" in symbols
    assert "sz.000001" in symbols
    
    total_bars = storage.get_total_bars(table_id)
    assert total_bars == 3
    
    t_min, t_max = storage.get_global_time_range(table_id)
    assert t_min == 1704067200000
    assert t_max == 1735689600000
    
    unique_ts = storage.get_unique_timestamps(table_id)
    assert len(unique_ts) == 3
    
    print("测试通过！Storage 统计方法验证成功。")

if __name__ == "__main__":
    test_csv_storage_timestamp_merge()
    test_csv_storage_metadata_stats()
