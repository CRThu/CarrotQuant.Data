import pytest
import polars as pl
from app.storage.data_merger import DataMerger

def test_merge_by_symbol_timestamp_basic():
    """
    测试基本合并功能：两个无重复数据的DataFrame合并
    """
    old_df = pl.DataFrame({
        "symbol": ["sh.600000", "sh.600000"],
        "timestamp": [1704067200000, 1704153600000],  # 2024-01-01, 2024-01-02
        "close": [10.0, 10.1]
    })
    
    new_df = pl.DataFrame({
        "symbol": ["sh.600000"],
        "timestamp": [1704240000000],  # 2024-01-03
        "close": [10.2]
    })
    
    result = DataMerger.merge_by_symbol_timestamp(old_df, new_df)
    
    assert len(result) == 3
    # 验证按timestamp排序
    assert result["timestamp"].to_list() == [1704067200000, 1704153600000, 1704240000000]

def test_merge_by_symbol_timestamp_duplicate():
    """
    测试重复timestamp去重逻辑：验证keep="last"
    """
    # 构造两个包含相同timestamp但close价格不同的DataFrame
    old_df = pl.DataFrame({
        "symbol": ["sh.600000"],
        "timestamp": [1704153600000],  # 2024-01-02
        "close": [10.0]  # 旧价格
    })
    
    new_df = pl.DataFrame({
        "symbol": ["sh.600000"],
        "timestamp": [1704153600000],  # 相同timestamp
        "close": [99.9]  # 新价格
    })
    
    result = DataMerger.merge_by_symbol_timestamp(old_df, new_df)
    
    # 结果行数应为1（去重）
    assert len(result) == 1
    # 价格应为后抓取的数据（验证keep="last"逻辑）
    assert result["close"][0] == 99.9

def test_merge_by_symbol_timestamp_multiple_symbols():
    """
    测试多symbol数据合并
    """
    old_df = pl.DataFrame({
        "symbol": ["sh.600000", "sz.000001"],
        "timestamp": [1704067200000, 1704067200000],
        "close": [10.0, 20.0]
    })
    
    new_df = pl.DataFrame({
        "symbol": ["sh.600000", "sz.000001"],
        "timestamp": [1704153600000, 1704153600000],
        "close": [10.1, 20.1]
    })
    
    result = DataMerger.merge_by_symbol_timestamp(old_df, new_df)
    
    assert len(result) == 4
    # 验证每个symbol都有2条记录
    sh_count = result.filter(pl.col("symbol") == "sh.600000").height
    sz_count = result.filter(pl.col("symbol") == "sz.000001").height
    assert sh_count == 2
    assert sz_count == 2

def test_merge_by_symbol_timestamp_empty_old():
    """
    测试old_df为空的情况
    """
    old_df = pl.DataFrame({
        "symbol": [],
        "timestamp": [],
        "close": []
    })
    
    new_df = pl.DataFrame({
        "symbol": ["sh.600000"],
        "timestamp": [1704067200000],
        "close": [10.0]
    })
    
    result = DataMerger.merge_by_symbol_timestamp(old_df, new_df)
    
    assert len(result) == 1
    assert result["close"][0] == 10.0

def test_merge_by_symbol_timestamp_empty_new():
    """
    测试new_df为空的情况
    """
    old_df = pl.DataFrame({
        "symbol": ["sh.600000"],
        "timestamp": [1704067200000],
        "close": [10.0]
    })
    
    new_df = pl.DataFrame({
        "symbol": [],
        "timestamp": [],
        "close": []
    })
    
    result = DataMerger.merge_by_symbol_timestamp(old_df, new_df)
    
    assert len(result) == 1
    assert result["close"][0] == 10.0

def test_merge_by_symbol_timestamp_both_empty():
    """
    测试两个DataFrame都为空的情况
    """
    old_df = pl.DataFrame({
        "symbol": [],
        "timestamp": [],
        "close": []
    })
    
    new_df = pl.DataFrame({
        "symbol": [],
        "timestamp": [],
        "close": []
    })
    
    result = DataMerger.merge_by_symbol_timestamp(old_df, new_df)
    
    assert len(result) == 0

def test_merge_by_symbol_timestamp_sorting():
    """
    测试合并后的排序逻辑：按symbol和timestamp升序
    """
    old_df = pl.DataFrame({
        "symbol": ["sz.000001", "sh.600000"],
        "timestamp": [1704153600000, 1704067200000],  # 时间戳顺序相反
        "close": [20.0, 10.0]
    })
    
    new_df = pl.DataFrame({
        "symbol": ["sh.600000", "sz.000001"],
        "timestamp": [1704240000000, 1704240000000],
        "close": [10.2, 20.2]
    })
    
    result = DataMerger.merge_by_symbol_timestamp(old_df, new_df)
    
    # 验证按symbol排序（sh.600000在前）
    symbols = result["symbol"].to_list()
    assert symbols[0] == "sh.600000"
    assert symbols[1] == "sh.600000"
    assert symbols[2] == "sz.000001"
    assert symbols[3] == "sz.000001"
    
    # 验证每个symbol内按timestamp排序
    sh_timestamps = result.filter(pl.col("symbol") == "sh.600000")["timestamp"].to_list()
    assert sh_timestamps == [1704067200000, 1704240000000]
    
    sz_timestamps = result.filter(pl.col("symbol") == "sz.000001")["timestamp"].to_list()
    assert sz_timestamps == [1704153600000, 1704240000000]

def test_merge_by_symbol_timestamp_timestamp_type_conversion():
    """
    测试timestamp类型转换：确保统一转换为Int64
    """
    # 创建timestamp为Int64的DataFrame（避免溢出）
    old_df = pl.DataFrame({
        "symbol": ["sh.600000"],
        "timestamp": [1704067200000],
        "close": [10.0]
    })
    
    new_df = pl.DataFrame({
        "symbol": ["sh.600000"],
        "timestamp": [1704153600000],
        "close": [10.1]
    })
    
    result = DataMerger.merge_by_symbol_timestamp(old_df, new_df)
    
    # 验证timestamp类型为Int64
    assert result["timestamp"].dtype == pl.Int64
    assert len(result) == 2
