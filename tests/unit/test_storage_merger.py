import pytest
import polars as pl
from app.storage.data_merger import DataMerger

def test_merge_basic():
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
    
    result = DataMerger.merge(old_df, new_df, subset=["symbol", "timestamp"])
    result = DataMerger.sort(result)
    
    assert len(result) == 3
    # 验证按timestamp排序
    assert result["timestamp"].to_list() == [1704067200000, 1704153600000, 1704240000000]

def test_merge_duplicate():
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
    
    result = DataMerger.merge(old_df, new_df, subset=["symbol", "timestamp"])
    
    # 结果行数应为1（去重）
    assert len(result) == 1
    # 价格应为后抓取的数据（验证keep="last"逻辑）
    assert result["close"][0] == 99.9

def test_merge_multiple_symbols():
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
    
    result = DataMerger.merge(old_df, new_df, subset=["symbol", "timestamp"])
    
    assert len(result) == 4
    # 验证每个symbol都有2条记录
    sh_count = result.filter(pl.col("symbol") == "sh.600000").height
    sz_count = result.filter(pl.col("symbol") == "sz.000001").height
    assert sh_count == 2
    assert sz_count == 2

def test_merge_empty_old():
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
    
    result = DataMerger.merge(old_df, new_df, subset=["symbol", "timestamp"])
    
    assert len(result) == 1
    assert result["close"][0] == 10.0

def test_merge_empty_new():
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
    
    result = DataMerger.merge(old_df, new_df, subset=["symbol", "timestamp"])
    
    assert len(result) == 1
    assert result["close"][0] == 10.0

def test_merge_both_empty():
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
    
    result = DataMerger.merge(old_df, new_df, subset=["symbol", "timestamp"])
    
    assert len(result) == 0

def test_sort():
    """
    测试排序逻辑：按timestamp和symbol升序
    """
    df = pl.DataFrame({
        "symbol": ["sz.000001", "sh.600000"],
        "timestamp": [1704153600000, 1704067200000],
        "close": [20.0, 10.0]
    })
    
    result = DataMerger.sort(df)
    
    # 验证按timestamp排序
    assert result["timestamp"].to_list() == [1704067200000, 1704153600000]
    # 验证symbol排序（相同timestamp时）
    assert result["symbol"].to_list() == ["sh.600000", "sz.000001"]

def test_merge_timestamp_type_conversion():
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
    
    result = DataMerger.merge(old_df, new_df, subset=["symbol", "timestamp"])
    
    # 验证timestamp类型为Int64
    assert result["timestamp"].dtype == pl.Int64
    assert len(result) == 2

def test_merge_full_row_dedup():
    """
    测试全行去重功能（subset=None）
    """
    old_df = pl.DataFrame({
        "symbol": ["sh.600000"],
        "timestamp": [1704067200000],
        "close": [10.0]
    })
    
    # 完全相同的行
    new_df = pl.DataFrame({
        "symbol": ["sh.600000"],
        "timestamp": [1704067200000],
        "close": [10.0]
    })
    
    result = DataMerger.merge(old_df, new_df, subset=None)
    
    # 全行去重后应只剩1行
    assert len(result) == 1
    assert result["close"][0] == 10.0


def test_data_merger_dynamic_sort():
    """
    测试 DataMerger 的动态排序功能
    验证当 keys 列表中的某列缺失时，仅按存在的列排序
    """
    # 测试1：包含 symbol 列
    df_with_symbol = pl.DataFrame({
        "timestamp": [3, 1, 2],
        "symbol": ["A", "B", "A"],
        "value": [30, 10, 20]
    })
    
    sorted_df = DataMerger.sort(df_with_symbol, keys=["timestamp", "symbol"])
    
    # 验证排序正确
    assert sorted_df["timestamp"].to_list() == [1, 2, 3], "timestamp 排序错误"
    assert sorted_df["symbol"].to_list() == ["B", "A", "A"], "symbol 排序错误"
    
    # 测试2：不包含 symbol 列
    df_without_symbol = pl.DataFrame({
        "timestamp": [3, 1, 2],
        "value": [30, 10, 20]
    })
    
    sorted_df2 = DataMerger.sort(df_without_symbol, keys=["timestamp", "symbol"])
    
    # 验证排序正确（只按 timestamp 排序）
    assert sorted_df2["timestamp"].to_list() == [1, 2, 3], "timestamp 排序错误"
    assert "symbol" not in sorted_df2.columns, "不应存在 symbol 列"
    
    # 测试3：不包含任何指定的 keys 列
    df_no_keys = pl.DataFrame({
        "value": [30, 10, 20],
        "name": ["C", "A", "B"]
    })
    
    sorted_df3 = DataMerger.sort(df_no_keys, keys=["timestamp", "symbol"])
    
    # 验证返回原始数据（没有任何列可排序）
    assert sorted_df3["value"].to_list() == [30, 10, 20], "应该返回原始数据"
    assert sorted_df3["name"].to_list() == ["C", "A", "B"], "应该返回原始数据"
