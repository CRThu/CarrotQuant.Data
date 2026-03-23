import pytest
import polars as pl
import shutil
from pathlib import Path
from app.storage.parquet_storage import ParquetStorage

@pytest.fixture
def temp_storage_root(tmp_path):
    root = tmp_path / "storage_parquet"
    root.mkdir()
    yield str(root)
    if root.exists():
        shutil.rmtree(root)

def test_parquet_storage_write_read(temp_storage_root):
    storage = ParquetStorage(storage_root=temp_storage_root)
    table_id = "test.parquet.table"
    
    # 模拟数据 1: 2023-01
    df1 = pl.DataFrame({
        "symbol": ["000001.SH", "000001.SH"],
        "timestamp": [1673740800000, 1673827200000], # 2023-01-15, 2023-01-16
        "close": [10.0, 11.0]
    })
    
    # 模拟数据 2: 2023-02
    df2 = pl.DataFrame({
        "symbol": ["000001.SH"],
        "timestamp": [1676419200000], # 2023-02-15
        "close": [12.0]
    })
    
    # 写入
    storage.write(table_id, df1)
    storage.write(table_id, df2)
    
    # 验证文件路径
    path1 = Path(temp_storage_root) / table_id / "year=2023" / "2023-01.parquet"
    path2 = Path(temp_storage_root) / table_id / "year=2023" / "2023-02.parquet"
    assert path1.exists()
    assert path2.exists()
    
    # 读取验证
    read_df = storage.read(table_id, "000001.SH", 2023)
    assert len(read_df) == 3
    assert read_df["close"].to_list() == [10.0, 11.0, 12.0]
    assert read_df["timestamp"].is_sorted()

def test_parquet_storage_deduplication(temp_storage_root):
    storage = ParquetStorage(storage_root=temp_storage_root)
    table_id = "test.parquet.dedup"
    
    df1 = pl.DataFrame({
        "symbol": ["000001.SH"],
        "timestamp": [1672531200000],
        "close": [10.0]
    })
    
    # 相同 symbol 和 timestamp，价格更新
    df2 = pl.DataFrame({
        "symbol": ["000001.SH"],
        "timestamp": [1672531200000],
        "close": [10.5]
    })
    
    storage.write(table_id, df1)
    storage.write(table_id, df2)
    
    read_df = storage.read(table_id, "000001.SH", 2023)
    assert len(read_df) == 1
    assert read_df["close"][0] == 10.5

def test_parquet_storage_sorting(temp_storage_root):
    storage = ParquetStorage(storage_root=temp_storage_root)
    table_id = "test.parquet.sort"
    
    df = pl.DataFrame({
        "symbol": ["000002.SH", "000001.SH"],
        "timestamp": [1672617600000, 1672531200000],
        "close": [20.0, 10.0]
    })
    
    storage.write(table_id, df)
    
    # 获取全量 symbol
    symbols = storage.get_all_symbols(table_id)
    assert symbols == ["000001.SH", "000002.SH"]
    
    # 直接读取文件验证物理排序
    path = Path(temp_storage_root) / table_id / "year=2023" / "2023-01.parquet"
    p_df = pl.read_parquet(path)
    # 按 symbol, timestamp 排序
    assert p_df["symbol"].to_list() == ["000001.SH", "000002.SH"]
    assert p_df["timestamp"].to_list() == [1672531200000, 1672617600000]
