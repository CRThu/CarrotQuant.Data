import pytest
import polars as pl
from pathlib import Path
from app.storage.parquet_storage import ParquetStorage


def test_parquet_storage_write_read(temp_storage_root):
    """
    测试 Parquet 的 Hive 分区写入（按年/月分区）
    """
    storage = ParquetStorage(str(temp_storage_root / "parquet"))
    table_id = "test.parquet.write_read"
    
    # 写入 2023 年 1 月的数据
    df = pl.DataFrame({
        "timestamp": [1672531200000, 1672617600000],  # 2023-01-01, 2023-01-02
        "datetime": ["2023-01-01T00:00:00.000", "2023-01-02T00:00:00.000"],
        "symbol": ["sh.600000"] * 2,
        "open": [10.0, 10.5],
        "high": [10.5, 11.0],
        "low": [9.5, 10.0],
        "close": [10.2, 10.8],
        "volume": [1000000, 1100000]
    })
    
    storage.write(table_id, df)
    
    # 验证文件系统中是否生成了 year=2023/2023-01.parquet 这种结构的路径
    year_dir = temp_storage_root / "parquet" / table_id / "year=2023"
    assert year_dir.exists(), "2023 年目录应该存在"
    
    parquet_file = year_dir / "2023-01.parquet"
    assert parquet_file.exists(), "2023-01.parquet 文件应该存在"
    
    # 验证读取回的数据顺序和长度
    read_df = storage.read(table_id, "sh.600000", 2023)
    assert len(read_df) == 2, "应该读取到 2 条记录"
    
    # 验证数据按 timestamp 升序排列
    timestamps = read_df["timestamp"].to_list()
    assert timestamps == sorted(timestamps), "数据应该按 timestamp 升序排列"
    
    # 验证数据内容
    assert read_df["symbol"].to_list() == ["sh.600000", "sh.600000"]
    assert read_df["close"].to_list() == [10.2, 10.8]


def test_parquet_storage_deduplication(temp_storage_root):
    """
    测试 Parquet 存储层的幂等/去重能力
    写入两条具有相同 symbol 和 timestamp 但数值不同的数据，验证读取时应仅保留最新的一条
    """
    storage = ParquetStorage(str(temp_storage_root / "parquet"))
    table_id = "test.parquet.dedup"
    
    # 第一次写入
    df1 = pl.DataFrame({
        "timestamp": [1672531200000, 1672617600000],
        "datetime": ["2023-01-01T00:00:00.000", "2023-01-02T00:00:00.000"],
        "symbol": ["sh.600000"] * 2,
        "close": [10.0, 10.5]
    })
    storage.write(table_id, df1)
    
    # 第二次写入，包含相同 timestamp 但不同数值的数据
    df2 = pl.DataFrame({
        "timestamp": [1672617600000, 1672704000000],  # 2023-01-02 重复，2023-01-03 新增
        "datetime": ["2023-01-02T00:00:00.000", "2023-01-03T00:00:00.000"],
        "symbol": ["sh.600000"] * 2,
        "close": [99.9, 11.0]  # 01-02 的值被修改
    })
    storage.write(table_id, df2)
    
    # 读取数据
    read_df = storage.read(table_id, "sh.600000", 2023)
    
    # 验证去重逻辑：应该保留 3 条记录（01-01, 01-02, 01-03）
    assert len(read_df) == 3, "去重后应该有 3 条记录"
    
    # 验证 01-02 的值是最后一次写入的值（99.9）
    row_0102 = read_df.filter(pl.col("timestamp") == 1672617600000)
    assert row_0102["close"][0] == 99.9, "应该保留最后一次写入的数据"


def test_parquet_storage_sorting(temp_storage_root):
    """
    测试 Parquet 存储层的排序能力
    写入乱序的股票数据，验证物理磁盘上的 Parquet 文件内部是否已按照 symbol 和 timestamp 升序排列
    """
    storage = ParquetStorage(str(temp_storage_root / "parquet"))
    table_id = "test.parquet.sorting"
    
    # 写入乱序数据
    df = pl.DataFrame({
        "timestamp": [1672704000000, 1672531200000, 1672617600000, 1672531200000],
        "datetime": ["2023-01-03T00:00:00.000", "2023-01-01T00:00:00.000", 
                     "2023-01-02T00:00:00.000", "2023-01-01T00:00:00.000"],
        "symbol": ["sz.000001", "sh.600000", "sh.600000", "sz.000001"],
        "close": [20.0, 10.0, 10.5, 19.5]
    })
    
    storage.write(table_id, df)
    
    # 验证逻辑层面的排序（通过 get_all_symbols）
    symbols = storage.get_all_symbols(table_id)
    assert symbols == sorted(symbols), "symbol 列表应该按字母顺序排列"
    
    # 验证物理存储的排序（直接读取 Parquet 文件）
    parquet_file = temp_storage_root / "parquet" / table_id / "year=2023" / "2023-01.parquet"
    assert parquet_file.exists(), "Parquet 文件应该存在"
    
    # 直接读取物理文件验证排序
    physical_df = pl.read_parquet(parquet_file)
    
    # 验证 symbol 列是有序的
    physical_symbols = physical_df["symbol"].to_list()
    for i in range(len(physical_symbols) - 1):
        assert physical_symbols[i] <= physical_symbols[i + 1], \
            f"物理文件中 symbol 应该有序: {physical_symbols[i]} > {physical_symbols[i+1]}"
    
    # 验证相同 symbol 内 timestamp 是有序的
    for symbol in symbols:
        symbol_df = physical_df.filter(pl.col("symbol") == symbol)
        timestamps = symbol_df["timestamp"].to_list()
        assert timestamps == sorted(timestamps), \
            f"Symbol {symbol} 的 timestamp 应该按升序排列"


def test_parquet_storage_cross_year(temp_storage_root):
    """
    测试 Parquet 存储层的跨年分区能力
    """
    storage = ParquetStorage(str(temp_storage_root / "parquet"))
    table_id = "test.parquet.cross_year"
    
    # 写入跨年数据
    df = pl.DataFrame({
        "timestamp": [1735689599000, 1735689600000],  # 2024-12-31 23:59:59 和 2025-01-01 00:00:00
        "datetime": ["2024-12-31T23:59:59.000", "2025-01-01T00:00:00.000"],
        "symbol": ["sh.600000"] * 2,
        "close": [10.5, 10.6]
    })
    
    storage.write(table_id, df)
    
    # 验证 2024 年目录和文件
    year_2024_dir = temp_storage_root / "parquet" / table_id / "year=2024"
    assert year_2024_dir.exists(), "2024 年目录应该存在"
    assert (year_2024_dir / "2024-12.parquet").exists(), "2024-12.parquet 文件应该存在"
    
    # 验证 2025 年目录和文件
    year_2025_dir = temp_storage_root / "parquet" / table_id / "year=2025"
    assert year_2025_dir.exists(), "2025 年目录应该存在"
    assert (year_2025_dir / "2025-01.parquet").exists(), "2025-01.parquet 文件应该存在"


def test_parquet_storage_multiple_symbols(temp_storage_root):
    """
    测试 Parquet 存储层的多 symbol 支持
    """
    storage = ParquetStorage(str(temp_storage_root / "parquet"))
    table_id = "test.parquet.multi_symbols"
    
    # 写入多个 symbol 的数据
    df = pl.DataFrame({
        "timestamp": [1672531200000] * 3,
        "datetime": ["2023-01-01T00:00:00.000"] * 3,
        "symbol": ["sh.600000", "sz.000001", "sz.000002"],
        "close": [10.0, 20.0, 30.0]
    })
    
    storage.write(table_id, df)
    
    # 验证所有 symbol 都能正确读取
    symbols = storage.get_all_symbols(table_id)
    assert set(symbols) == {"sh.600000", "sz.000001", "sz.000002"}
    
    # 验证每个 symbol 的数据量
    for symbol in symbols:
        symbol_df = storage.read(table_id, symbol, 2023)
        assert len(symbol_df) == 1, f"Symbol {symbol} 应该有 1 条记录"
        assert symbol_df["symbol"][0] == symbol


def test_parquet_storage_empty_write(temp_storage_root):
    """
    测试 Parquet 存储层的空数据写入拦截
    """
    storage = ParquetStorage(str(temp_storage_root / "parquet"))
    table_id = "test.parquet.empty"
    
    # 写入空 DataFrame
    empty_df = pl.DataFrame()
    storage.write(table_id, empty_df)
    
    # 验证不应该创建表目录
    table_dir = temp_storage_root / "parquet" / table_id
    assert not table_dir.exists(), "空数据不应该创建表目录"


def test_parquet_storage_metadata_stats(temp_storage_root):
    """
    测试 Parquet 存储层的统计接口
    """
    storage = ParquetStorage(str(temp_storage_root / "parquet"))
    table_id = "test.parquet.stats"
    
    # 写入跨年、多 symbol 的数据
    df = pl.DataFrame({
        "timestamp": [1672531200000, 1672617600000, 1735689600000],
        "datetime": ["2023-01-01T00:00:00.000", "2023-01-02T00:00:00.000", "2025-01-01T00:00:00.000"],
        "symbol": ["sh.600000", "sh.600000", "sz.000001"],
        "close": [10.0, 10.5, 20.0]
    })
    
    storage.write(table_id, df)
    
    # 验证统计方法
    total_bars = storage.get_total_bars(table_id)
    assert total_bars == 3, "总行数应该是 3"
    
    symbols = storage.get_all_symbols(table_id)
    assert len(symbols) == 2, "symbol 数量应该是 2"
    
    timestamps = storage.get_unique_timestamps(table_id)
    assert len(timestamps) == 3, "唯一的 timestamp 数量应该是 3"
    
    time_range = storage.get_global_time_range(table_id)
    assert time_range[0] == 1672531200000, "最小时间戳应该是 2023-01-01"
    assert time_range[1] == 1735689600000, "最大时间戳应该是 2025-01-01"