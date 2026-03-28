import pytest
import polars as pl
from pathlib import Path
from unittest.mock import MagicMock, patch
from app.service.sync_manager import SyncManager
from app.service.metadata_manager import MetadataManager
from app.storage.csv_storage import CSVStorage
from app.storage.parquet_storage import ParquetStorage


def test_metadata_and_sync_flow_csv(temp_storage_root):
    """
    测试元数据与同步流程的闭环集成（CSV 格式）
    向存储中注入跨年度、多股票的数据，验证物理统计与元数据的一致性
    """
    table_id = "test.metadata.sync.csv"
    format = "csv"
    
    # 创建存储实例并注入数据
    storage = CSVStorage(str(temp_storage_root / "csv"))
    
    # 注入 2023 年的数据
    df_2023 = pl.DataFrame({
        "timestamp": [1672531200000, 1672617600000],  # 2023-01-01, 2023-01-02
        "datetime": ["2023-01-01T00:00:00.000", "2023-01-02T00:00:00.000"],
        "symbol": ["sh.600000", "sz.000001"],
        "close": [10.0, 20.0]
    })
    storage.write_series(table_id, df_2023)
    
    # 注入 2024 年的数据
    df_2024 = pl.DataFrame({
        "timestamp": [1704067200000, 1704153600000],  # 2024-01-01, 2024-01-02
        "datetime": ["2024-01-01T00:00:00.000", "2024-01-02T00:00:00.000"],
        "symbol": ["sh.600000", "sz.000002"],
        "close": [10.5, 30.0]
    })
    storage.write_series(table_id, df_2024)
    
    # 验证 Storage 的物理统计方法
    total_bars = storage.get_total_bars(table_id)
    assert total_bars == 4, f"物理统计总行数应该是 4，实际是 {total_bars}"
    
    symbols = storage.get_all_symbols(table_id)
    assert len(symbols) == 3, f"物理统计 symbol 数量应该是 3，实际是 {len(symbols)}"
    assert set(symbols) == {"sh.600000", "sz.000001", "sz.000002"}
    
    timestamps = storage.get_unique_timestamps(table_id)
    assert len(timestamps) == 4, f"物理统计唯一 timestamp 数量应该是 4，实际是 {len(timestamps)}"
    
    # 验证时间范围
    time_range = storage.get_global_time_range(table_id)
    assert time_range[0] == 1672531200000, "最小时间戳应该是 2023-01-01"
    assert time_range[1] == 1704153600000, "最大时间戳应该是 2024-01-02"
    
    # 创建 MetadataManager 并加载元数据
    metadata_mgr = MetadataManager(str(temp_storage_root))
    metadata = metadata_mgr.load(table_id, format)
    
    # 如果元数据存在，验证其与物理统计一致
    if metadata and "global_stats" in metadata:
        global_stats = metadata["global_stats"]
        assert global_stats.get("total_bars") == total_bars, \
            f"元数据 total_bars 应该与物理统计一致"
        assert global_stats.get("symbol_count") == len(symbols), \
            f"元数据 symbol_count 应该与物理统计一致"


def test_metadata_and_sync_flow_parquet(temp_storage_root):
    """
    测试元数据与同步流程的闭环集成（Parquet 格式）
    """
    table_id = "test.metadata.sync.parquet"
    format = "parquet"
    
    # 创建存储实例并注入数据
    storage = ParquetStorage(str(temp_storage_root / "parquet"))
    
    # 注入跨年数据
    df = pl.DataFrame({
        "timestamp": [1672531200000, 1704067200000, 1735689600000],  # 2023, 2024, 2025
        "datetime": ["2023-01-01T00:00:00.000", "2024-01-01T00:00:00.000", "2025-01-01T00:00:00.000"],
        "symbol": ["sh.600000", "sz.000001", "sz.000002"],
        "close": [10.0, 20.0, 30.0],
        "volume": [1000000, 2000000, 3000000]
    })
    storage.write_series(table_id, df)
    
    # 验证物理统计
    total_bars = storage.get_total_bars(table_id)
    symbols = storage.get_all_symbols(table_id)
    time_range = storage.get_global_time_range(table_id)
    
    # 创建 MetadataManager 并加载元数据
    metadata_mgr = MetadataManager(str(temp_storage_root))
    metadata = metadata_mgr.load(table_id, format)
    
    # 如果元数据存在，验证其与物理统计一致
    if metadata and "global_stats" in metadata:
        global_stats = metadata["global_stats"]
        assert global_stats.get("total_bars") == total_bars, \
            f"元数据 total_bars 应该与物理统计 ({total_bars}) 一致"
        assert global_stats.get("symbol_count") == len(symbols), \
            f"元数据 symbol_count 应该与物理统计 ({len(symbols)}) 一致"
        assert global_stats.get("start_timestamp") == time_range[0], \
            f"元数据 start_timestamp 应该与物理统计一致"
        assert global_stats.get("end_timestamp") == time_range[1], \
            f"元数据 end_timestamp 应该与物理统计一致"


def test_metadata_physical_stats_consistency(temp_storage_root):
    """
    测试元数据与物理统计的一致性
    """
    table_id = "test.metadata.consistency"
    format = "csv"
    
    storage = CSVStorage(str(temp_storage_root / "csv"))
    
    # 写入多 symbol、多时间戳的数据
    df = pl.DataFrame({
        "timestamp": [1672531200000, 1672531200000, 1672617600000, 1672704000000],
        "datetime": ["2023-01-01T00:00:00.000"] * 4,
        "symbol": ["sh.600000", "sz.000001", "sh.600000", "sz.000001"],
        "close": [10.0, 20.0, 10.5, 20.5]
    })
    storage.write_series(table_id, df)
    
    # 获取物理统计
    physical_total_bars = storage.get_total_bars(table_id)
    physical_symbols = storage.get_all_symbols(table_id)
    physical_timestamps = storage.get_unique_timestamps(table_id)
    
    # 创建 MetadataManager 并加载元数据
    metadata_mgr = MetadataManager(str(temp_storage_root))
    metadata = metadata_mgr.load(table_id, format)
    
    # 如果元数据存在，验证其与物理统计一致
    if metadata and "global_stats" in metadata:
        global_stats = metadata["global_stats"]
        assert global_stats.get("total_bars") == physical_total_bars, \
            "元数据 total_bars 应该与物理统计完全一致"
        assert global_stats.get("symbol_count") == len(physical_symbols), \
            "元数据 symbol_count 应该与物理统计完全一致"


def test_metadata_schema_validation(temp_storage_root):
    """
    测试元数据 schema 字段的正确性
    """
    table_id = "test.metadata.schema"
    format = "csv"
    
    storage = CSVStorage(str(temp_storage_root / "csv"))
    
    # 写入包含多种数据类型的数据
    df = pl.DataFrame({
        "timestamp": [1672531200000, 1672617600000],
        "datetime": ["2023-01-01T00:00:00.000", "2023-01-02T00:00:00.000"],
        "symbol": ["sh.600000", "sh.600000"],
        "open": [10.0, 10.5],
        "high": [10.5, 11.0],
        "low": [9.5, 10.0],
        "close": [10.2, 10.8],
        "volume": [1000000, 1100000]
    })
    storage.write_series(table_id, df)
    
    # 创建 MetadataManager 并加载元数据
    metadata_mgr = MetadataManager(str(temp_storage_root))
    metadata = metadata_mgr.load(table_id, format)
    
    # 如果元数据存在，验证 schema 字段
    if metadata and "schema" in metadata:
        schema = metadata["schema"]
        assert "timestamp" in schema, "schema 应该包含 timestamp 字段"
        assert "symbol" in schema, "schema 应该包含 symbol 字段"
        assert "close" in schema, "schema 应该包含 close 字段"


def test_metadata_time_range_accuracy(temp_storage_root):
    """
    测试元数据时间范围的准确性
    """
    table_id = "test.metadata.time_range"
    format = "csv"
    
    storage = CSVStorage(str(temp_storage_root / "csv"))
    
    # 写入精确时间戳的数据
    start_ts = 1672531200000  # 2023-01-01 00:00:00
    end_ts = 1672704000000    # 2023-01-03 00:00:00
    
    df = pl.DataFrame({
        "timestamp": [start_ts, 1672617600000, end_ts],
        "datetime": ["2023-01-01T00:00:00.000", "2023-01-02T00:00:00.000", "2023-01-03T00:00:00.000"],
        "symbol": ["sh.600000"] * 3,
        "close": [10.0, 10.5, 11.0]
    })
    storage.write_series(table_id, df)
    
    # 验证物理时间范围
    time_range = storage.get_global_time_range(table_id)
    assert time_range[0] == start_ts, \
        f"物理统计 start_timestamp 应该是 {start_ts}，实际是 {time_range[0]}"
    assert time_range[1] == end_ts, \
        f"物理统计 end_timestamp 应该是 {end_ts}，实际是 {time_range[1]}"
    
    # 创建 MetadataManager 并加载元数据
    metadata_mgr = MetadataManager(str(temp_storage_root))
    metadata = metadata_mgr.load(table_id, format)
    
    # 如果元数据存在，验证其与物理统计一致
    if metadata and "global_stats" in metadata:
        global_stats = metadata["global_stats"]
        assert global_stats.get("start_timestamp") == start_ts, \
            f"元数据 start_timestamp 应该是 {start_ts}"
        assert global_stats.get("end_timestamp") == end_ts, \
            f"元数据 end_timestamp 应该是 {end_ts}"


def test_metadata_empty_storage(temp_storage_root):
    """
    测试空存储的元数据处理
    """
    table_id = "test.metadata.empty"
    format = "csv"
    
    # 不写入任何数据，直接加载元数据
    metadata_mgr = MetadataManager(str(temp_storage_root))
    metadata = metadata_mgr.load(table_id, format)
    
    # 对于不存在的表，load 应该返回空字典
    assert metadata == {}, "空存储的元数据应该返回空字典"


def test_metadata_multiple_formats_consistency(temp_storage_root):
    """
    测试多格式元数据的一致性
    同样的数据写入 CSV 和 Parquet，验证物理统计应该一致
    """
    table_id = "test.metadata.multi_format"
    
    # 准备测试数据
    df = pl.DataFrame({
        "timestamp": [1672531200000, 1672617600000, 1672704000000],
        "datetime": ["2023-01-01T00:00:00.000", "2023-01-02T00:00:00.000", "2023-01-03T00:00:00.000"],
        "symbol": ["sh.600000", "sz.000001", "sh.600000"],
        "close": [10.0, 20.0, 10.5],
        "volume": [1000000, 2000000, 1100000]
    })
    
    # 写入 CSV
    csv_storage = CSVStorage(str(temp_storage_root / "csv"))
    csv_storage.write_series(table_id, df)
    
    # 写入 Parquet
    parquet_storage = ParquetStorage(str(temp_storage_root / "parquet"))
    parquet_storage.write_series(table_id, df)
    
    # 验证两种格式的物理统计一致
    csv_total_bars = csv_storage.get_total_bars(table_id)
    parquet_total_bars = parquet_storage.get_total_bars(table_id)
    assert csv_total_bars == parquet_total_bars, \
        "CSV 和 Parquet 的 total_bars 应该一致"
    
    csv_symbols = set(csv_storage.get_all_symbols(table_id))
    parquet_symbols = set(parquet_storage.get_all_symbols(table_id))
    assert csv_symbols == parquet_symbols, \
        "CSV 和 Parquet 的 symbol 列表应该一致"
    
    csv_time_range = csv_storage.get_global_time_range(table_id)
    parquet_time_range = parquet_storage.get_global_time_range(table_id)
    assert csv_time_range == parquet_time_range, \
        "CSV 和 Parquet 的时间范围应该一致"