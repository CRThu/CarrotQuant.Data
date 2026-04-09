from pathlib import Path
import polars as pl
from app.storage.csv_storage import CSVStorage
from app.service.metadata_manager import MetadataManager


def _stamp_metadata(storage, table_id, df, category="TS"):
    """辅助函数：为测试生成元数据"""
    meta_mgr = MetadataManager(storage.storage_root.parent)
    meta_mgr.save(table_id, "csv", {
        "table_id": table_id, "category": category, "format": "csv",
        "schema": {k: str(v) for k, v in df.schema.items()}
    })


def test_csv_storage_timestamp_merge(temp_storage_root):
    """
    测试 CSV 存储层的增量覆盖逻辑（Keep Last）
    当写入的数据中包含已有时间戳但数值不同时，旧数据应被新数据覆盖
    """
    storage = CSVStorage(str(temp_storage_root / "csv"))
    table_id = "test.csv.timestamp_merge"
    
    # 第一次写入
    df1 = pl.DataFrame({
        "timestamp": [1672531200000, 1672617600000],  # 2023-01-01, 2023-01-02
        "datetime": ["2023-01-01T00:00:00.000", "2023-01-02T00:00:00.000"],
        "symbol": ["sh.600000"] * 2,
        "close": [10.0, 10.5]
    })
    storage.write_series(table_id, df1)
    _stamp_metadata(storage, table_id, df1)
    
    # 第二次写入，包含相同时间戳但不同数值的数据
    df2 = pl.DataFrame({
        "timestamp": [1672617600000, 1672704000000],  # 2023-01-02 重复，2023-01-03 新增
        "datetime": ["2023-01-02T00:00:00.000", "2023-01-03T00:00:00.000"],
        "symbol": ["sh.600000"] * 2,
        "close": [99.9, 11.0]  # 01-02 的值被修改
    })
    storage.write_series(table_id, df2)
    
    # 读取数据验证覆盖逻辑
    read_df = storage.read_series(table_id, "sh.600000", 2023)
    
    # 验证去重后有 3 条记录
    assert len(read_df) == 3, "去重后应该有 3 条记录"
    
    # 验证 01-02 的值是最后一次写入的值（99.9）
    row_0102 = read_df.filter(pl.col("timestamp") == 1672617600000)
    assert row_0102["close"][0] == 99.9, "应该保留最后一次写入的数据（Keep Last）"


def test_csv_storage_cross_year_partition(temp_storage_root):
    """
    测试 CSV 存储层的跨年分区写入
    验证是否正确根据 Hive 分区规则（year=YYYY）分拆到不同的文件夹
    """
    storage = CSVStorage(str(temp_storage_root / "csv"))
    table_id = "test.csv.cross_year"
    
    # 写入跨年数据
    df = pl.DataFrame({
        "timestamp": [1735689599000, 1735689600000],  # 2024-12-31 23:59:59 和 2025-01-01 00:00:00
        "datetime": ["2024-12-31T23:59:59.000", "2025-01-01T00:00:00.000"],
        "symbol": ["sh.600000"] * 2,
        "close": [10.5, 10.6]
    })
    
    storage.write_series(table_id, df)
    _stamp_metadata(storage, table_id, df)
    
    # 验证 2024 年目录和文件
    year_2024_dir = temp_storage_root / "csv" / table_id / "year=2024"
    assert year_2024_dir.exists(), "2024 年目录应该存在"
    assert (year_2024_dir / "sh.600000.csv").exists(), "2024 年的 sh.600000.csv 文件应该存在"
    
    # 验证 2025 年目录和文件
    year_2025_dir = temp_storage_root / "csv" / table_id / "year=2025"
    assert year_2025_dir.exists(), "2025 年目录应该存在"
    assert (year_2025_dir / "sh.600000.csv").exists(), "2025 年的 sh.600000.csv 文件应该存在"


def test_csv_storage_metadata_stats(temp_storage_root):
    """
    测试 CSV 存储引擎的统计接口
    验证在多分区文件夹（2024, 2025）存在的情况下，能正确汇总计算全局的股票总数和总行数
    """
    storage = CSVStorage(str(temp_storage_root / "csv"))
    table_id = "test.csv.metadata_stats"
    
    # 写入 2024 年的数据
    df_2024 = pl.DataFrame({
        "timestamp": [1704067200000, 1704153600000],  # 2024-01-01, 2024-01-02
        "datetime": ["2024-01-01T00:00:00.000", "2024-01-02T00:00:00.000"],
        "symbol": ["sh.600000", "sz.000001"],
        "close": [10.0, 20.0]
    })
    storage.write_series(table_id, df_2024)
    
    # 写入 2025 年的数据
    df_2025 = pl.DataFrame({
        "timestamp": [1735689600000, 1735776000000],  # 2025-01-01, 2025-01-02
        "datetime": ["2025-01-01T00:00:00.000", "2025-01-02T00:00:00.000"],
        "symbol": ["sh.600000", "sz.000002"],
        "close": [10.5, 30.0]
    })
    storage.write_series(table_id, df_2025)
    
    # 验证统计方法
    total_bars = storage.get_total_bars(table_id)
    assert total_bars == 4, "总行数应该是 4（2024年2条 + 2025年2条）"
    
    symbols = storage.get_all_symbols(table_id)
    assert len(symbols) == 3, "symbol 数量应该是 3（sh.600000, sz.000001, sz.000002）"
    assert set(symbols) == {"sh.600000", "sz.000001", "sz.000002"}
    
    timestamps = storage.get_unique_timestamps(table_id)
    assert len(timestamps) == 4, "唯一的 timestamp 数量应该是 4"
    
    time_range = storage.get_global_time_range(table_id)
    assert time_range[0] == 1704067200000, "最小时间戳应该是 2024-01-01"
    assert time_range[1] == 1735776000000, "最大时间戳应该是 2025-01-02"


def test_csv_storage_write_read(temp_storage_root):
    """
    测试 CSV 存储层的基本写入和读取功能
    """
    storage = CSVStorage(str(temp_storage_root / "csv"))
    table_id = "test.csv.write_read"
    
    # 写入数据
    df = pl.DataFrame({
        "timestamp": [1672531200000, 1672617600000, 1672704000000],
        "datetime": ["2023-01-01T00:00:00.000", "2023-01-02T00:00:00.000", "2023-01-03T00:00:00.000"],
        "symbol": ["sh.600000"] * 3,
        "open": [10.0, 10.5, 11.0],
        "high": [10.5, 11.0, 11.5],
        "low": [9.5, 10.0, 10.5],
        "close": [10.2, 10.8, 11.2],
        "volume": [1000000, 1100000, 1200000]
    })
    
    storage.write_series(table_id, df)
    _stamp_metadata(storage, table_id, df)
    
    # 验证文件系统中是否生成了 year=2023/sh.600000.csv 这种结构的路径
    year_dir = temp_storage_root / "csv" / table_id / "year=2023"
    assert year_dir.exists(), "2023 年目录应该存在"
    
    csv_file = year_dir / "sh.600000.csv"
    assert csv_file.exists(), "sh.600000.csv 文件应该存在"
    
    # 验证读取回的数据
    read_df = storage.read_series(table_id, "sh.600000", 2023)
    assert len(read_df) == 3, "应该读取到 3 条记录"
    
    # 验证数据按 timestamp 升序排列
    timestamps = read_df["timestamp"].to_list()
    assert timestamps == sorted(timestamps), "数据应该按 timestamp 升序排列"


def test_csv_storage_multiple_symbols(temp_storage_root):
    """
    测试 CSV 存储层的多 symbol 支持
    """
    storage = CSVStorage(str(temp_storage_root / "csv"))
    table_id = "test.csv.multi_symbols"
    
    # 写入多个 symbol 的数据
    df = pl.DataFrame({
        "timestamp": [1672531200000] * 3,
        "datetime": ["2023-01-01T00:00:00.000"] * 3,
        "symbol": ["sh.600000", "sz.000001", "sz.000002"],
        "close": [10.0, 20.0, 30.0]
    })
    
    storage.write_series(table_id, df)
    _stamp_metadata(storage, table_id, df)
    
    # 验证所有 symbol 都能正确读取
    symbols = storage.get_all_symbols(table_id)
    assert set(symbols) == {"sh.600000", "sz.000001", "sz.000002"}
    
    # 验证每个 symbol 的数据量
    for symbol in symbols:
        symbol_df = storage.read_series(table_id, symbol, 2023)
        assert len(symbol_df) == 1, f"Symbol {symbol} 应该有 1 条记录"
        assert symbol_df["symbol"][0] == symbol


def test_csv_storage_empty_write(temp_storage_root):
    """
    测试 CSV 存储层的空数据写入拦截
    """
    storage = CSVStorage(str(temp_storage_root / "csv"))
    table_id = "test.csv.empty"
    
    # 写入空 DataFrame
    empty_df = pl.DataFrame()
    storage.write_series(table_id, empty_df)
    
    # 验证不应该创建表目录
    table_dir = temp_storage_root / "csv" / table_id
    assert not table_dir.exists(), "空数据不应该创建表目录"


def test_csv_storage_incremental_update(temp_storage_root):
    """
    测试 CSV 存储层的增量更新能力
    """
    storage = CSVStorage(str(temp_storage_root / "csv"))
    table_id = "test.csv.incremental"
    
    # 第一次写入
    df1 = pl.DataFrame({
        "timestamp": [1672531200000, 1672617600000],
        "datetime": ["2023-01-01T00:00:00.000", "2023-01-02T00:00:00.000"],
        "symbol": ["sh.600000"] * 2,
        "close": [10.0, 10.5]
    })
    storage.write_series(table_id, df1)
    _stamp_metadata(storage, table_id, df1)
    
    # 第二次写入（增量）
    df2 = pl.DataFrame({
        "timestamp": [1672704000000, 1672790400000],
        "datetime": ["2023-01-03T00:00:00.000", "2023-01-04T00:00:00.000"],
        "symbol": ["sh.600000"] * 2,
        "close": [11.0, 11.5]
    })
    storage.write_series(table_id, df2)
    
    # 验证数据合并
    read_df = storage.read_series(table_id, "sh.600000", 2023)
    assert len(read_df) == 4, "增量更新后应该有 4 条记录"
    
    # 验证数据按时间排序
    timestamps = read_df["timestamp"].to_list()
    assert timestamps == sorted(timestamps), "数据应该按 timestamp 升序排列"


def test_csv_storage_deduplication(temp_storage_root):
    """
    测试 CSV 存储层的幂等/去重能力
    """
    storage = CSVStorage(str(temp_storage_root / "csv"))
    table_id = "test.csv.dedup"
    
    # 写入包含重复 timestamp 的数据
    df1 = pl.DataFrame({
        "timestamp": [1672531200000, 1672617600000],
        "datetime": ["2023-01-01T00:00:00.000", "2023-01-02T00:00:00.000"],
        "symbol": ["sh.600000"] * 2,
        "close": [10.0, 10.5]
    })
    
    df2 = pl.DataFrame({
        "timestamp": [1672617600000, 1672704000000],  # 2023-01-02 重复
        "datetime": ["2023-01-02T00:00:00.000", "2023-01-03T00:00:00.000"],
        "symbol": ["sh.600000"] * 2,
        "close": [99.9, 11.0]  # 01-02 的值被修改
    })
    
    storage.write_series(table_id, df1)
    _stamp_metadata(storage, table_id, df1)
    storage.write_series(table_id, df2)
    
    # 验证去重逻辑
    read_df = storage.read_series(table_id, "sh.600000", 2023)
    assert len(read_df) == 3, "重复 timestamp 应该被去重"
    
    # 验证 keep="last" 逻辑
    duplicate_row = read_df.filter(pl.col("timestamp") == 1672617600000)
    assert duplicate_row["close"][0] == 99.9, "应该保留最后写入的数据"


def test_csv_storage_sorting(temp_storage_root):
    """
    测试 CSV 存储层的排序能力
    """
    storage = CSVStorage(str(temp_storage_root / "csv"))
    table_id = "test.csv.sorting"
    
    # 写入乱序数据
    df = pl.DataFrame({
        "timestamp": [1672704000000, 1672531200000, 1672617600000],
        "datetime": ["2023-01-03T00:00:00.000", "2023-01-01T00:00:00.000", "2023-01-02T00:00:00.000"],
        "symbol": ["sh.600000"] * 3,
        "close": [11.0, 10.0, 10.5]
    })
    
    storage.write_series(table_id, df)
    _stamp_metadata(storage, table_id, df)
    
    # 验证读取的数据是有序的
    read_df = storage.read_series(table_id, "sh.600000", 2023)
    timestamps = read_df["timestamp"].to_list()
    assert timestamps == sorted(timestamps), "数据应该按 timestamp 升序排列"
    assert timestamps == [1672531200000, 1672617600000, 1672704000000]


def test_csv_storage_global_time_range(temp_storage_root):
    """
    测试 CSV 存储层的全局时间范围计算
    """
    storage = CSVStorage(str(temp_storage_root / "csv"))
    table_id = "test.csv.time_range"
    
    # 写入跨年数据
    df = pl.DataFrame({
        "timestamp": [1704067200000, 1735689600000],  # 2024-01-01, 2025-01-01
        "datetime": ["2024-01-01T00:00:00.000", "2025-01-01T00:00:00.000"],
        "symbol": ["sh.600000"] * 2,
        "close": [10.0, 10.5]
    })
    
    storage.write_series(table_id, df)
    _stamp_metadata(storage, table_id, df)
    
    # 验证全局时间范围
    time_range = storage.get_global_time_range(table_id)
    assert time_range[0] == 1704067200000, "最小时间戳应该是 2024-01-01"
    assert time_range[1] == 1735689600000, "最大时间戳应该是 2025-01-01"


def test_csv_storage_ev_no_symbol(temp_storage_root):
    """
    测试 CSV 存储对无 symbol 列 EV 数据的处理
    验证系统能够正确处理没有 symbol 列的宏观数据（如利率、指数成分变动）
    """
    storage = CSVStorage(str(temp_storage_root / "csv"), category="EV")
    table_id = "test.csv.ev_no_symbol"
    
    # 创建测试数据：没有 symbol 列，只有 timestamp 和 value
    df = pl.DataFrame({
        "timestamp": [1704067200000, 1704153600000, 1704240000000],  # 2024-01-01, 02, 03
        "value": [100.0, 101.0, 102.0]
    })
    
    # 写入数据
    storage.write_event(table_id, df, mode="overwrite")
    _stamp_metadata(storage, table_id, df, category="EV")
    
    # 手动补齐元数据
    meta_mgr = MetadataManager(storage.storage_root.parent)
    meta_mgr.save(table_id, "csv", {
        "table_id": table_id, "category": "EV", "format": "csv",
        "schema": {k: str(v) for k, v in df.schema.items()}
    })
    
    # 验证文件创建
    table_dir = temp_storage_root / "csv" / table_id
    data_file = table_dir / "year=2024" / "data.csv"
    
    assert data_file.exists(), "数据文件未创建"
    
    # 读取数据验证
    read_df = storage.read_event(table_id, 2024)
    
    # 验证数据完整性
    assert len(read_df) == 3, f"期望3行数据，实际得到{len(read_df)}行"
    assert "timestamp" in read_df.columns, "缺少timestamp列"
    assert "value" in read_df.columns, "缺少value列"
    assert "symbol" not in read_df.columns, "不应存在symbol列"
    
    # 测试增量写入（验证全行去重逻辑）
    df_new = pl.DataFrame({
        "timestamp": [
            1704067200000,  # 01-01: 与第一笔数据完全相同 -> 应被去重合并
            1704153600000,  # 01-02: timestamp 相同但 value 不同 -> 应均被保留 (README 规范：全行去重)
            1704326400000   # 01-04: 全新数据 -> 应新增
        ],
        "value": [100.0, 101.5, 103.0]
    })
    
    storage.write_event(table_id, df_new, mode="append")
    
    # 重新读取验证
    read_df_final = storage.read_event(table_id, 2024)
    
    # 验证去重结果：原有3行 + 新增2行（其中一笔完全重复被合并，一笔逻辑重复但值不同被保留）= 5 行
    # 如果系统错误地按 timestamp 去重，则为 4 行；如果不去重，则为 6 行。
    assert len(read_df_final) == 5, f"期望 5 行（验证全行去重）：原有3 + 新增2，当前 {len(read_df_final)}"
    
    # 测试 get_all_symbols 方法（应该返回空列表，因为没有 symbol 列）
    symbols = storage.get_all_symbols(table_id)
    assert symbols == [], f"期望返回空列表，实际得到 {symbols}"
