import pytest
import polars as pl
from pathlib import Path
from unittest.mock import MagicMock, patch
from app.service.sync_manager import SyncManager
from app.provider.baostock_provider import BaostockProvider
from app.storage.csv_storage import CSVStorage
from app.storage.parquet_storage import ParquetStorage
from app.service.metadata_manager import MetadataManager

def test_baostock_provider_error_fetch(mock_baostock):
    """
    测试 BaostockProvider API 报错：应当抛出 RuntimeError (取代原 test_baostock_provider_empty_fetch)
    """
    provider = BaostockProvider()
    table_id = "ashare.kline.1d.adj.baostock"
    
    # Mock 返回错误码
    mock_rs = MagicMock()
    mock_rs.error_code = "10001001"
    mock_rs.error_msg = "用户未登录"
    mock_baostock.query_history_k_data_plus.return_value = mock_rs
    
    with pytest.raises(RuntimeError) as exc_info:
        provider.fetch(table_id, "sh.600000", "2024-01-01", "2024-01-02")
    
    assert "Baostock API Error" in str(exc_info.value)
    assert "10001001" in str(exc_info.value)

def test_baostock_provider_legit_empty(mock_baostock):
    """
    测试 BaostockProvider 正常无数据（如停牌）：返回标准化的空 DataFrame
    """
    provider = BaostockProvider()
    table_id = "ashare.kline.1d.adj.baostock"
    
    # Mock 返回成功但无数据
    mock_rs = MagicMock()
    mock_rs.error_code = "0"
    mock_rs.next.return_value = False
    mock_baostock.query_history_k_data_plus.return_value = mock_rs
    
    df = provider.fetch(table_id, "sh.600000", "2099-01-01", "2099-01-02")
    
    # 验证返回空 DataFrame
    assert df.is_empty()
    
    # 验证标准列存在
    assert "timestamp" in df.columns
    assert "symbol" in df.columns
    assert "datetime" in df.columns

def test_baostock_index_unsupported_adj():
    """
    测试 Baostock 指数不支持复权：请求 aindex + adj 应该抛出 ValueError
    """
    provider = BaostockProvider()
    # 虽然 Table ID 可能合法，但 Provider 内部拦截
    table_id = "aindex.kline.1d.adj.baostock"
    
    with pytest.raises(ValueError) as exc_info:
        # fetch 内部解析 table_id 后发现是 index 且 adj != raw
        provider.fetch(table_id, "sh.000001", "2024-01-01", "2024-01-02")
    
    assert "is not supported by BaostockProvider" in str(exc_info.value)

def test_baostock_index_unsupported_freq():
    """
    测试 Baostock 指数不支持分钟线：请求 aindex + 5m 应该抛出 ValueError
    """
    provider = BaostockProvider()
    table_id = "aindex.kline.5m.raw.baostock"
    
    with pytest.raises(ValueError) as exc_info:
        provider.fetch(table_id, "sh.000001", "2024-01-01", "2024-01-02")
    
    assert "is not supported by BaostockProvider" in str(exc_info.value)

def test_sync_manager_initial_silent(temp_storage_root, mock_baostock):
    """
    测试初次同步静默：无数据不创建文件夹
    """
    sync_mgr = SyncManager()
    table_id = "ashare.kline.1d.adj.baostock"
    
    # Mock 返回空数据
    mock_rs = MagicMock()
    mock_rs.error_code = "0"
    mock_rs.next.return_value = False
    mock_baostock.query_history_k_data_plus.return_value = mock_rs
    
    # 同步空数据
    sync_mgr.sync(table_id, "csv", "1990-01-01", "1990-01-02")
    
    # 验证表目录不存在
    table_dir = temp_storage_root / "csv" / table_id
    assert not table_dir.exists(), "初次同步无数据不应该创建表目录"

def test_sync_manager_incremental_silent(temp_storage_root, mock_baostock):
    """
    测试增量同步防写：无数据不更新元数据
    """
    sync_mgr = SyncManager()
    table_id = "ashare.kline.1d.adj.baostock"
    format = "csv"
    
    # 1. 手动注入数据
    storage = CSVStorage(str(temp_storage_root / "csv"))
    test_df = pl.DataFrame({
        "timestamp": [1704067200000], 
        "datetime": ["2024-01-01T00:00:00.000"],
        "symbol": ["sh.600000"],
        "open": [10.0]
    })
    storage.write_series(table_id, test_df)
    
    # 创建初始元数据
    metadata_mgr = MetadataManager(str(temp_storage_root))
    metadata_mgr.save(table_id, "csv", {"table_id": table_id, "format": "csv", "schema": {k: str(v) for k, v in test_df.schema.items()}})
    initial_metadata = {
        "table_id": table_id,
        "format": format,
        "global_stats": {"total_bars": 1},
        "schema": {"timestamp": "Int64", "symbol": "String", "datetime": "String", "open": "Float64"}
    }
    metadata_mgr.save(table_id, format, initial_metadata)
    
    # 记录初始元数据修改时间
    metadata_path = Path(temp_storage_root) / format / table_id / "metadata.json"
    initial_mtime = metadata_path.stat().st_mtime
    initial_content = metadata_path.read_text()
    
    # 2. Mock 返回空数据
    mock_rs = MagicMock()
    mock_rs.error_code = "0"
    mock_rs.next.return_value = False
    mock_baostock.query_history_k_data_plus.return_value = mock_rs
    
    # 3. 增量同步无数据
    sync_mgr.sync(table_id, format, "2099-01-01", "2099-01-02")
    
    # 4. 验证元数据未变
    assert metadata_path.stat().st_mtime == initial_mtime
    assert metadata_path.read_text() == initial_content

def test_storage_empty_write_interception(temp_storage_root):
    """
    测试存储层空拦截：空 DataFrame 不写入文件
    """
    # CSV
    csv_root = temp_storage_root / "csv"
    csv_storage = CSVStorage(str(csv_root))
    csv_storage.write_series("test.table", pl.DataFrame())
    assert not (csv_root / "test.table").exists(), "空数据不应该创建表目录"

    # Parquet
    pq_root = temp_storage_root / "parquet"
    pq_storage = ParquetStorage(str(pq_root))
    pq_storage.write_series("test.table", pl.DataFrame())
    assert not (pq_root / "test.table").exists(), "空数据不应该创建表目录"

def test_sync_manager_provider_exception(temp_storage_root):
    """
    测试 SyncManager 异常中断：Provider 抛出异常时应该中断同步
    """
    sync_mgr = SyncManager()
    table_id = "ashare.kline.1d.adj.baostock"
    
    # 创建抛出异常的 Provider
    class ExceptionProvider:
        def get_supported_tables(self):
            return ["ashare.kline.1d.adj.baostock"]
        
        def get_all_symbols(self, table_id):
            return ["sh.600000"]
        
        def fetch(self, table_id, symbol, start_date, end_date, **kwargs):
            raise RuntimeError("模拟网络异常")
    
    with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=ExceptionProvider()):
        # 同步应该抛出异常
        with pytest.raises(RuntimeError) as exc_info:
            sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-05")
        
        assert "模拟网络异常" in str(exc_info.value)
        
        # 验证表目录不存在（异常中断不应该创建）
        table_dir = temp_storage_root / "csv" / table_id
        assert not table_dir.exists(), "异常中断不应该创建表目录"

def test_sync_manager_partial_failure(temp_storage_root):
    """
    测试 SyncManager Fail-Fast 策略：一个 symbol 失败应该中断整个同步
    """
    sync_mgr = SyncManager()
    table_id = "ashare.kline.1d.adj.baostock"
    
    # 创建部分失败的 Provider
    class PartialFailureProvider:
        def get_supported_tables(self):
            return ["ashare.kline.1d.adj.baostock"]
        
        def get_all_symbols(self, table_id):
            return ["sh.600000", "sz.000001"]
        
        def fetch(self, table_id, symbol, start_date, end_date, **kwargs):
            if symbol == "sh.600000":
                # 第一个 symbol 成功
                return pl.DataFrame({
                    "timestamp": [start_date, end_date],
                    "datetime": ["2024-01-01T00:00:00.000", "2024-01-02T00:00:00.000"],
                    "symbol": [symbol] * 2,
                    "close": [10.0, 11.0]
                })
            else:
                # 第二个 symbol 失败
                raise RuntimeError("第二个 symbol 失败")
    
    with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=PartialFailureProvider()):
        # 同步应该抛出异常（Fail-Fast）
        with pytest.raises(RuntimeError) as exc_info:
            sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-05")
        
        assert "第二个 symbol 失败" in str(exc_info.value)
        
        # Fail-Fast 策略：失败时不写入任何数据
        csv_storage = CSVStorage(str(temp_storage_root / "csv"))
        symbols = csv_storage.get_all_symbols(table_id)
        # 由于是批量写入，第一个symbol成功后会立即写入，但第二个失败会中断
        # 实际行为取决于批量大小，这里验证没有数据写入也是合理的
        assert len(symbols) == 0, "Fail-Fast 策略下，失败时不应该写入数据"

def test_storage_duplicate_timestamp_handling(temp_storage_root):
    """
    测试存储层重复 timestamp 处理：毫秒级对齐和 unique 排序
    """
    csv_storage = CSVStorage(str(temp_storage_root / "csv"))
    table_id = "test.duplicate"
    
    # 写入包含重复 timestamp 的数据
    df1 = pl.DataFrame({
        "timestamp": [1704067200000, 1704153600000],
        "datetime": ["2024-01-01T00:00:00.000", "2024-01-02T00:00:00.000"],
        "symbol": ["sh.600000"] * 2,
        "close": [10.0, 10.1]
    })
    
    df2 = pl.DataFrame({
        "timestamp": [1704153600000, 1704240000000],  # 重复的 1704153600000
        "datetime": ["2024-01-02T00:00:00.000", "2024-01-03T00:00:00.000"],
        "symbol": ["sh.600000"] * 2,
        "close": [99.9, 10.2]  # 修改 01-02 的值
    })
    
    csv_storage.write_series(table_id, df1)
    metadata_mgr = MetadataManager(str(temp_storage_root))
    metadata_mgr.save(table_id, "csv", {"table_id": table_id, "format": "csv", "schema": {k: str(v) for k, v in df1.schema.items()}})
    csv_storage.write_series(table_id, df2)
    
    # 验证去重逻辑
    read_df = csv_storage.read_series(table_id, "sh.600000", 2024)
    assert len(read_df) == 3, "重复 timestamp 应该被去重"
    
    # 验证 keep="last" 逻辑
    duplicate_row = read_df.filter(pl.col("timestamp") == 1704153600000)
    assert duplicate_row["close"][0] == 99.9, "应该保留最后写入的数据"

def test_storage_cross_year_partition(temp_storage_root):
    """
    测试存储层跨年分区：验证年份目录创建
    """
    csv_storage = CSVStorage(str(temp_storage_root / "csv"))
    table_id = "test.cross.year"
    
    # 写入跨年数据
    df = pl.DataFrame({
        "timestamp": [1735689599000, 1735689600000],  # 2024-12-31 和 2025-01-01
        "datetime": ["2024-12-31T23:59:59.000", "2025-01-01T00:00:00.000"],
        "symbol": ["sh.600000"] * 2,
        "close": [10.5, 10.6]
    })
    
    csv_storage.write_series(table_id, df)
    
    # 验证年份目录存在
    year_2024_dir = temp_storage_root / "csv" / table_id / "year=2024"
    year_2025_dir = temp_storage_root / "csv" / table_id / "year=2025"
    
    assert year_2024_dir.exists(), "2024 年目录应该存在"
    assert year_2025_dir.exists(), "2025 年目录应该存在"
    
    # 验证文件存在
    assert (year_2024_dir / "sh.600000.csv").exists()
    assert (year_2025_dir / "sh.600000.csv").exists()