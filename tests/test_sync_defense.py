import pytest
import polars as pl
from pathlib import Path
import time
import os
import sys

# 将当前根目录加入系统路径
sys.path.append(os.getcwd())

from app.provider.baostock_provider import BaostockProvider
from app.service.sync_manager import SyncManager
from app.storage.csv_storage import CSVStorage
from app.storage.parquet_storage import ParquetStorage
from app.service.metadata_manager import MetadataManager
from app.config.settings import settings

from unittest.mock import patch, MagicMock

@pytest.fixture
def temp_storage_root(tmp_path):
    """
    创建一个临时的存储根目录，并自动清理。
    """
    original_root = settings.STORAGE_ROOT
    settings.STORAGE_ROOT = str(tmp_path)
    yield tmp_path
    settings.STORAGE_ROOT = original_root

@pytest.fixture
def mock_baostock():
    """
    Mock Baostock 核心驱动，避免网络 IO。
    """
    with patch("app.provider.baostock_provider.bs") as mock_bs:
        mock_bs.login.return_value = MagicMock(error_code="0")
        # 默认模拟 query_stock_basic
        mock_rs_basic = MagicMock()
        mock_rs_basic.error_code = "0"
        mock_rs_basic.next.side_effect = [True, False]
        mock_rs_basic.get_row_data.return_value = ["sh.600000", "浦发银行", "1999-11-10", "", "1", "1"]
        mock_bs.query_stock_basic.return_value = mock_rs_basic
        
        # 默认模拟 query_history_k_data_plus
        mock_rs_k = MagicMock()
        mock_rs_k.error_code = "0"
        mock_rs_k.next.return_value = False # 默认空数据
        mock_bs.query_history_k_data_plus.return_value = mock_rs_k
        
        yield mock_bs

def test_baostock_provider_empty_fetch(mock_baostock):
    """
    Test Case 1: 驱动骨架测试
    调用 BaostockProvider.fetch 抓取一个未来日期（通过 Mock 模拟空结果）。
    断言：返回的 DF 是空的，但必须包含 timestamp, symbol, datetime 等标准列。
    """
    provider = BaostockProvider()
    table_id = "ashare.kline.1d.adj.baostock"
    
    # 执行抓取
    df = provider.fetch(table_id, "sh.600000", "2099-01-01", "2099-01-02")
    
    assert df.is_empty()
    # 核心列必须存在且经过标准化
    assert "timestamp" in df.columns
    assert "symbol" in df.columns
    assert "datetime" in df.columns
    # 业务字段也应被重命名并保留
    assert "open" in df.columns

def test_sync_manager_initial_silent(temp_storage_root, mock_baostock):
    """
    Test Case 2: 初次同步静默测试
    """
    sync_mgr = SyncManager()
    table_id = "ashare.kline.1d.adj.baostock"
    
    # 使用 Mock 保证 get_all_symbols 和 fetch 都是空的
    sync_mgr.sync(table_id, "csv", "1990-01-01", "1990-01-02")
    
    table_dir = temp_storage_root / "csv" / table_id
    assert not table_dir.exists(), "Initial sync with no data should NOT create table directory"

def test_sync_manager_incremental_silent(temp_storage_root, mock_baostock):
    """
    Test Case 3: 增量同步防写测试
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
    storage.write(table_id, test_df)
    
    metadata_mgr = MetadataManager(str(temp_storage_root))
    initial_metadata = {
        "table_id": table_id,
        "format": format,
        "global_stats": {"total_bars": 1},
        "schema": {"timestamp": "Int64", "symbol": "String", "datetime": "String", "open": "Float64"}
    }
    metadata_mgr.save(table_id, format, initial_metadata)
    
    metadata_path = Path(temp_storage_root) / format / table_id / "metadata.json"
    initial_mtime = metadata_path.stat().st_mtime
    initial_content = metadata_path.read_text()
    
    time.sleep(0.1)
    
    # 2. 增量同步无数据
    sync_mgr.sync(table_id, format, "2099-01-01", "2099-01-02")
    
    # 3. 断言 mtime 未变且内容一致
    assert metadata_path.stat().st_mtime == initial_mtime
    assert metadata_path.read_text() == initial_content

def test_storage_empty_write_interception(temp_storage_root):
    """
    Test Case 4: 存储层空拦截测试
    """
    # CSV
    csv_root = temp_storage_root / "csv"
    csv_storage = CSVStorage(str(csv_root))
    csv_storage.write("test.table", pl.DataFrame())
    assert not (csv_root / "test.table").exists()

    # Parquet
    pq_root = temp_storage_root / "parquet"
    pq_storage = ParquetStorage(str(pq_root))
    pq_storage.write("test.table", pl.DataFrame())
    assert not (pq_root / "test.table").exists()
