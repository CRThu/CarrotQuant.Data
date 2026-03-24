import pytest
from unittest.mock import patch, MagicMock
import polars as pl
from app.service.sync_manager import SyncManager
from app.config.settings import settings
import shutil
import os

@pytest.fixture
def mock_provider():
    with patch("app.provider.provider_manager.ProviderManager.get_provider") as mock_mgr:
        provider = MagicMock()
        provider.get_all_symbols.return_value = ["sh.600000"]
        # 返回一条假数据
        df = pl.DataFrame({
            "timestamp": [1704067200000],
            "datetime": ["2024-01-01T00:00:00.000"],
            "symbol": ["sh.600000"],
            "close": [10.5]
        })
        provider.fetch.return_value = df
        mock_mgr.return_value = provider
        yield provider

def test_sync_multi_formats(mock_provider):
    """验证 SyncManager 一次抓取、多次存储逻辑"""
    test_root = "test_sync_multi_root"
    if os.path.exists(test_root):
        shutil.rmtree(test_root)
    
    # 模拟环境配置
    with patch("app.config.settings.settings.STORAGE_ROOT", test_root):
        sync_mgr = SyncManager()
        table_id = "ashare.kline.1d.adj.baostock"
        # 同时同步到 csv 和 parquet 格式
        formats = ["csv", "parquet"]
        
        # 规划补丁
        # mock task planner so we don't depend on actual metadata
        with patch("app.service.task_planner.TaskPlanner.plan") as mock_plan:
            mock_plan.return_value = [{"symbol": "sh.600000", "start": 0, "end": 1}]
            
            sync_mgr.sync(
                table_ids=[table_id],
                formats=formats,
                start_date="2024-01-01",
                end_date="2024-01-02",
                batch_size=1
            )
            
            # 1. 验证 Provider.fetch 仅被调用一次 (原子性)
            assert mock_provider.fetch.call_count == 1
            
            # 2. 验证磁盘文件是否都生成了
            csv_path = os.path.join(test_root, "csv", table_id, "year=2024", "sh.600000.csv")
            parquet_path = os.path.join(test_root, "parquet", table_id, "year=2024", "2024-01.parquet")
            assert os.path.exists(csv_path)
            assert os.path.exists(parquet_path)
            
            # 3. 验证元数据是否均生成
            assert os.path.exists(os.path.join(test_root, "csv", table_id, "metadata.json"))
            assert os.path.exists(os.path.join(test_root, "parquet", table_id, "metadata.json"))

    # 清理
    if os.path.exists(test_root):
        shutil.rmtree(test_root)

def test_sync_fail_fast(mock_provider):
    """验证同步 Fail-Fast 策略"""
    # 模拟 fetch 失败
    mock_provider.fetch.side_effect = Exception("Network Error")
    
    sync_mgr = SyncManager()
    with patch("app.service.task_planner.TaskPlanner.plan") as mock_plan:
        mock_plan.return_value = [{"symbol": "sh.600000", "start": 0, "end": 1}]
        
        try:
            sync_mgr.sync(
                table_ids="error.table",
                formats="csv",
                start_date="2024-01-01"
            )
            assert False, "Should raise exception on fetch error"
        except Exception as e:
            assert "Network Error" in str(e)
