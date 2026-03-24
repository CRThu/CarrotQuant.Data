import pytest
import polars as pl
from pathlib import Path
from unittest.mock import MagicMock, patch
from app.service.sync_manager import SyncManager
from app.provider.base import BaseProvider
from app.utils.time_utils import parse_date_to_ts

class FakeProvider(BaseProvider):
    """模拟数据源驱动"""
    def get_supported_tables(self):
        return ["ashare.kline.1d.adj.baostock"]

    def get_all_symbols(self, table_id):
        return ["sh.600000", "sz.000001"]

    def fetch(self, table_id, symbol, start_date, end_date, **kwargs):
        # 简单根据 symbol 和日期生成 mock 数据
        data = {
            "timestamp": [start_date, end_date],
            "datetime": ["2024-01-01T00:00:00.000", "2024-01-02T00:00:00.000"],
            "symbol": [symbol] * 2,
            "close": [10.0, 11.0]
        }
        if start_date <= end_date:
            return pl.DataFrame(data)
        return pl.DataFrame()

def test_sync_full_flow(temp_storage_root):
    """
    测试 SyncManager 全链路流程：全量下载 -> 增量下载 -> 物理巡检 -> 元数据盖章
    """
    table_id = "ashare.kline.1d.adj.baostock"
    
    with patch("app.config.settings.settings.STORAGE_ROOT", str(temp_storage_root)):
        sync_mgr = SyncManager()
        fake_provider = FakeProvider()
        
        with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=fake_provider):
            # --- Step 1: 第一次同步 (全量) ---
            sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-02")
            
            # 验证元数据
            metadata = sync_mgr.metadata_mgr.load(table_id, "csv")
            expected_end_ts = parse_date_to_ts("2024-01-02")
            actual_end_ts = metadata["global_stats"]["end_timestamp"]
            assert actual_end_ts == expected_end_ts, f"预期 {expected_end_ts}，实际 {actual_end_ts}"
            
            # --- Step 2: 第二次同步 (增量) ---
            # Mock fetch 以验证传入参数
            original_fetch = fake_provider.fetch
            def mock_fetch(tid, sym, start, end, **kwargs):
                # 增量同步应该从本地结束时间（2024-01-02）开始
                expected_start = parse_date_to_ts("2024-01-02")
                assert start == expected_start, f"预期 {expected_start}，实际 {start}"
                return original_fetch(tid, sym, start, end, **kwargs)
            
            fake_provider.fetch = MagicMock(side_effect=mock_fetch)
            
            sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-05")
            
            # 验证总行数
            metadata = sync_mgr.metadata_mgr.load(table_id, "csv")
            # 2个symbol，每个symbol有3条记录（去重后）
            assert metadata["global_stats"]["total_bars"] == 6, f"预期 6 条，实际 {metadata['global_stats']['total_bars']} 条"
            assert metadata["global_stats"]["symbol_count"] == 2
            
            # --- Step 3: 验证物理巡检与元数据 1:1 匹配 ---
            from app.storage.csv_storage import CSVStorage
            storage = CSVStorage(str(temp_storage_root / "csv"))
            total_disk_bars = storage.get_total_bars(table_id)
            assert total_disk_bars == metadata["global_stats"]["total_bars"]

def test_sync_incremental_logic(temp_storage_root):
    """
    测试增量同步逻辑：验证本地已有数据时的补丁计算
    """
    table_id = "ashare.kline.1d.adj.baostock"
    
    with patch("app.config.settings.settings.STORAGE_ROOT", str(temp_storage_root)):
        sync_mgr = SyncManager()
        fake_provider = FakeProvider()
        
        with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=fake_provider):
            # --- 先同步 2024-01-01 到 2024-01-02 ---
            sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-02")
            
            # --- 再同步 2024-01-01 到 2024-01-05 ---
            # Mock fetch 验证增量逻辑
            original_fetch = fake_provider.fetch
            call_count = 0
            
            def mock_fetch(tid, sym, start, end, **kwargs):
                nonlocal call_count
                call_count += 1
                # 增量同步应该从本地结束时间（2024-01-02）开始
                if call_count == 1:  # 第一个symbol
                    expected_start = parse_date_to_ts("2024-01-02")
                    assert start == expected_start
                return original_fetch(tid, sym, start, end, **kwargs)
            
            fake_provider.fetch = MagicMock(side_effect=mock_fetch)
            
            sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-05")
            
            # 验证增量同步后数据量
            metadata = sync_mgr.metadata_mgr.load(table_id, "csv")
            assert metadata["global_stats"]["total_bars"] == 6

def test_sync_force_refresh(temp_storage_root):
    """
    测试强制刷新模式：忽略本地水位，直接覆盖
    """
    table_id = "ashare.kline.1d.adj.baostock"
    
    with patch("app.config.settings.settings.STORAGE_ROOT", str(temp_storage_root)):
        sync_mgr = SyncManager()
        fake_provider = FakeProvider()
        
        with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=fake_provider):
            # --- 先同步一次 ---
            sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-02")
            
            # --- 强制刷新同一时间范围 ---
            original_fetch = fake_provider.fetch
            def mock_fetch(tid, sym, start, end, **kwargs):
                # 强制刷新应该直接使用请求的时间范围
                expected_start = parse_date_to_ts("2024-01-01")
                expected_end = parse_date_to_ts("2024-01-02")
                assert start == expected_start
                assert end == expected_end
                return original_fetch(tid, sym, start, end, **kwargs)
            
            fake_provider.fetch = MagicMock(side_effect=mock_fetch)
            
            sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-02", force_refresh=True)
            
            # 验证数据量不变（覆盖写入）
            metadata = sync_mgr.metadata_mgr.load(table_id, "csv")
            assert metadata["global_stats"]["total_bars"] == 4

def test_sync_empty_data_defense(temp_storage_root):
    """
    测试空数据防御：无数据不创建文件夹、不更新元数据
    """
    table_id = "ashare.kline.1d.adj.baostock"
    
    # 创建返回空数据的 Provider
    class EmptyProvider(BaseProvider):
        def get_supported_tables(self):
            return ["ashare.kline.1d.adj.baostock"]
        
        def get_all_symbols(self, table_id):
            return []
        
        def fetch(self, table_id, symbol, start_date, end_date, **kwargs):
            return pl.DataFrame()
    
    with patch("app.config.settings.settings.STORAGE_ROOT", str(temp_storage_root)):
        sync_mgr = SyncManager()
        empty_provider = EmptyProvider()
        
        with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=empty_provider):
            # 同步空数据
            sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-05")
            
            # 验证表目录不存在
            table_dir = temp_storage_root / "csv" / table_id
            assert not table_dir.exists(), "空数据不应该创建表目录"