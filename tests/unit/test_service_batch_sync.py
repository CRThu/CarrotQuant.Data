import pytest
import polars as pl
from unittest.mock import MagicMock, patch
from app.service.sync_manager import SyncManager

@pytest.fixture
def sync_manager():
    with patch("app.service.sync_manager.MetadataManager"), \
         patch("app.service.sync_manager.TaskPlanner"), \
         patch("app.service.sync_manager.ProviderManager"), \
         patch("app.service.sync_manager.StorageFactory"):
        sm = SyncManager()
        yield sm

def test_sync_manager_batch_write(sync_manager):
    table_id = "test.table"
    format = "parquet"
    start_date = "2023-01-01"
    end_date = "2023-01-10"
    
    # 模拟 10 支股票
    symbols = [f"SZ{i:06d}" for i in range(10)]
    
    # 模拟 Planner
    sync_manager.planner.plan.return_value = [{"symbol": s, "start": 0, "end": 0} for s in symbols]
    
    # 模拟 Provider
    provider = MagicMock()
    # 每次 fetch 返回一个简单的 DataFrame
    provider.fetch.side_effect = lambda tid, sym, s, e: pl.DataFrame({
        "symbol": [sym],
        "timestamp": [1672531200000],
        "close": [10.0]
    })
    sync_manager.provider_mgr.get_provider.return_value = provider
    provider.get_all_symbols.return_value = symbols
    
    # 模拟 Storage
    storage = MagicMock()
    # 设置 Storage 返回规范值
    storage.get_all_symbols.return_value = symbols
    storage.get_total_bars.return_value = 10
    storage.get_global_time_range.return_value = (0, 0)
    storage.get_unique_timestamps.return_value = []
    storage.read.return_value = pl.DataFrame()
    storage.category = "TS"
    
    # 直接在 sync_manager 的 mock StorageFactory 上设置返回值
    from app.service import sync_manager as sm_mod
    sync_manager.storage_root = "test_root"
    
    # 注意：sync_manager.py 中 StorageFactory 已被 fixture patch 为 MagicMock
    # 我们只需要配置这个已有的 mock
    with patch("app.service.sync_manager.StorageFactory.get_storage", return_value=storage):
        # 设置 batch_size=3
        # 10 支股票，batch_size=3，应调用 storage.write 4 次 (3+3+3+1)
        sync_manager.sync(table_id, format, start_date, end_date, batch_size=3)
        
        # 验证调用次数
        assert storage.write.call_count == 4
        
        # 验证第一次写入的内容 (全量写，包含 3 支股票)
        first_write_df = storage.write.call_args_list[0][0][1] # 第二个参数是 df
        assert len(first_write_df) == 3
        assert first_write_df["symbol"].to_list() == symbols[:3]
        
        # 验证最后一次写入的内容
        last_write_df = storage.write.call_args_list[3][0][1]
        assert len(last_write_df) == 1
        assert last_write_df["symbol"].to_list() == symbols[9:]
