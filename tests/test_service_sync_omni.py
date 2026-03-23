import os
import sys
import shutil
import polars as pl
from pathlib import Path
from typing import List, Any
import datetime as dt_mod
from unittest.mock import MagicMock, patch

# 将当前根目录加入系统路径
sys.path.append(os.getcwd())

from app.storage.storage_factory import StorageFactory
from app.service.sync_manager import SyncManager
from app.provider.base import BaseProvider
from app.utils.time_utils import parse_date_to_ts

class FakeProvider(BaseProvider):
    """模拟数据源驱动"""
    def get_supported_tables(self) -> List[str]:
        return ["ashare.kline.1d.adj.baostock"]

    def get_all_symbols(self, table_id: str) -> List[str]:
        return ["sh.600000"]

    def fetch(self, table_id: str, symbol: str, start_date: Any, end_date: Any, **kwargs) -> pl.DataFrame:
        # 使用传入的时间戳直接生成数据 (简化处理：2 个点，起点和终点)
        data = {
            "timestamp": [start_date, end_date],
            "datetime": ["2010-01-01T00:00:00.000", "2025-01-01T00:00:00.000"], # 占位
            "symbol": [symbol] * 2,
            "close": [10.0, 11.0]
        }
        if start_date < end_date:
            return pl.DataFrame(data)
        elif start_date == end_date:
            return pl.DataFrame(data).head(1)
        return pl.DataFrame()

def test_omni_sync_logic():
    test_root = Path("test_sync_omni_root")
    if test_root.exists():
        shutil.rmtree(test_root)
    
    table_id = "ashare.kline.1d.adj.baostock"
    
    with patch("app.config.settings.settings.STORAGE_ROOT", str(test_root)):
        sync_mgr = SyncManager()
        fake_provider = FakeProvider()
        
        with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=fake_provider):
            # --- 基础状态：本地已有 [2015-01-01, 2020-01-01] ---
            print("\nPrerequisite: Setting up local data [2015-01-01, 2020-01-01]...")
            sync_mgr.sync(table_id, "csv", "2015-01-01", "2020-01-01")
            
            # --- Test Case 1: 前向补全 (Prepend) ---
            # 本地已有：[2015, 2020]
            # 请求范围：[2010, 2012]
            # 预期任务：[2010, 2015]
            print("\n--- Test Case 1: 前向补全 (Prepend) ---")
            ts_2010 = parse_date_to_ts("2010-01-01")
            ts_2015 = parse_date_to_ts("2015-01-01")
            
            original_fetch = fake_provider.fetch
            def mock_fetch_c1(tid, sym, start, end, **kwargs):
                print(f"[*] Fetching {sym}: {start} to {end}")
                assert start == ts_2010
                assert end == ts_2015
                return original_fetch(tid, sym, start, end, **kwargs)
            
            fake_provider.fetch = MagicMock(side_effect=mock_fetch_c1)
            sync_mgr.sync(table_id, "csv", "2010-01-01", "2012-01-01")
            
            # --- Test Case 2: 后向拓展 (Append) ---
            # 本地已有：[2010, 2015] (从 Case 1 同步来的) + [2015, 2020] -> 实质已覆盖 [2010, 2020]
            # 请求范围：[2021, 2025]
            # 预期任务：[2020, 2025]
            print("\n--- Test Case 2: 后向拓展 (Append) ---")
            ts_2020 = parse_date_to_ts("2020-01-01")
            ts_2025 = parse_date_to_ts("2025-01-01")
            
            def mock_fetch_c2(tid, sym, start, end, **kwargs):
                print(f"[*] Fetching {sym}: {start} to {end}")
                assert start == ts_2020
                assert end == ts_2025
                return original_fetch(tid, sym, start, end, **kwargs)
            
            fake_provider.fetch = MagicMock(side_effect=mock_fetch_c2)
            sync_mgr.sync(table_id, "csv", "2021-01-01", "2025-01-01")
            
            # --- Test Case 3: 强制覆盖 (Force Refresh) ---
            # 本地已有：[2010, 2025]
            # 请求范围：[2015, 2020]，force_refresh=True
            # 预期任务：[2015, 2020]
            print("\n--- Test Case 3: 强制覆盖 (Force Refresh) ---")
            ts_req_start = parse_date_to_ts("2015-01-01")
            ts_req_end = parse_date_to_ts("2020-01-01")
            
            def mock_fetch_c3(tid, sym, start, end, **kwargs):
                print(f"[*] Fetching {sym}: {start} to {end}")
                assert start == ts_req_start
                assert end == ts_req_end
                return original_fetch(tid, sym, start, end, **kwargs)
            
            fake_provider.fetch = MagicMock(side_effect=mock_fetch_c3)
            sync_mgr.sync(table_id, "csv", "2015-01-01", "2020-01-01", force_refresh=True)

            # --- Test Case 4: 双向穿透 (Full Patch) ---
            # 本地已有：[2010, 2025] (由于 Case 3 覆盖，现在实际上还是 [2010, 2025] 的并集)
            # 请求范围：[2005-01-01, 2030-01-01]
            # 预期任务：[2005, 2030]
            print("\n--- Test Case 4: 双向穿透 (Full Patch) ---")
            ts_full_start = parse_date_to_ts("2005-01-01")
            ts_full_end = parse_date_to_ts("2030-01-01")
            
            def mock_fetch_c4(tid, sym, start, end, **kwargs):
                print(f"[*] Fetching {sym}: {start} to {end}")
                assert start == ts_full_start
                assert end == ts_full_end
                return original_fetch(tid, sym, start, end, **kwargs)
            
            fake_provider.fetch = MagicMock(side_effect=mock_fetch_c4)
            sync_mgr.sync(table_id, "csv", "2005-01-01", "2030-01-01")

    print("\n[OK] Omni-directional sync logic tests passed!")

if __name__ == "__main__":
    test_omni_sync_logic()
