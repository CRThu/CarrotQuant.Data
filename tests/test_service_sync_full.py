import os
import sys
import shutil
import json
import polars as pl
from pathlib import Path
from typing import List, Dict, Any
import datetime as dt_mod
from unittest.mock import MagicMock, patch

# 将当前根目录加入系统路径
sys.path.append(os.getcwd())

from app.storage.storage_factory import StorageFactory
from app.service.metadata_manager import MetadataManager
from app.service.task_planner import TaskPlanner
from app.service.sync_manager import SyncManager
from app.provider.base import BaseProvider
from app.config.settings import settings

class FakeProvider(BaseProvider):
    """模拟数据源驱动"""
    def fetch(self, table_id: str, symbol: str, start_date: Any, end_date: Any, **kwargs) -> pl.DataFrame:
        # 简单根据 symbol 和日期生成 mock 数据
        # 假设 start_date 和 end_date 已经是毫秒戳 (由 SyncManager 传入)
        data = {
            "timestamp": [start_date, end_date],
            "datetime": ["2024-01-01T00:00:00.000", "2024-01-02T00:00:00.000"], # 简化处理
            "symbol": [symbol] * 2,
            "close": [10.0, 11.0]
        }
        # 只有在 start <= end 时返回数据，模拟真实源的行为。
        # 此处固定返回 2 条数据：一条在 start 点，一条在 end 点。
        if start_date <= end_date:
            return pl.DataFrame(data)
        return pl.DataFrame()

def test_full_sync_flow():
    test_root = Path("test_sync_full_root")
    if test_root.exists():
        shutil.rmtree(test_root)
    
    # 1. 准备组件
    with patch("app.config.settings.settings.STORAGE_ROOT", str(test_root)):
        sync_mgr = SyncManager()
        # 为了后续验证，直接从 sync_mgr 拿内部实例
        metadata_mgr = sync_mgr.metadata_mgr
        planner = sync_mgr.planner
        # 由于 StorageFactory 内部会拼接 format，这里手动创建一个用于外部验证的 storage 实例
        storage = StorageFactory.get_storage("csv", str(test_root))
    
    table_id = "ashare.kline.1d.adj.baostock"
    symbols = ["sh.600000", "sz.000001"]
    
    # 2. 模拟 ProviderManager.get_provider 返回 FakeProvider
    fake_provider = FakeProvider()
    
    try:
        with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=fake_provider):
            print("\n--- [Step 1] 第一次同步 (全量) ---")
            # 请求区间：2024-01-01 -> 2024-01-02
            sync_mgr.sync(table_id, "csv", symbols, "2024-01-01", "2024-01-02")
            
            # 验证元数据
            meta = metadata_mgr.load(table_id, "csv")
            actual_end_ts = meta["global_stats"]["end_timestamp"]
            print(f"[*] Meta end_timestamp: {actual_end_ts}")
            
            # 预期：2024-01-02 00:00:00+08 (根据 parse_date_to_ts 解析结果)
            expected_end_ts = int(dt_mod.datetime(2024, 1, 2, tzinfo=dt_mod.timezone(dt_mod.timedelta(hours=8))).timestamp() * 1000)
            assert actual_end_ts == expected_end_ts, f"预期 {expected_end_ts}, 实际 {actual_end_ts}"
            
            print("\n--- [Step 2] 第二次同步 (增量) ---")
            # 请求区间：2024-01-01 -> 2024-01-05
            # 核心预期：Planner 应计算出起始点为 2024-01-02
            
            # 我们需要先获取旧的 end_ts 以便验证
            old_end_ts = meta["global_stats"]["end_timestamp"]
            
            # 再次 patch fetch 以验证传入参数
            original_fetch = fake_provider.fetch
            def mock_fetch(tid, sym, start, end, **kwargs):
                print(f"[*] Fetching {sym}: {start} to {end}")
                # 验证起始点对齐逻辑
                assert start == old_end_ts
                return original_fetch(tid, sym, start, end, **kwargs)
            
            fake_provider.fetch = MagicMock(side_effect=mock_fetch)
            
            sync_mgr.sync(table_id, "csv", symbols, "2024-01-01", "2024-01-05")
            
            # 验证总行数 (计算推导逻辑如下):
            # 1. 第一次同步: 2 个 symbol, 每个 [01-01, 01-02] 2 条 -> 共 4 条
            # 2. 第二次同步: 2 个 symbol, 每个 [01-02, 01-05] 2 条 -> 共 4 条
            # 3. 存储层合并: 物理上每个 symbol 拥有 [01-01, 01-02, 01-02, 01-05] (01-02 重叠)
            # 4. 去重逻辑: Storage.write 内部调用 unique(timestamp) 剔除 symbol 内部重复的 01-02 点
            # 5. 最终状态: 每个 symbol 拥有 [01-01, 01-02, 01-05] 3 条 -> 2 symbol * 3 = 6 条
            meta = metadata_mgr.load(table_id, "csv")
            assert meta["global_stats"]["total_bars"] == 6, f"预期 6 条, 实际 {meta['global_stats']['total_bars']} 条 (检查去重逻辑)"
            assert meta["global_stats"]["symbol_count"] == 2
            
            print("\n--- [Step 3] 验证物理巡检与元数据 1:1 匹配 ---")
            # 存储层提供的统计接口必须与 metadata.json 记录的数据完全一致
            total_disk_bars = storage.get_total_bars(table_id)
            assert total_disk_bars == meta["global_stats"]["total_bars"]
            
        print("\n[OK] 全链路同步测试通过！")
    except Exception as e:
        print(f"\n[FAIL] 测试运行失败: {e}")
        import traceback
        traceback.print_exc()
        raise e
    # shutil.rmtree(test_root)

if __name__ == "__main__":
    test_full_sync_flow()
