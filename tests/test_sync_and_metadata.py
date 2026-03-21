import os
import sys
import shutil
import json
import polars as pl
from pathlib import Path

# 将当前根目录加入系统路径
sys.path.append(os.getcwd())

from app.storage.csv_storage import CSVStorage
from app.service.metadata_manager import MetadataManager
from app.service.sync_manager import SyncManager
from app.service.task_planner import TaskPlanner
from app.provider.provider_manager import ProviderManager

def test_metadata_and_sync_flow():
    test_root = Path("test_sync_root")
    if test_root.exists():
        shutil.rmtree(test_root)
    
    # 1. 初始化组件
    storage = CSVStorage(storage_root=test_root / "csv")
    metadata_mgr = MetadataManager(storage_root=test_root)
    planner = TaskPlanner(metadata_mgr=metadata_mgr)
    sync_mgr = SyncManager(storage=storage, metadata_mgr=metadata_mgr, planner=planner)
    
    table_id = "test.kline.1d.mock"
    symbol_a = "sh.600000"
    symbol_b = "sz.000001"
    
    # 2. 模拟注入数据 (2024年 2 条, 2025年 1 条)
    df1 = pl.DataFrame({
        "timestamp": [1704067200000, 1704153600000], # 2024-01-01, 01-02
        "datetime": ["2024-01-01T00:00:00.000", "2024-01-02T00:00:00.000"],
        "symbol": [symbol_a] * 2,
        "close": [10.0, 10.1]
    })
    
    df2 = pl.DataFrame({
        "timestamp": [1735689600000], # 2025-01-01
        "datetime": ["2025-01-01T00:00:00.000"],
        "symbol": [symbol_b],
        "close": [20.0]
    })
    
    storage.write(table_id, df1)
    storage.write(table_id, df2)
    
    print("--- 开始 Storage 元数据统计方法测试 ---")
    
    # 验证 Storage 物理统计接口
    symbols = storage.get_all_symbols(table_id)
    assert len(symbols) == 2, f"应包含 2 个 symbol, 实际得到 {len(symbols)}"
    assert symbol_a in symbols
    assert symbol_b in symbols
    
    total_bars = storage.get_total_bars(table_id)
    assert total_bars == 3, f"总行数应为 3, 实际为 {total_bars}"
    
    t_min, t_max = storage.get_global_time_range(table_id)
    assert t_min == 1704067200000
    assert t_max == 1735689600000
    
    unique_ts = storage.get_unique_timestamps(table_id)
    assert len(unique_ts) == 3, "唯一时间点个数应为 3"
    assert unique_ts == sorted([1704067200000, 1704153600000, 1735689600000])
    
    print("[OK] Storage 统计方法验证通过")
    
    print("--- 开始 SyncManager 闭环测试 ---")
    
    # 3. 执行同步闭环 (统计磁盘状态并生成元数据)
    # 由于目前 sync 方法固定执行驱动采集，我们通过 mock 掉 provider 避免真实网络请求
    from unittest.mock import patch
    with patch.object(sync_mgr.provider_mgr, 'get_provider'):
        sync_mgr.sync(table_id, "csv", [symbol_a, symbol_b], "2024-01-01", "2025-01-01")
    
    # 验证元数据文件
    meta_path = test_root / "csv" / table_id / "metadata.json"
    assert meta_path.exists()
    
    with open(meta_path, "r", encoding="utf-8") as f:
        meta = json.load(f)
        
    assert meta["table_id"] == table_id
    assert meta["global_stats"]["total_bars"] == 3
    assert meta["global_stats"]["symbol_count"] == 2
    assert meta["global_stats"]["time_steps"] == 3
    assert "schema" in meta
    assert meta["schema"]["timestamp"] == "Int64"
    
    print("[OK] SyncManager 及 MetadataManager 验证通过")
    
    # 清理
    # shutil.rmtree(test_root)

if __name__ == "__main__":
    try:
        test_metadata_and_sync_flow()
        print("\n所有功能测试均已通过！")
    except Exception as e:
        print(f"\n测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
