import polars as pl
from typing import List, Any
from .task_planner import TaskPlanner
from ..provider.manager import ProviderManager
from ..storage.metadata_manager import MetadataManager
from ..utils.time_utils import ts_to_iso

class SyncManager:
    """
    同步总管：串联规划、采集、落地、巡检、盖章整个流程。
    """

    def __init__(self, storage: Any, metadata_mgr: MetadataManager, planner: TaskPlanner):
        self.storage = storage
        self.metadata_mgr = metadata_mgr
        self.planner = planner
        self.provider_mgr = ProviderManager()

    def sync(self, table_id: str, format: str, symbols: List[str], start_date: str, end_date: str):
        """
        执行全自动化同步闭环。
        """
        print(f"[*] Starting orchestrated sync for {table_id}...")
        
        # 1. 规划补丁 (Logic Implementation 2.2)
        tasks = self.planner.plan(table_id, format, symbols, start_date, end_date)
        total_tasks = len(tasks)
        print(f"[*] Task planning finished. {total_tasks} symbols need to be patched.")
        
        # 2. 获取驱动 (Logic Implementation 2.3 Provider 插件化)
        provider = self.provider_mgr.get_provider(table_id)
        
        # 3. 执行采集与落地循环
        last_success_df = None
        for i, task in enumerate(tasks):
            symbol = task['symbol']
            start_ts = task['start']
            end_ts = task['end']
            
            # Step 1: Provider 采集标准化数据 (含 timestamp/datetime)
            df = provider.fetch(table_id, symbol, start_ts, end_ts)
            
            # Step 2: Storage 统一 Upsert 落地 (Logic Implementation 2.3)
            if not df.is_empty():
                self.storage.write(table_id, df, mode="append")
                last_success_df = df
                print(f"[Sync] {symbol} ({ts_to_iso(start_ts)} -> {ts_to_iso(end_ts)}) 写入成功, 累计 {i+1}/{total_tasks}")
            else:
                print(f"[Sync] {symbol} (No new data), 累计 {i+1}/{total_tasks}")
                
        # 4. 物理巡检 (The Truth)
        print(f"[*] Running physical data inspection for {table_id}...")
        all_symbols = self.storage.get_all_symbols(table_id)
        total_bars = self.storage.get_total_bars(table_id)
        start_ts, end_ts = self.storage.get_global_time_range(table_id)
        unique_tss = self.storage.get_unique_timestamps(table_id)
        
        # 5. 元数据盖章 (Metadata Stamp)
        # 获取 Schema: 优先从下载的数据中获取，若没下载则从已有元数据继承或物理扫描
        old_metadata = self.metadata_mgr.load(table_id, format)
        schema_dict = old_metadata.get("schema", {})
        
        if last_success_df is not None:
            schema_dict = {k: str(v) for k, v in last_success_df.schema.items()}
        elif not schema_dict and all_symbols:
            # 实在没有，尝试从磁盘读一个
            start_iso = ts_to_iso(start_ts)
            if start_iso:
                year = int(start_iso[:4])
                test_df = self.storage.read(table_id, all_symbols[0], year)
                if not test_df.is_empty():
                    schema_dict = {k: str(v) for k, v in test_df.schema.items()}
            
        metadata = {
            "table_id": table_id,
            "category": self.storage.category,
            "format": format,
            "schema": schema_dict,
            "global_stats": {
                "start_timestamp": start_ts,
                "end_timestamp": end_ts,
                "start_datetime": ts_to_iso(start_ts),
                "end_datetime": ts_to_iso(end_ts),
                "time_steps": len(unique_tss),
                "symbol_count": len(all_symbols),
                "total_bars": total_bars
            }
        }

        # 5. 原子化保存元数据 (Stamp)
        self.metadata_mgr.save(table_id, format, metadata)
        print(f"[+] Sync finished. Global total bars: {total_bars}")
