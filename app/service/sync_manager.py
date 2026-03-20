import polars as pl
from datetime import datetime, timezone, timedelta
from typing import Any
from ..storage.base import StorageManager
from ..storage.metadata_manager import MetadataManager
from ..utils.time_utils import ts_to_iso

class SyncManager:
    """
    同步管理器：协调数据下载与元数据更新的闭环流水线。
    执行流：同步数据 -> Storage 元数据更新 -> 生成 metadata.json。
    """

    def __init__(self, storage: Any, metadata_mgr: MetadataManager):
        # 注入具名 Storage 实现以获得元数据统计能力
        self.storage = storage
        self.metadata_mgr = metadata_mgr

    def sync_table(self, table_id: str, format: str, provider: Any = None):
        """
        同步一个完整的数据集并闭环元数据更新。
        """
        print(f"[*] Starting sync for {table_id}...")
        
        # 1. 执行物理同步逻辑 (省略 Provider 下载过程)
        # 2. 核心步骤：Storage 元数据更新 (Metadata Stats Update)
        all_symbols = self.storage.get_all_symbols(table_id)
        total_bars = self.storage.get_total_bars(table_id)
        start_ts, end_ts = self.storage.get_global_time_range(table_id)
        unique_tss = self.storage.get_unique_timestamps(table_id)
        
        # 3. 构建 Schema (读取任意一个文件获取结构即可)
        schema_dict = {}
        table_dir = self.storage.storage_root / table_id
        all_csvs = list(table_dir.glob("year=*/*.csv"))
        if all_csvs:
            # 极速读取 Schema
            schema = pl.read_csv(all_csvs[0], n_rows=0).schema
            schema_dict = {k: str(v) for k, v in schema.items()}

        # 4. 构建最新的 Metadata 字典 (Finalize)
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
