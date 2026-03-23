from loguru import logger
from .task_planner import TaskPlanner
from ..provider.provider_manager import ProviderManager
from .metadata_manager import MetadataManager
from ..utils.time_utils import ts_to_iso
from ..storage.storage_factory import StorageFactory
from ..config.settings import settings

class SyncManager:
    """
    同步总管：串联规划、采集、落地、巡检、盖章整个流程。
    """

    def __init__(self):
        # 内部自治实例化：基于配置中心
        self.storage_root = settings.STORAGE_ROOT
        self.metadata_mgr = MetadataManager(self.storage_root)
        self.planner = TaskPlanner(self.metadata_mgr)
        self.provider_mgr = ProviderManager()

    def sync(self, table_id: str, format: str, start_date: str, end_date: str, force_refresh: bool = False):
        """
        执行全自动化同步闭环。
        """
        # 动态获取对应的物理存储引擎
        storage = StorageFactory.get_storage(format, self.storage_root)
        
        logger.info(f"[*] Starting orchestrated sync for {table_id} (force_refresh={force_refresh})...")
        
        # 1. 获取驱动 (Logic Implementation 2.3 Provider 插件化)
        provider = self.provider_mgr.get_provider(table_id)
        
        # 2. 自动发现全量代码 (New Discovery Logic)
        symbols = provider.get_all_symbols(table_id)
        
        # 3. 规划补丁 (Logic Implementation 2.2)
        tasks = self.planner.plan(table_id, format, symbols, start_date, end_date, force_refresh=force_refresh)
        total_tasks = len(tasks)
        logger.info(f"[*] Task planning finished. {total_tasks}/{len(symbols)} symbols need to be patched.")
        
        # 4. 执行采集与落地循环
        last_success_df = None
        success_count = 0
        fail_count = 0
        
        for i, task in enumerate(tasks):
            symbol = task['symbol']
            start_ts = task['start']
            end_ts = task['end']
            
            # 进度记录
            logger.info(f"[PROGRESS] {table_id} | {i+1}/{total_tasks} | {symbol}")
            
            try:
                # Step 1: Provider 采集标准化数据 (含 timestamp/datetime)
                df = provider.fetch(table_id, symbol, start_ts, end_ts)
                
                # Step 2: Storage 统一 Upsert 落地 (Logic Implementation 2.3)
                if not df.is_empty():
                    storage.write(table_id, df, mode="append")
                    last_success_df = df
                    logger.debug(f"[Sync] {symbol} ({ts_to_iso(start_ts)} -> {ts_to_iso(end_ts)}) 写入成功")
                else:
                    logger.debug(f"[Sync] {symbol} (No new data)")
                
                success_count += 1
            except Exception as e:
                logger.error(f"[Sync] {symbol} 失败: {e}")
                fail_count += 1
                
        # 5. 物理巡检 (The Truth)
        logger.info(f"[*] Running physical data inspection for {table_id}...")
        all_symbols = storage.get_all_symbols(table_id)
        total_bars = storage.get_total_bars(table_id)
        start_ts, end_ts = storage.get_global_time_range(table_id)
        unique_tss = storage.get_unique_timestamps(table_id)
        
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
                test_df = storage.read(table_id, all_symbols[0], year)
                if not test_df.is_empty():
                    schema_dict = {k: str(v) for k, v in test_df.schema.items()}
            
        metadata = {
            "table_id": table_id,
            "category": storage.category,
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

        # 6. 原子化保存元数据 (Stamp)
        self.metadata_mgr.save(table_id, format, metadata)
        logger.info(f"[+] Sync finished. Global total bars: {total_bars}")
        logger.info(f"[SUMMARY] Total: {total_tasks} | Success: {success_count} | Fail: {fail_count}")
