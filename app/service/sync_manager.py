from loguru import logger
import polars as pl
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

    def sync(self, table_id: str, format: str, start_date: str, end_date: str, force_refresh: bool = False, batch_size: int = 100):
        """
        执行全自动化同步闭环。
        重构为“批处理聚合模式”，降低 Parquet 写放大。
        """
        
        # 动态获取对应的物理存储引擎
        storage = StorageFactory.get_storage(format, self.storage_root)
        
        logger.info(f"[*] Starting orchestrated sync for {table_id} (batch_size={batch_size}, force_refresh={force_refresh})...")
        
        # 1. 获取驱动
        provider = self.provider_mgr.get_provider(table_id)
        
        # 2. 自动发现全量代码
        symbols = provider.get_all_symbols(table_id)
        
        # 3. 规划补丁
        tasks = self.planner.plan(table_id, format, symbols, start_date, end_date, force_refresh=force_refresh)
        total_tasks = len(tasks)
        logger.info(f"[*] Task planning finished. {total_tasks}/{len(symbols)} symbols need to be patched.")
        
        # 4. 执行采集与落地循环
        last_success_df = None
        success_count = 0
        fail_count = 0
        
        # 将任务切分为批次
        for batch_idx in range(0, total_tasks, batch_size):
            batch_tasks = tasks[batch_idx : batch_idx + batch_size]
            batch_dfs = []
            
            logger.info(f"[BATCH] Processing batch {batch_idx//batch_size + 1} ({len(batch_tasks)} symbols)")
            
            for j, task in enumerate(batch_tasks):
                symbol = task['symbol']
                start_ts = task['start']
                end_ts = task['end']
                
                # 实时进度 Log
                current_idx = batch_idx + j + 1
                logger.info(f"[PROGRESS] {table_id} | {current_idx}/{total_tasks} | {symbol}")
                
                try:
                    # Step 1: Provider 采集标准化数据
                    df = provider.fetch(table_id, symbol, start_ts, end_ts)
                    
                    # 采样 Schema：即使没数据，Provider 返回的空表通常也带有字段类型
                    if last_success_df is None:
                        last_success_df = df
                    
                    if not df.is_empty():
                        batch_dfs.append(df)
                        last_success_df = df # 优先保留有数据的样本
                        logger.debug(f"[Sync] {symbol} 下载成功 ({len(df)} rows)")
                    else:
                        logger.debug(f"[Sync] {symbol} (No new data)")
                    
                    success_count += 1
                except Exception as e:
                    logger.error(f"[Sync] {symbol} 失败: {e}")
                    fail_count += 1
            
            # Step 2: 批次内存聚合与单次下沉
            if batch_dfs:
                big_df = pl.concat(batch_dfs)
                storage.write(table_id, big_df, mode="append")
                logger.info(f"[BATCH] Aggregated {len(batch_dfs)} symbols, total {len(big_df)} rows written.")
                
                # 显式释放内存列表
                batch_dfs.clear()
                del big_df
        
        # 5. 物理巡检 (The Truth)
        logger.info(f"[*] Running physical data inspection for {table_id}...")
        all_symbols = storage.get_all_symbols(table_id)
        total_bars = storage.get_total_bars(table_id)
        start_ts, end_ts = storage.get_global_time_range(table_id)
        unique_tss = storage.get_unique_timestamps(table_id)
        
        # 5. 元数据盖章 (Metadata Stamp)
        # 获取 Schema: 仅从最近一次成功下载的数据中提取，或保留已有 Schema
        old_metadata = self.metadata_mgr.load(table_id, format)
        schema_dict = old_metadata.get("schema", {})
        
        if last_success_df is not None:
            schema_dict = {k: str(v) for k, v in last_success_df.schema.items()}
            
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

        # 6. 原子化保存元数据
        self.metadata_mgr.save(table_id, format, metadata)
        logger.info(f"[+] Sync finished. Global total bars: {total_bars}")
        logger.info(f"[SUMMARY] Total: {total_tasks} | Success: {success_count} | Fail: {fail_count}")
