from loguru import logger
import polars as pl
from .task_planner import TaskPlanner
from ..provider.provider_manager import ProviderManager
from .metadata_manager import MetadataManager
from ..utils.time_utils import ts_to_iso
from ..storage.storage_factory import StorageFactory
from ..config.settings import settings
from typing import Any

class SyncManager:
    """
    同步总管：串联规划、采集、落地、巡检、盖章整个流程。
    支持单次抓取、多格式并行落地。
    """

    def __init__(self):
        # 内部自治实例化：基于配置中心
        self.storage_root = settings.STORAGE_ROOT
        self.metadata_mgr = MetadataManager(self.storage_root)
        self.planner = TaskPlanner(self.metadata_mgr)
        self.provider_mgr = ProviderManager()

    def sync(self, table_ids: list[str] | str, formats: list[str] | str, start_date: str = None, end_date: str = None, force_refresh: bool = False, batch_size: int = 100):
        """
        执行全自动化同步闭环。
        支持多表、多格式列表传入。
        """
        if isinstance(table_ids, str):
            table_ids = [table_ids]
        if isinstance(formats, str):
            formats = [formats]

        logger.info(f"[*] Starting orchestrated sync for {table_ids} into {formats} (batch_size={batch_size}, force_refresh={force_refresh})...")

        for table_id in table_ids:
            self._sync_single_table(table_id, formats, start_date, end_date, force_refresh, batch_size)

    def _sync_single_table(self, table_id: str, formats: list[str], start_date: str, end_date: str, force_refresh: bool, batch_size: int):
        """
        单表同步逻辑：一次拉取，多处落地。
        """
        # 1. 获取驱动
        provider = self.provider_mgr.get_provider(table_id)
        
        # 2. 获取所有目标存储引擎
        storages = {fmt: StorageFactory.get_storage(fmt, self.storage_root) for fmt in formats}
        
        # 3. 自动发现全量代码
        symbols = provider.get_all_symbols(table_id)
        
        # 4. 规划补丁
        tasks = self.planner.plan(table_id, formats, symbols, start_date, end_date, force_refresh=force_refresh)
        total_tasks = len(tasks)
        logger.info(f"[*] Task planning finished for {table_id}. {total_tasks}/{len(symbols)} symbols need to be patched.")
        
        # 5. 执行采集与落地循环
        last_success_df = None
        success_count = 0
        data_written = False
        
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
                    
                    if last_success_df is None or not df.is_empty():
                        last_success_df = df
                    
                    if not df.is_empty():
                        batch_dfs.append(df)
                        logger.debug(f"[Sync] {symbol} 下载成功 ({len(df)} rows)")
                    else:
                        logger.debug(f"[Sync] {symbol} (No new data)")
                    
                    success_count += 1
                except Exception as e:
                    # 失败策略：立即抛出异常，停止任务
                    logger.error(f"[Sync] {symbol} 失败，执行 Fail-Fast 策略: {e}")
                    raise e
            
            # Step 2: 批次内存聚合与多路下沉
            if batch_dfs:
                big_df = pl.concat(batch_dfs)
                for fmt, storage in storages.items():
                    logger.debug(f"[BATCH] Writing to storage: {fmt}")
                    storage.write(table_id, big_df, mode="append")
                
                data_written = True
                logger.info(f"[BATCH] Aggregated {len(batch_dfs)} symbols, total {len(big_df)} rows written to {formats}.")
                batch_dfs.clear()
                del big_df
        
        # 6. 巡检与元数据更新 (针对每个格式独立更新)
        for fmt, storage in storages.items():
            self._update_metadata(table_id, fmt, storage, last_success_df, data_written, force_refresh)

        logger.info(f"[+] Sync finished for {table_id}. Success: {success_count}/{total_tasks}")

    def _update_metadata(self, table_id: str, format: str, storage: Any, last_success_df: pl.DataFrame, data_written: bool, force_refresh: bool):
        """
        执行物理巡检并更新元数据
        """
        logger.info(f"[*] Updating metadata for {table_id} ({format})...")
        all_symbols = storage.get_all_symbols(table_id)
        total_bars = storage.get_total_bars(table_id)
        start_ts, end_ts = storage.get_global_time_range(table_id)
        unique_tss = storage.get_unique_timestamps(table_id)
        
        # 5. 元数据盖章 (Metadata Stamp)
        old_metadata = self.metadata_mgr.load(table_id, format)
        
        # 如果物理巡检结果为 0，且无元数据或已存在元数据时，执行静默拦截逻辑
        if total_bars == 0:
            if not old_metadata:
                # 场景 A（初次同步）：若本地无 metadata.json，直接记录 logger.warning 并 return
                logger.warning(f"[!] No data found for {table_id} | {format} on disk. Skipping metadata creation.")
                return
            else:
                # 场景 B（增量同步）：若本地已有元数据，直接 return，不更新现有 JSON 文件
                logger.debug(f"[*] Total bars is 0, but metadata already exists for {table_id} | {format}. Keeping existing metadata.")
                return

        # 物理库存变更判定：仅在数据有新增、强制刷新或元数据不存在时才落盘
        if not data_written and old_metadata and not force_refresh:
            logger.debug(f"[*] No new data written and metadata exists for {table_id} | {format}. Skipping metadata update.")
            return

        # 仅当 total_bars > 0 且 (有新数据 或 首次生成) 时才更新 Schema 并保存元数据
        schema_dict = old_metadata.get("schema", {}) if old_metadata else {}
        
        if last_success_df is not None:
            schema_dict = {k: str(v) for k, v in last_success_df.schema.items()}
        
        # 根据 category 分类构建统计结构
        # category == "TS" (TimeSeries): 包含 time_steps 和 symbol_count
        # category == "EV" (Event): 仅包含 total_bars
        category = storage.category
        if category == "TS":
            statistics = {
                "start_timestamp": start_ts,
                "end_timestamp": end_ts,
                "start_datetime": ts_to_iso(start_ts),
                "end_datetime": ts_to_iso(end_ts),
                "total_bars": total_bars,
                "symbol_count": len(all_symbols),
                "time_steps": len(unique_tss)
            }
        elif category == "EV":
            statistics = {
                "start_timestamp": start_ts,
                "end_timestamp": end_ts,
                "start_datetime": ts_to_iso(start_ts),
                "end_datetime": ts_to_iso(end_ts),
                "total_bars": total_bars
            }
        else:
            # 兼容其他未预期的 category
            statistics = {
                "start_timestamp": start_ts,
                "end_timestamp": end_ts,
                "start_datetime": ts_to_iso(start_ts),
                "end_datetime": ts_to_iso(end_ts),
                "total_bars": total_bars
            }
        
        metadata = {
            "table_id": table_id,
            "category": category,
            "format": format,
            "schema": schema_dict,
            "statistics": statistics
        }

        # 6. 原子化保存元数据
        self.metadata_mgr.save(table_id, format, metadata)
        logger.info(f"[+] Metadata updated for {table_id} | {format}")
