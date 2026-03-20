from typing import List, Dict, Any
from ..storage.metadata_manager import MetadataManager
from ..utils.time_utils import parse_date_to_ts

class TaskPlanner:
    """
    任务规划器：负责计算下载补丁。
    查询本地元数据，对比用户请求，剔除已下载部分。
    """

    def __init__(self, metadata_mgr: MetadataManager):
        self.metadata_mgr = metadata_mgr

    def plan(self, table_id: str, format: str, symbols: List[str], start_date: Any, end_date: Any) -> List[Dict[str, Any]]:
        """
        根据元数据计算计划任务列表。
        
        Args:
            table_id: 数据表 ID
            format: 数据格式 (csv/parquet)
            symbols: 目标证券代码列表
            start_date: 请求起始时间 (str 或 ts)
            end_date: 请求结束时间 (str 或 ts)
            
        Returns:
            List[Dict]: 补丁任务列表，每项含 symbol, start, end
        """
        metadata = self.metadata_mgr.load(table_id, format)
        global_stats = metadata.get("global_stats", {})
        # 获取全局最后同步的时间戳
        global_last_ts = global_stats.get("end_timestamp", 0)
        
        # 解析请求区间
        req_start_ts = parse_date_to_ts(start_date)
        req_end_ts = parse_date_to_ts(end_date)
        
        planned_tasks = []
        
        # 针对每个 symbol 计算增量区间
        for symbol in symbols:
            # 优先从 symbols 级别的元数据中获取 (若未来支持分 symbol 统计)
            symbol_last_ts = global_last_ts 
            
            # 计算实际起始点
            # 起始点逻辑：从本地终点开始，重复数据由存储层 unique(timestamp) 自动去重。
            effective_start = req_start_ts
            if symbol_last_ts > 0 and req_start_ts < symbol_last_ts:
                effective_start = symbol_last_ts
            
            # 如果起始点已经超过了请求结束点，说明无需下载
            if effective_start >= req_end_ts:
                # 检查是否真的已经覆盖
                if symbol_last_ts >= req_end_ts:
                    continue
                
            planned_tasks.append({
                "symbol": symbol,
                "start": effective_start,
                "end": req_end_ts
            })
            
        return planned_tasks
