from typing import List, Dict, Any
from .metadata_manager import MetadataManager
from ..utils.time_utils import parse_date_to_ts

class TaskPlanner:
    """
    任务规划器：负责计算下载补丁。
    查询本地元数据，对比用户请求，剔除已下载部分。
    """

    def __init__(self, metadata_mgr: MetadataManager):
        self.metadata_mgr = metadata_mgr

    def plan(self, table_id: str, format: str, symbols: List[str], start_date: Any, end_date: Any, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """
        根据元数据计算计划任务列表。实现向前补全、向后拓展或强制全量更新。
        
        Args:
            table_id: 数据表 ID
            format: 数据格式 (csv/parquet)
            symbols: 目标证券代码列表
            start_date: 请求起始时间 (str 或 ts)
            end_date: 请求结束时间 (str 或 ts)
            force_refresh: 是否强制全量刷新 (忽略本地水位线)
            
        Returns:
            List[Dict]: 补丁任务列表，每项含 symbol, start, end。每个 symbol 最多 1 个任务。
        """
        metadata = self.metadata_mgr.load(table_id, format)
        global_stats = metadata.get("global_stats", {})
        
        # 水位线：loc_start 到 loc_end
        loc_start = global_stats.get("start_timestamp", 0)
        loc_end = global_stats.get("end_timestamp", 0)
        
        # 解析请求区间
        req_start = parse_date_to_ts(start_date)
        req_end = parse_date_to_ts(end_date)
        
        planned_tasks = []
        
        for symbol in symbols:
            # 1. 强制刷新或本地无数据：直接覆盖全量
            if force_refresh or loc_start == 0 or loc_end == 0:
                task_start, task_end = req_start, req_end
            
            # 2. 前向补全 (req_start < loc_start)
            elif req_start < loc_start:
                # 任务终点取 req_end 和 loc_start 的最大值，确保至少覆盖之前缺失部分并衔接旧数据
                task_start = req_start
                task_end = max(req_end, loc_start)
            
            # 3. 后向拓展 (req_end > loc_end)
            elif req_end > loc_end:
                # 任务起点取 req_start 和 loc_end 的最小值，确保从旧数据终点开始延伸
                task_start = min(req_start, loc_end)
                task_end = req_end
                
            # 4. 请求范围已被本地覆盖且非强制刷新：跳过
            else:
                continue
                
            # 安全检查：如果计算出的任务区间无效（起点 >= 终点），跳过
            if task_start >= task_end:
                continue
                
            planned_tasks.append({
                "symbol": symbol,
                "start": task_start,
                "end": task_end
            })
            
        return planned_tasks
