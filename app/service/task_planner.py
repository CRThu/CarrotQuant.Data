from typing import List, Dict, Any
from .metadata_manager import MetadataManager
from ..utils.time_utils import parse_date_to_ts, align_to_day_start, align_to_day_end

class TaskPlanner:
    """
    任务规划器：负责计算下载补丁。
    查询本地元数据，对比用户请求，剔除已下载部分。
    """

    def __init__(self, metadata_mgr: MetadataManager):
        self.metadata_mgr = metadata_mgr

    def plan(self, table_id: str, formats: List[str], symbols: List[str], start_date: Any = None, end_date: Any = None, force_refresh: bool = False) -> List[Dict[str, Any]]:
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
            List[Dict]: 补丁任务列表，每项含 symbol, start, end。每个 symbol 最多 2 个任务（双向穿透时各一个）。
        """
        # 空格式列表直接返回空任务
        if not formats:
            return []
        
        # 1. 聚合多个格式的水位线。
        # 起始取 max, 结束取 min：这是为了找到所有格式共同拥有的“最窄”水位区间。
        # 只要有一路格式缺失，我们就需要通过任务补齐。
        loc_start = None
        loc_end = None
        
        for fmt in formats:
            metadata = self.metadata_mgr.load(table_id, fmt)
            statistics = metadata.get("statistics", {})
            f_start = statistics.get("start_timestamp", 0)
            f_end = statistics.get("end_timestamp", 0)
            
            if f_start > 0:
                loc_start = max(loc_start, f_start) if loc_start is not None else f_start
            if f_end > 0:
                loc_end = min(loc_end, f_end) if loc_end is not None else f_end

        # 2. 默认日期推导
        if start_date is None:
            if loc_end and loc_end > 0:
                # 如果有本地水位，从本地结束时间开始（增量）
                req_start = loc_end
            else:
                raise ValueError(f"No local metadata for {table_id}. You MUST provide 'start_date' for the first sync.")
        else:
            req_start = parse_date_to_ts(start_date)

        # 3. 结束日期处理 (默认为 None，由 Provider 处理或此处设为极其遥远的未来/现在)
        # 如果 end_date 为空，通常意味着同步到最新，暂设为一个极大值或由具体处理逻辑转换
        if end_date is None:
            import time
            req_end = int(time.time() * 1000)
        else:
            req_end = parse_date_to_ts(end_date)
            
        planned_tasks = []
        
        # 初始水位修正
        loc_start = loc_start or 0
        loc_end = loc_end or 0

        for symbol in symbols:
            # 强制刷新或本地无数据：直接覆盖全量
            if force_refresh or loc_start == 0 or loc_end == 0:
                if req_start <= req_end:
                    planned_tasks.append({
                        "symbol": symbol,
                        "start": req_start,
                        "end": req_end
                    })
                continue

            # 前向补全与后向拓展独立判断，支持双向穿透同时生成两个精准任务
            # 前向补全：填补 [req_start, loc_start) 的缺口，不与后向任务重叠
            if req_start < loc_start:
                task_start = req_start
                task_end = min(req_end, loc_start)
                if task_start <= task_end:
                    planned_tasks.append({
                        "symbol": symbol,
                        "start": task_start,
                        "end": align_to_day_end(task_end)
                    })

            # 后向拓展：从 loc_end 开始补齐未来数据
            # 对齐到整天：start 向下取整，end 向上取整，确保 Provider 按天拉取时不会丢数据
            if req_end >= loc_end:
                task_start = align_to_day_start(loc_end)
                task_end = align_to_day_end(req_end)
                if task_start < task_end:
                    planned_tasks.append({
                        "symbol": symbol,
                        "start": task_start,
                        "end": task_end
                    })
            
        return planned_tasks
