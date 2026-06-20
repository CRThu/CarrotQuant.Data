from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from typing import List, Optional
from pydantic import BaseModel
from loguru import logger
from pathlib import Path
import glob
import polars as pl

from ..service.sync_manager import SyncManager
from ..provider.provider_manager import ProviderManager
from ..config.settings import settings
from ..storage.storage_factory import StorageFactory
from ..utils.time_utils import parse_date_to_ts

app = FastAPI(title="CarrotQuant.Data API", version="1.0.0")

# 全局任务锁集合，防止同一 table_id 并发写入
ACTIVE_SYNC_TASKS = set()

class SyncRequest(BaseModel):
    table_ids: List[str]
    formats: List[str] = ["csv"]
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    force_refresh: bool = False
    batch_size: int = 100

def run_sync_task(table_id: str, formats: List[str], start_date: Optional[str], end_date: Optional[str], force_refresh: bool, batch_size: int):
    """
    后台同步任务执行器
    """
    try:
        sync_mgr = SyncManager()
        sync_mgr.sync(
            table_ids=[table_id],
            formats=formats,
            start_date=start_date,
            end_date=end_date,
            force_refresh=force_refresh,
            batch_size=batch_size
        )
        logger.info(f"[API] Background sync finished for {table_id}")
    except Exception as e:
        logger.error(f"[API] Background sync failed for {table_id}: {e}")
    finally:
        ACTIVE_SYNC_TASKS.remove(table_id)
        logger.info(f"[API] Task lock released for {table_id}")

@app.post("/api/v1/sync")
async def sync_data(request: SyncRequest, background_tasks: BackgroundTasks):
    """
    异步触发数据同步任务。
    具备并发防御机制，防止同一表同时同步。
    """
    processing_tables = []
    locked_tables = []

    for table_id in request.table_ids:
        if table_id in ACTIVE_SYNC_TASKS:
            locked_tables.append(table_id)
        else:
            ACTIVE_SYNC_TASKS.add(table_id)
            processing_tables.append(table_id)

    if locked_tables and not processing_tables:
        raise HTTPException(status_code=409, detail=f"Tasks already running for tables: {locked_tables}")

    for table_id in processing_tables:
        background_tasks.add_task(
            run_sync_task,
            table_id,
            request.formats,
            request.start_date,
            request.end_date,
            request.force_refresh,
            request.batch_size
        )

    return {
        "status": "accepted",
        "started_tasks": processing_tables,
        "ignored_tasks": locked_tables,
        "message": "Sync tasks started in background."
    }

@app.get("/api/v1/tasks")
async def get_active_tasks():
    """
    获取当前正在运行的同步任务
    """
    return {"active_tasks": list(ACTIVE_SYNC_TASKS)}

@app.get("/api/v1/data/{table_id}")
async def get_data(
    table_id: str,
    filters: Optional[str] = None,
    format: str = "csv"
):
    """
    查询标准化后的数据。
    
    参数:
        table_id: 表标识符
        filters: 通用过滤条件，格式 key:value，多个用逗号分隔
                  - symbol:sh.600000    按证券代码过滤 (TS 必填)
                  - board_code:BK1717   按板块代码过滤
                  - start:2024-01-01    起始日期
                  - end:2024-12-31      结束日期
                  例: "symbol:sh.600000,start:2024-01-01,end:2024-12-31"
        format: 存储格式 csv/parquet
    """
    # 1. 解析 filters 参数
    filter_dict = {}
    if filters:
        for pair in filters.split(","):
            if ":" in pair:
                k, v = pair.split(":", 1)
                filter_dict[k.strip()] = v.strip()

    try:
        storage = StorageFactory.get_storage(format, settings.STORAGE_ROOT)
        provider = ProviderManager().get_provider(table_id)
        category = provider.get_table_category(table_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    # 2. TS 必须提供 symbol
    if category == "timeseries" and "symbol" not in filter_dict:
        raise HTTPException(status_code=400, detail="TS data query requires 'symbol' in filters.")

    # 3. 提取时间参数
    start_date = filter_dict.pop("start", None)
    end_date = filter_dict.pop("end", None)
    start_ts = parse_date_to_ts(start_date) if start_date else None
    end_ts = parse_date_to_ts(end_date) if end_date else None

    # 4. 定位可用年份
    table_path = Path(settings.STORAGE_ROOT) / format / table_id
    flat_file = table_path / "data.csv" if format == "csv" else table_path / "data.parquet"
    year_dirs = sorted(glob.glob(str(table_path / "year=*")))
    
    if not year_dirs and not flat_file.exists():
        return {"table_id": table_id, "format": format, "data": [], "message": "No data files found."}

    years = [int(p.split("=")[-1]) for p in year_dirs]
    
    # 缩小年份扫描范围
    if start_date:
        import datetime
        start_year = datetime.datetime.fromtimestamp(start_ts/1000, datetime.timezone.utc).year
        years = [y for y in years if y >= start_year]
    if end_date:
        import datetime
        end_year = datetime.datetime.fromtimestamp(end_ts/1000, datetime.timezone.utc).year
        years = [y for y in years if y <= end_year]

    all_dfs = []
    
    try:
        if category == "timeseries":
            # TS 逻辑：直接按 symbol 查询
            symbol = filter_dict.pop("symbol")
            for year in years:
                df = storage.read_series(table_id, symbol, year)
                if not df.is_empty():
                    all_dfs.append(df)
        elif flat_file.exists():
            # 平铺 EV 逻辑：直接读取单文件
            df = storage.read_event(table_id)
            if not df.is_empty():
                all_dfs.append(df)
        else:
            # Hive 分区 EV 逻辑：读取全量
            for year in years:
                df = storage.read_event(table_id, year)
                if not df.is_empty():
                    all_dfs.append(df)

        if not all_dfs:
            return {"table_id": table_id, "format": format, "data": [], "message": "No records match criteria."}

        final_df = pl.concat(all_dfs)
        
        # 5. 应用字段过滤条件
        for key, value in filter_dict.items():
            if key in final_df.columns:
                final_df = final_df.filter(pl.col(key) == value)
        
        # 6. 应用时间过滤（仅在有 timestamp 列时）
        if "timestamp" in final_df.columns:
            if start_ts is not None:
                final_df = final_df.filter(pl.col("timestamp") >= start_ts)
            if end_ts is not None:
                final_df = final_df.filter(pl.col("timestamp") <= end_ts)
            
        # 7. 排序与限制
        if "timestamp" in final_df.columns:
            final_df = final_df.sort("timestamp")
        final_df = final_df.limit(1000)
        
        return {
            "table_id": table_id,
            "category": category,
            "format": format,
            "count": final_df.height,
            "data": final_df.to_dicts(),
            "message": "Success"
        }
    except Exception as e:
        logger.error(f"Query Error: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
