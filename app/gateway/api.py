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
    symbol: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    format: str = "csv"
):
    """
    查询标准化后的数据。
    TS 模式：必须提供 symbol，基于 [Symbol, Year] 物理路径极速定位。
    EV 模式：返回全年事件流，支持在内存中按 symbol 灵活过滤。
    """
    try:
        storage = StorageFactory.get_storage(format, settings.STORAGE_ROOT)
        provider = ProviderManager().get_provider(table_id)
        category = provider.get_table_category(table_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    # 1. 基础参数校验
    if category == "TS" and not symbol:
        raise HTTPException(status_code=400, detail="TS data query requires 'symbol' parameter.")

    # 2. 定位可用年份
    table_path = Path(settings.STORAGE_ROOT) / format / table_id
    year_dirs = sorted(glob.glob(str(table_path / "year=*")))
    if not year_dirs:
        return {"table_id": table_id, "format": format, "data": [], "message": "No data files found."}

    years = [int(p.split("=")[-1]) for p in year_dirs]
    
    # 3. 构造查询范围
    start_ts = parse_date_to_ts(start_date) if start_date else None
    end_ts = parse_date_to_ts(end_date) if end_date else None
    
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
        if category == "TS":
            # TS 逻辑：直接查询
            for year in years:
                df = storage.read_series(table_id, symbol, year)
                if not df.is_empty():
                    all_dfs.append(df)
        else:
            # EV 逻辑：读取全量并可选过滤
            for year in years:
                df = storage.read_event(table_id, year)
                if not df.is_empty():
                    if symbol:
                        # 内存过滤：若用户在 API 请求中传入了 symbol 参数
                        if "symbol" in df.columns:
                            df = df.filter(pl.col("symbol") == symbol)
                    all_dfs.append(df)

        if not all_dfs:
            return {"table_id": table_id, "format": format, "data": [], "message": "No records match criteria."}

        final_df = pl.concat(all_dfs)
        
        # 4. 应用时间过滤
        if start_ts is not None:
            final_df = final_df.filter(pl.col("timestamp") >= start_ts)
        if end_ts is not None:
            final_df = final_df.filter(pl.col("timestamp") <= end_ts)
            
        # 5. 排序与限制
        final_df = final_df.sort("timestamp").limit(1000)
        
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
