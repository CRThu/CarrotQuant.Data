from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from typing import List, Optional
from pydantic import BaseModel
from loguru import logger
from ..service.sync_manager import SyncManager
from ..config.settings import settings
import polars as pl

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
    支持按 symbol 和时间范围过滤。
    """
    from ..storage.storage_factory import StorageFactory
    from ..utils.time_utils import parse_date_to_ts
    
    try:
        storage = StorageFactory.get_storage(format, settings.STORAGE_ROOT)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # 1. 获取该表所有 symbol 用于验证 (可选)
    symbols = storage.get_all_symbols(table_id)
    if symbol and symbol not in symbols:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found in {table_id}")
    
    # 2. 构造查询路径。由于不同存储引擎路径略有不同，这里直接利用 polars 的通配符扫描
    pattern = ""
    if format == "csv":
        pattern = f"{settings.STORAGE_ROOT}/csv/{table_id}/year=*/*.csv"
    elif format == "parquet":
        pattern = f"{settings.STORAGE_ROOT}/parquet/{table_id}/year=*/*.parquet"
    
    import glob
    if not glob.glob(pattern):
        return {
            "table_id": table_id,
            "format": format,
            "data": [],
            "message": "No data files found for the given criteria."
        }

    try:
        # 使用 LazyFrame 进行高效过滤
        if format == "csv":
            lf = pl.scan_csv(pattern, schema_overrides={"timestamp": pl.Int64})
        else:
            lf = pl.scan_parquet(pattern)
        
        # 应用过滤条件
        if symbol:
            lf = lf.filter(pl.col("symbol") == symbol)
        
        if start_date:
            start_ts = parse_date_to_ts(start_date)
            lf = lf.filter(pl.col("timestamp") >= start_ts)
        
        if end_date:
            end_ts = parse_date_to_ts(end_date)
            lf = lf.filter(pl.col("timestamp") <= end_ts)
        
        # 排序并限制返回条数，防止 API 响应过大
        df = lf.sort("timestamp").limit(1000).collect()
        
        return {
            "table_id": table_id,
            "format": format,
            "count": df.height,
            "data": df.to_dicts(),
            "message": "Success" if df.height > 0 else "No records match the filters."
        }
    except Exception as e:
        logger.error(f"Error querying data: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
