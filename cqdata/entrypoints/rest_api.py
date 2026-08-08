"""
cqdata/entrypoints/rest_api.py

FastAPI RESTful HTTP API 接入面模块。
全面复用统一的 Service 探查与查询接口，为 Web 应用、微服务与跨语言客户端提供 HTTP 数据服务。
数据切片查询统一使用 HTTP GET 形式，并提供标准的 page / page_size 分页支持。
"""

import math
from typing import List, Optional, Union
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from pydantic import BaseModel
from loguru import logger

from cqdata.entrypoints.python_api import (
    list_series_tables,
    list_event_tables,
    list_formats,
    list_symbols,
    get_time_range,
    get_schema,
    get_row_count,
    read_series,
    read_events,
    sync
)

app = FastAPI(title="CarrotQuant.Data REST API", version="1.1.0")

# 全局并发锁集合，防止同一 table_id 重复并发同步
ACTIVE_SYNC_TASKS = set()


class SyncRequest(BaseModel):
    table_ids: List[str]
    formats: List[str] = ["parquet"]
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    force_refresh: bool = False
    batch_size: int = 100
    symbol_limit: Optional[int] = None


def parse_comma_param(val: Optional[str]) -> Optional[List[str]]:
    """将逗号分隔的 Query 字符串解析为 List[str] 清单"""
    if not val:
        return None
    items = [item.strip() for item in val.split(",") if item.strip()]
    return items if items else None


def run_sync_task(
    table_id: str,
    formats: List[str],
    start_date: Optional[str],
    end_date: Optional[str],
    force_refresh: bool,
    batch_size: int,
    symbol_limit: Optional[int]
):
    """后台同步任务执行器"""
    try:
        sync(
            table_ids=[table_id],
            formats=formats,
            start_date=start_date,
            end_date=end_date,
            force_refresh=force_refresh,
            batch_size=batch_size,
            symbol_limit=symbol_limit
        )
        logger.info(f"[REST API] Background sync finished for {table_id}")
    except Exception as e:
        logger.error(f"[REST API] Background sync failed for {table_id}: {e}")
    finally:
        ACTIVE_SYNC_TASKS.remove(table_id)


# ==================== 元数据探查 Endpoints ====================

@app.get("/api/v1/tables/series")
async def api_list_series_tables(format: str = "auto"):
    """获取所有时间序列表 ID"""
    return {"tables": list_series_tables(format=format)}


@app.get("/api/v1/tables/events")
async def api_list_event_tables(format: str = "auto"):
    """获取所有事件表 ID"""
    return {"tables": list_event_tables(format=format)}


@app.get("/api/v1/tables/{table_id}/formats")
async def api_list_formats(table_id: str):
    """获取某表在本地已有的格式列表"""
    return {"table_id": table_id, "formats": list_formats(table_id)}


@app.get("/api/v1/tables/{table_id}/symbols")
async def api_list_symbols(table_id: str, format: str = "auto"):
    """获取某表包含的 symbol 唯一代码列表"""
    symbols = list_symbols(table_id, format=format)
    return {"table_id": table_id, "symbol_count": len(symbols), "symbols": symbols}


@app.get("/api/v1/tables/{table_id}/time_range")
async def api_get_time_range(table_id: str, format: str = "auto"):
    """获取某表的时间跨度 (start_datetime, end_datetime)"""
    start_dt, end_dt = get_time_range(table_id, format=format)
    return {"table_id": table_id, "start_datetime": start_dt, "end_datetime": end_dt}


@app.get("/api/v1/tables/{table_id}/schema")
async def api_get_schema(table_id: str, format: str = "auto"):
    """获取某表在元数据中的字段 Schema 列名与类型"""
    return {"table_id": table_id, "schema": get_schema(table_id, format=format)}


@app.get("/api/v1/tables/{table_id}/row_count")
async def api_get_row_count(table_id: str, format: str = "auto"):
    """获取某表在物理存储中的记录总行数/条目数"""
    return {"table_id": table_id, "row_count": get_row_count(table_id, format=format)}


# ==================== 数据切片查询 Endpoints (纯 HTTP GET 形式) ====================

@app.get("/api/v1/query/series")
async def api_query_series(
    table_id: str = Query(..., description="数据表 ID (如 ashare.kline.1d.raw.baostock)"),
    symbols: Optional[str] = Query(None, description="股票代码或以逗号分隔的代码列表 (如 sh.600000,sz.000001)"),
    start_date: Optional[str] = Query(None, description="起始日期 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYY-MM-DD"),
    columns: Optional[str] = Query(None, description="选挑字段清单，以逗号分隔 (如 timestamp,close)"),
    format: str = Query("auto", description="存储格式 (auto, parquet, csv)"),
    page: int = Query(1, ge=1, description="当前页码 (从1开始)"),
    page_size: int = Query(5000, ge=1, description="每页记录数")
):
    """
    切片查询【时间序列 (TimeSeries)】数据 (K线/分笔)，支持 HTTP GET 查询参数与物理分页
    """
    try:
        parsed_symbols = parse_comma_param(symbols)
        parsed_columns = parse_comma_param(columns)

        df = read_series(
            table_id=table_id,
            symbols=parsed_symbols,
            start_date=start_date,
            end_date=end_date,
            columns=parsed_columns,
            format=format
        )

        total = df.height
        total_pages = math.ceil(total / page_size) if total > 0 else 0
        offset = (page - 1) * page_size

        sliced_df = df.slice(offset, page_size) if not df.is_empty() else df

        return {
            "table_id": table_id,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "count": sliced_df.height,
            "data": sliced_df.to_dicts() if not sliced_df.is_empty() else []
        }
    except Exception as e:
        logger.error(f"[REST API] GET query_series error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/query/events")
async def api_query_events(
    table_id: str = Query(..., description="数据表 ID (如 ashare.dragon_tiger.eastmoney)"),
    symbols: Optional[str] = Query(None, description="股票代码或以逗号分隔的代码列表"),
    start_date: Optional[str] = Query(None, description="起始日期 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYY-MM-DD"),
    columns: Optional[str] = Query(None, description="选挑字段清单，以逗号分隔"),
    format: str = Query("auto", description="存储格式 (auto, parquet, csv)"),
    page: int = Query(1, ge=1, description="当前页码 (从1开始)"),
    page_size: int = Query(5000, ge=1, description="每页记录数")
):
    """
    切片查询【事件/静态 (Event)】数据 (板块成分股/龙虎榜等)，支持 HTTP GET 查询参数与物理分页
    """
    try:
        parsed_symbols = parse_comma_param(symbols)
        parsed_columns = parse_comma_param(columns)

        df = read_events(
            table_id=table_id,
            symbols=parsed_symbols,
            start_date=start_date,
            end_date=end_date,
            columns=parsed_columns,
            format=format
        )

        total = df.height
        total_pages = math.ceil(total / page_size) if total > 0 else 0
        offset = (page - 1) * page_size

        sliced_df = df.slice(offset, page_size) if not df.is_empty() else df

        return {
            "table_id": table_id,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "count": sliced_df.height,
            "data": sliced_df.to_dicts() if not sliced_df.is_empty() else []
        }
    except Exception as e:
        logger.error(f"[REST API] GET query_events error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 同步与控制 Endpoints ====================

@app.post("/api/v1/sync")
async def api_sync_data(request: SyncRequest, background_tasks: BackgroundTasks):
    """异步触发数据同步 (写/非幂等操作，保留 HTTP POST 方法)"""
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
            request.batch_size,
            request.symbol_limit
        )

    return {
        "status": "accepted",
        "started_tasks": processing_tables,
        "ignored_tasks": locked_tables,
        "message": "Sync tasks started in background."
    }


@app.get("/api/v1/tasks")
async def api_get_active_tasks():
    """获取正在运行的后台同步任务"""
    return {"active_tasks": list(ACTIVE_SYNC_TASKS)}
