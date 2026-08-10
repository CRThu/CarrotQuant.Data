"""
cqdata/entrypoints/rest_api.py

FastAPI RESTful HTTP API 接入面模块。
全面复用统一的 Service 探查与查询接口，为 Web 应用、微服务与跨语言客户端提供 HTTP 数据服务。
数据切片查询统一使用 HTTP GET 形式，并提供标准的 page / page_size 分页支持。
"""

import math
import importlib.resources
from pathlib import Path
from typing import List, Optional, Union
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException
from pydantic import BaseModel
from loguru import logger
from cqdata.config import settings


class SPAStaticFiles(StaticFiles):
    """
    FastAPI / Starlette 标准 SPA 静态文件托管支持：
    - 已存在的静态文件 (JS/CSS/Font/Icon)：提供高性能异步流响应并支持 ETag 304 缓存
    - 前端 SPA 页面路由：文件不存在时自动回退渲染 index.html
    - /api/* 端点：未匹配时保持标准的 404 HTTP 异常
    """
    async def get_response(self, path: str, scope):
        try:
            return await super().get_response(path, scope)
        except StarletteHTTPException as ex:
            if ex.status_code == 404:
                norm_path = path.replace("\\", "/")
                if norm_path.startswith("api/"):
                    raise StarletteHTTPException(status_code=404, detail=f"API endpoint '/{norm_path}' not found")
                return await super().get_response("index.html", scope)
            raise ex









from cqdata.entrypoints.python_api import (
    read,
    list_tables,
    list_formats,
    list_symbols,
    get_time_range,
    get_schema,
    get_row_count,
    sync
)

from cqdata import __version__

app = FastAPI(title="CarrotQuant.Data REST API", version=__version__)

# 挂载 CORS 跨域中间件，允许 Web 前端应用直接调用 API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/v1/health")
async def api_health_check():
    """健康检查与系统运行状态探针"""
    return {
        "status": "ok",
        "version": __version__,
        "data_dir": str(settings.data_dir),
        "active_tasks": len(ACTIVE_SYNC_TASKS)
    }


def handle_endpoint_exception(e: Exception, endpoint_name: str):
    """
    REST API 精细化异常分发与 HTTP 状态码转换：
    - FileNotFoundError / KeyError -> 404 Not Found
    - ValueError -> 400 Bad Request
    - HTTPException -> 原样抛出
    - 其他未捕获系统异常 -> 500 Internal Server Error
    """
    if isinstance(e, HTTPException):
        raise e
    if isinstance(e, (FileNotFoundError, KeyError)):
        logger.warning(f"[REST API] {endpoint_name} 资源未找到: {e}")
        raise HTTPException(status_code=404, detail=f"Resource not found: {e}")
    if isinstance(e, ValueError):
        logger.warning(f"[REST API] {endpoint_name} 请求参数错误: {e}")
        raise HTTPException(status_code=400, detail=f"Bad request parameter: {e}")

    logger.error(f"[REST API] {endpoint_name} 服务器内部错误: {e}")
    raise HTTPException(status_code=500, detail=str(e))


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

@app.get("/api/v1/tables")
async def api_list_all_tables(format: str = "auto"):
    """获取所有本地数据表总览清单 (平铺对象列表，含 category 属性)"""
    try:
        tables = list_tables(format=format)
        return {
            "tables": tables,
            "total": len(tables)
        }
    except Exception as e:
        handle_endpoint_exception(e, "GET tables")


@app.get("/api/v1/tables/{table_id}/formats")
async def api_list_formats(table_id: str):
    """获取某表在本地已有的格式列表"""
    try:
        return {"table_id": table_id, "formats": list_formats(table_id)}
    except Exception as e:
        handle_endpoint_exception(e, f"GET formats for {table_id}")


@app.get("/api/v1/tables/{table_id}/symbols")
async def api_list_symbols(table_id: str, format: str = "auto"):
    """获取某表包含的 symbol 唯一代码列表"""
    try:
        symbols = list_symbols(table_id, format=format)
        return {"table_id": table_id, "symbol_count": len(symbols), "symbols": symbols}
    except Exception as e:
        handle_endpoint_exception(e, f"GET symbols for {table_id}")


@app.get("/api/v1/tables/{table_id}/time_range")
async def api_get_time_range(table_id: str, format: str = "auto"):
    """获取某表的时间跨度 (start_datetime, end_datetime)"""
    try:
        start_dt, end_dt = get_time_range(table_id, format=format)
        return {"table_id": table_id, "start_datetime": start_dt, "end_datetime": end_dt}
    except Exception as e:
        handle_endpoint_exception(e, f"GET time_range for {table_id}")


@app.get("/api/v1/tables/{table_id}/schema")
async def api_get_schema(table_id: str, format: str = "auto"):
    """获取某表在元数据中的字段 Schema 列名与类型"""
    try:
        return {"table_id": table_id, "schema": get_schema(table_id, format=format)}
    except Exception as e:
        handle_endpoint_exception(e, f"GET schema for {table_id}")


@app.get("/api/v1/tables/{table_id}/row_count")
async def api_get_row_count(table_id: str, format: str = "auto"):
    """获取某表在物理存储中的记录总行数/条目数"""
    try:
        return {"table_id": table_id, "row_count": get_row_count(table_id, format=format)}
    except Exception as e:
        handle_endpoint_exception(e, f"GET row_count for {table_id}")


# ==================== 数据切片查询 Endpoints (纯 HTTP GET 形式) ====================

@app.get("/api/v1/query")
async def api_query(
    table_id: str = Query(..., description="数据表 ID (如 ashare.kline.1d.raw.baostock 或 ashare.dragon_tiger.eastmoney)"),
    symbols: Optional[str] = Query(None, description="股票代码或以逗号分隔的代码列表 (如 sh.600000,sz.000001)"),
    start_date: Optional[str] = Query(None, description="起始日期 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYY-MM-DD"),
    columns: Optional[str] = Query(None, description="选挑字段清单，以逗号分隔 (如 timestamp,close)"),
    format: str = Query("auto", description="存储格式 (auto, parquet, csv)"),
    page: int = Query(1, ge=1, description="当前页码 (从1开始)"),
    page_size: int = Query(5000, ge=1, description="每页记录数")
):
    """
    统一切片查询接口 (自动按 table_id 智能路由)，支持 HTTP GET 查询参数与物理分页，输出二维 List 矩阵
    """
    try:
        parsed_symbols = parse_comma_param(symbols)
        parsed_columns = parse_comma_param(columns)

        df = read(
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
            "columns": sliced_df.columns,
            "data": sliced_df.rows() if not sliced_df.is_empty() else []
        }
    except Exception as e:
        handle_endpoint_exception(e, "GET query")


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


# ==================== 静态前端 UI 托管 (SPA 标准挂载) ====================

try:
    STATIC_DIR = Path(importlib.resources.files("cqdata") / "static")
except Exception:
    STATIC_DIR = Path(__file__).parent.parent / "static"

if STATIC_DIR.exists() and (STATIC_DIR / "index.html").exists():
    logger.info(f"[REST API] 已挂载静态前端 UI 托管: {STATIC_DIR}")
    app.mount("/", SPAStaticFiles(directory=STATIC_DIR, html=True), name="static")


