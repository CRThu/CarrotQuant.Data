"""
tests/unit/test_entrypoint_rest_api.py

FastAPI REST 服务 endpoint 路由单元测试。
包含纯 HTTP GET 数据切片查询与 page / page_size 分页逻辑验证。
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import polars as pl

from cqdata.entrypoints.rest_api import app

client = TestClient(app)


def test_list_series_tables():
    with patch("cqdata.entrypoints.rest_api.list_series_tables", return_value=["ashare.kline.1d.adj.baostock"]):
        response = client.get("/api/v1/tables/series")
        assert response.status_code == 200
        assert response.json() == {"tables": ["ashare.kline.1d.adj.baostock"]}


def test_list_event_tables():
    with patch("cqdata.entrypoints.rest_api.list_event_tables", return_value=["ashare.adj_factor.baostock"]):
        response = client.get("/api/v1/tables/events")
        assert response.status_code == 200
        assert response.json() == {"tables": ["ashare.adj_factor.baostock"]}


def test_list_formats():
    with patch("cqdata.entrypoints.rest_api.list_formats", return_value=["csv", "parquet"]):
        response = client.get("/api/v1/tables/ashare.kline.1d.adj.baostock/formats")
        assert response.status_code == 200
        assert response.json() == {
            "table_id": "ashare.kline.1d.adj.baostock",
            "formats": ["csv", "parquet"]
        }


def test_list_symbols():
    with patch("cqdata.entrypoints.rest_api.list_symbols", return_value=["sh.600000", "sz.000001"]):
        response = client.get("/api/v1/tables/ashare.kline.1d.adj.baostock/symbols")
        assert response.status_code == 200
        assert response.json() == {
            "table_id": "ashare.kline.1d.adj.baostock",
            "symbol_count": 2,
            "symbols": ["sh.600000", "sz.000001"]
        }


def test_get_time_range():
    with patch("cqdata.entrypoints.rest_api.get_time_range", return_value=("2024-01-01T15:00:00.000+08:00", "2024-05-20T15:00:00.000+08:00")):
        response = client.get("/api/v1/tables/ashare.kline.1d.adj.baostock/time_range")
        assert response.status_code == 200
        assert response.json()["start_datetime"] == "2024-01-01T15:00:00.000+08:00"


def test_get_schema():
    with patch("cqdata.entrypoints.rest_api.get_schema", return_value={"symbol": "String", "close": "Float64"}):
        response = client.get("/api/v1/tables/ashare.kline.1d.adj.baostock/schema")
        assert response.status_code == 200
        assert response.json()["schema"] == {"symbol": "String", "close": "Float64"}


def test_get_row_count():
    with patch("cqdata.entrypoints.rest_api.get_row_count", return_value=12345):
        response = client.get("/api/v1/tables/ashare.kline.1d.adj.baostock/row_count")
        assert response.status_code == 200
        assert response.json() == {"table_id": "ashare.kline.1d.adj.baostock", "row_count": 12345}


def test_get_query_series_basic_and_pagination():
    """测试 HTTP GET 切片查询与物理分页逻辑"""
    mock_df = pl.DataFrame({
        "timestamp": [100, 200, 300, 400, 500],
        "datetime": [f"2024-01-0{i}T15:00:00.000+08:00" for i in range(1, 6)],
        "symbol": ["sh.600000"] * 5,
        "close": [10.0 + i for i in range(5)]
    })
    
    with patch("cqdata.entrypoints.rest_api.read_series", return_value=mock_df) as mock_read:
        # 第一页，每页2条
        url = "/api/v1/query/series?table_id=ashare.kline.1d.adj.baostock&symbols=sh.600000,sz.000001&columns=timestamp,close&page=1&page_size=2"
        response = client.get(url)
        assert response.status_code == 200
        data = response.json()

        assert data["table_id"] == "ashare.kline.1d.adj.baostock"
        assert data["total"] == 5
        assert data["page"] == 1
        assert data["page_size"] == 2
        assert data["total_pages"] == 3
        assert data["count"] == 2
        assert len(data["data"]) == 2
        assert data["data"][0]["timestamp"] == 100
        assert data["data"][1]["timestamp"] == 200

        # 校验 mock 接收的参数
        mock_read.assert_called_with(
            table_id="ashare.kline.1d.adj.baostock",
            symbols=["sh.600000", "sz.000001"],
            start_date=None,
            end_date=None,
            columns=["timestamp", "close"],
            format="auto"
        )

        # 第二页，每页2条
        url_page2 = "/api/v1/query/series?table_id=ashare.kline.1d.adj.baostock&page=2&page_size=2"
        resp_p2 = client.get(url_page2)
        assert resp_p2.status_code == 200
        d2 = resp_p2.json()
        assert d2["page"] == 2
        assert d2["count"] == 2
        assert d2["data"][0]["timestamp"] == 300
        assert d2["data"][1]["timestamp"] == 400

        # 超出范围的页码
        url_p10 = "/api/v1/query/series?table_id=ashare.kline.1d.adj.baostock&page=10&page_size=2"
        resp_p10 = client.get(url_p10)
        assert resp_p10.status_code == 200
        d10 = resp_p10.json()
        assert d10["page"] == 10
        assert d10["count"] == 0
        assert d10["data"] == []


def test_get_query_events_and_error_handling():
    """测试 GET query/events 以及异常处理"""
    mock_df = pl.DataFrame({
        "symbol": ["sh.600000"],
        "board_name": ["概念板块"]
    })
    with patch("cqdata.entrypoints.rest_api.read_events", return_value=mock_df):
        url = "/api/v1/query/events?table_id=ashare.concept.eastmoney&page=1&page_size=10"
        response = client.get(url)
        assert response.status_code == 200
        data = response.json()
        assert data["table_id"] == "ashare.concept.eastmoney"
        assert data["total"] == 1
        assert data["page_size"] == 10
        assert data["total_pages"] == 1
        assert data["count"] == 1

    # 异常抛出 500
    with patch("cqdata.entrypoints.rest_api.read_events", side_effect=ValueError("Test Exception")):
        response_err = client.get("/api/v1/query/events?table_id=invalid_table")
        assert response_err.status_code == 500
        assert "Test Exception" in response_err.json()["detail"]


def test_query_method_not_allowed_for_post():
    """验证 POST 方法请求切片查询路由被禁止 (返回 405 Method Not Allowed)"""
    response = client.post("/api/v1/query/series", json={"table_id": "ashare.kline.1d.adj.baostock"})
    assert response.status_code == 405


def test_post_sync_and_active_tasks():
    """验证 POST 数据同步与后台任务状态接口"""
    with patch("cqdata.entrypoints.rest_api.sync") as mock_sync:
        payload = {
            "table_ids": ["ashare.kline.1d.adj.baostock"],
            "formats": ["parquet"],
            "start_date": "2024-01-01"
        }
        response = client.post("/api/v1/sync", json=payload)
        assert response.status_code == 200
        assert response.json()["status"] == "accepted"

        # 验证 active_tasks
        resp_tasks = client.get("/api/v1/tasks")
        assert resp_tasks.status_code == 200
