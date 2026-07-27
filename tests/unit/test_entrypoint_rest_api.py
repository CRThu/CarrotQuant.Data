"""
tests/unit/test_entrypoint_rest_api.py

FastAPI REST 服务 endpoint 路由单元测试。
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


def test_post_query_series():
    mock_df = pl.DataFrame({
        "timestamp": [1704067200000],
        "datetime": ["2024-01-01T15:00:00.000+08:00"],
        "symbol": ["sh.600000"],
        "close": [10.5]
    })
    with patch("cqdata.entrypoints.rest_api.read_series", return_value=mock_df):
        payload = {
            "table_id": "ashare.kline.1d.adj.baostock",
            "symbols": ["sh.600000"],
            "start_date": "2024-01-01",
            "end_date": "2024-01-05"
        }
        response = client.post("/api/v1/query/series", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["table_id"] == "ashare.kline.1d.adj.baostock"
        assert data["count"] == 1
        assert data["data"][0]["symbol"] == "sh.600000"


def test_post_query_events():
    mock_df = pl.DataFrame({
        "symbol": ["sh.600000"],
        "board_name": ["概念板块"]
    })
    with patch("cqdata.entrypoints.rest_api.read_events", return_value=mock_df):
        payload = {
            "table_id": "ashare.concept.eastmoney",
            "limit": 100
        }
        response = client.post("/api/v1/query/events", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["table_id"] == "ashare.concept.eastmoney"
        assert data["count"] == 1


def test_post_sync_and_active_tasks():
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
