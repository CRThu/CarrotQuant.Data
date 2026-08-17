"""
tests/unit/test_rest_api_sync.py

FastAPI REST API 扩展端点单元测试。
验证 /api/v1/tables/detailed 与 /api/v1/sync/status 端点结构。
"""

import pytest
from fastapi.testclient import TestClient
from cq.data.entrypoints.rest_api import app


@pytest.fixture
def client():
    return TestClient(app)


def test_api_tables_detailed(client):
    response = client.get("/api/v1/tables/detailed")
    assert response.status_code == 200
    data = response.json()
    assert "tables" in data
    assert "total" in data
    assert data["total"] > 0

    first_table = data["tables"][0]
    assert "table_id" in first_table
    assert "name" in first_table
    assert "formats" in first_table
    assert "parquet" in first_table["formats"]
    assert "csv" in first_table["formats"]
    assert "exists" in first_table["formats"]["parquet"]


def test_api_sync_status(client):
    response = client.get("/api/v1/sync/status")
    assert response.status_code == 200
    data = response.json()
    assert "active_tasks" in data
    assert "statuses" in data


def test_api_tables_detailed_with_metadata(client, tmp_path, monkeypatch):
    """验证 GET /api/v1/tables/detailed 在磁盘有物理元数据时的解析与返回结构"""
    from cq.data.service.metadata_manager import MetadataManager
    from cq.data.config.settings import settings

    monkeypatch.setattr(settings, "data_dir", str(tmp_path))
    meta_mgr = MetadataManager(str(tmp_path))
    
    table_id = "ashare.kline.1d.adj.baostock"
    dummy_meta = {
        "table_id": table_id,
        "category": "timeseries",
        "format": "parquet",
        "partition": "year",
        "layout": "hive",
        "schema": {"timestamp": "Int64", "close": "Float64"},
        "statistics": {
            "updated_at": "2026-08-10T16:25:00.000+08:00",
            "start_timestamp": 1704067200000,
            "end_timestamp": 1704153600000,
            "start_datetime": "2024-01-01T00:00:00.000+08:00",
            "end_datetime": "2024-01-02T00:00:00.000+08:00",
            "total_bars": 100,
            "symbol_count": 5,
            "time_steps": 2
        }
    }
    meta_mgr.save(table_id, "parquet", dummy_meta)

    response = client.get("/api/v1/tables/detailed")
    assert response.status_code == 200
    data = response.json()
    
    target_table = next((t for t in data["tables"] if t["table_id"] == table_id), None)
    assert target_table is not None
    assert target_table["formats"]["parquet"]["exists"] is True
    assert target_table["formats"]["parquet"]["updated_at"] == "2026-08-10T16:25:00.000+08:00"
    assert target_table["formats"]["parquet"]["total_bars"] == 100
    assert target_table["formats"]["parquet"]["symbol_count"] == 5
    assert target_table["formats"]["parquet"]["start_datetime"] == "2024-01-01T00:00:00.000+08:00"
    assert target_table["formats"]["csv"]["exists"] is False

