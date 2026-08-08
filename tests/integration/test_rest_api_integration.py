"""
tests/integration/test_rest_api_integration.py

FastAPI REST API 全流程端到端集成测试。
测试从元数据探查、物理数据 GET 切片查询到同步触发的完整闭环。
"""

import pytest
from fastapi.testclient import TestClient
from pathlib import Path
import polars as pl

from cqdata.entrypoints.rest_api import app
from cqdata.storage.storage_factory import StorageFactory
from cqdata.service.metadata_manager import MetadataManager

client = TestClient(app)


def test_rest_api_full_flow_with_physical_storage(temp_storage_root, monkeypatch):
    """
    通过 TestClient 进行全局端到端物理交互测试
    """
    # 动态 patch 全局 STORAGE_ROOT
    monkeypatch.setattr("cqdata.config.settings.STORAGE_ROOT", str(temp_storage_root))

    table_id = "ashare.kline.1d.raw.baostock"
    fmt = "parquet"

    # 1. 模拟写入物理数据
    sample_df = pl.DataFrame({
        "timestamp": [1704092400000, 1704178800000, 1704265200000],
        "datetime": [
            "2024-01-01T15:00:00.000+08:00",
            "2024-01-02T15:00:00.000+08:00",
            "2024-01-03T15:00:00.000+08:00"
        ],
        "symbol": ["sh.600000", "sh.600000", "sz.000001"],
        "open": [10.0, 10.2, 15.0],
        "high": [10.5, 10.6, 15.5],
        "low": [9.9, 10.1, 14.8],
        "close": [10.3, 10.4, 15.2],
        "volume": [100000.0, 120000.0, 200000.0]
    })

    storage = StorageFactory.get_storage(storage_format=fmt, storage_root=str(temp_storage_root), category="timeseries")
    storage.write_series(table_id, sample_df)

    meta_mgr = MetadataManager(str(temp_storage_root))
    meta_mgr.save(
        table_id=table_id,
        format=fmt,
        metadata={
            "table_id": table_id,
            "category": "timeseries",
            "schema": {"timestamp": "Int64", "datetime": "String", "symbol": "String", "open": "Float64", "high": "Float64", "low": "Float64", "close": "Float64", "volume": "Float64"},
            "statistics": {
                "start_datetime": "2024-01-01T15:00:00.000+08:00",
                "end_datetime": "2024-01-03T15:00:00.000+08:00",
                "total_bars": 3
            }
        }
    )

    # 2. 验证元数据 REST 接口
    resp_tables = client.get("/api/v1/tables/series")
    assert resp_tables.status_code == 200
    assert table_id in resp_tables.json()["tables"]

    resp_syms = client.get(f"/api/v1/tables/{table_id}/symbols")
    assert resp_syms.status_code == 200
    assert set(resp_syms.json()["symbols"]) == {"sh.600000", "sz.000001"}

    resp_schema = client.get(f"/api/v1/tables/{table_id}/schema")
    assert resp_schema.status_code == 200
    assert "close" in resp_schema.json()["schema"]

    # 3. 验证 GET 切片查询与物理分页 (page=1, page_size=2)
    query_url_p1 = f"/api/v1/query/series?table_id={table_id}&page=1&page_size=2"
    resp_q1 = client.get(query_url_p1)
    assert resp_q1.status_code == 200
    data_p1 = resp_q1.json()
    assert data_p1["total"] == 3
    assert data_p1["page"] == 1
    assert data_p1["page_size"] == 2
    assert data_p1["total_pages"] == 2
    assert data_p1["count"] == 2
    assert len(data_p1["data"]) == 2

    # 验证 GET 切片查询 (page=2, page_size=2)
    query_url_p2 = f"/api/v1/query/series?table_id={table_id}&page=2&page_size=2"
    resp_q2 = client.get(query_url_p2)
    assert resp_q2.status_code == 200
    data_p2 = resp_q2.json()
    assert data_p2["page"] == 2
    assert data_p2["count"] == 1
    assert len(data_p2["data"]) == 1
    assert data_p2["data"][0]["symbol"] == "sz.000001"

    # 4. 验证带符号和列过滤的 GET 切片查询
    filtered_url = f"/api/v1/query/series?table_id={table_id}&symbols=sh.600000&columns=timestamp,symbol,close&page=1&page_size=10"
    resp_f = client.get(filtered_url)
    assert resp_f.status_code == 200
    data_f = resp_f.json()
    assert data_f["total"] == 2
    assert data_f["count"] == 2
    for item in data_f["data"]:
        assert item["symbol"] == "sh.600000"
        assert set(item.keys()) == {"timestamp", "symbol", "close"}
