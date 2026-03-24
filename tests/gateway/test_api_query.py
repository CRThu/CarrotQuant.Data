import pytest
from fastapi.testclient import TestClient
from app.gateway.api import app
from unittest.mock import patch, MagicMock
import polars as pl

client = TestClient(app)

def test_query_data_symbol_filter():
    """
    测试 GET /api/v1/data/{table_id} 的 symbol 过滤
    """
    table_id = "test.query.symbol"
    
    # Mock 存储层返回数据
    mock_storage = MagicMock()
    mock_storage.get_all_symbols.return_value = ["sh.600000", "sz.000001"]
    
    # Mock scan_csv 返回 LazyFrame
    mock_lf = MagicMock()
    mock_lf.filter.return_value = mock_lf
    mock_lf.sort.return_value = mock_lf
    mock_lf.limit.return_value = mock_lf
    mock_lf.collect.return_value = pl.DataFrame({
        "timestamp": [1704067200000],
        "symbol": ["sh.600000"],
        "close": [10.0]
    })
    
    with patch("app.storage.storage_factory.StorageFactory.get_storage", return_value=mock_storage):
        with patch("polars.scan_csv", return_value=mock_lf):
            with patch("glob.glob", return_value=["test.csv"]):
                response = client.get(f"/api/v1/data/{table_id}?symbol=sh.600000&format=csv")
    
    assert response.status_code == 200
    data = response.json()
    assert data["table_id"] == table_id
    assert data["count"] == 1
    assert len(data["data"]) == 1
    assert data["data"][0]["symbol"] == "sh.600000"

def test_query_data_timestamp_range():
    """
    测试 GET /api/v1/data/{table_id} 的时间戳区间过滤
    """
    table_id = "test.query.time"
    
    mock_storage = MagicMock()
    mock_storage.get_all_symbols.return_value = ["sh.600000"]
    
    mock_lf = MagicMock()
    mock_lf.filter.return_value = mock_lf
    mock_lf.sort.return_value = mock_lf
    mock_lf.limit.return_value = mock_lf
    mock_lf.collect.return_value = pl.DataFrame({
        "timestamp": [1704067200000, 1704153600000],
        "symbol": ["sh.600000"] * 2,
        "close": [10.0, 10.1]
    })
    
    with patch("app.storage.storage_factory.StorageFactory.get_storage", return_value=mock_storage):
        with patch("polars.scan_csv", return_value=mock_lf):
            with patch("glob.glob", return_value=["test.csv"]):
                response = client.get(f"/api/v1/data/{table_id}?start_date=2024-01-01&end_date=2024-01-02&format=csv")
    
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 2

def test_query_data_symbol_not_found():
    """
    测试 symbol 不存在时返回 404
    """
    table_id = "test.query.notfound"
    
    mock_storage = MagicMock()
    mock_storage.get_all_symbols.return_value = ["sh.600000"]  # 不包含 sz.000001
    
    with patch("app.storage.storage_factory.StorageFactory.get_storage", return_value=mock_storage):
        response = client.get(f"/api/v1/data/{table_id}?symbol=sz.000001&format=csv")
    
    assert response.status_code == 404
    assert "Symbol sz.000001 not found" in response.json()["detail"]

def test_query_data_no_files():
    """
    测试没有数据文件时返回空结果
    """
    table_id = "test.query.empty"
    
    mock_storage = MagicMock()
    mock_storage.get_all_symbols.return_value = ["sh.600000"]
    
    with patch("app.storage.storage_factory.StorageFactory.get_storage", return_value=mock_storage):
        with patch("glob.glob", return_value=[]):  # 没有文件
            response = client.get(f"/api/v1/data/{table_id}?format=csv")
    
    assert response.status_code == 200
    data = response.json()
    assert data["data"] == []
    assert "No data files found" in data["message"]

def test_query_data_invalid_format():
    """
    测试无效格式返回 400
    """
    table_id = "test.query.format"
    
    with patch("app.storage.storage_factory.StorageFactory.get_storage", side_effect=ValueError("Unsupported storage format: invalid")):
        response = client.get(f"/api/v1/data/{table_id}?format=invalid")
    
    assert response.status_code == 400
    assert "Unsupported storage format" in response.json()["detail"]

def test_query_data_parquet_format():
    """
    测试 Parquet 格式查询
    """
    table_id = "test.query.parquet"
    
    mock_storage = MagicMock()
    mock_storage.get_all_symbols.return_value = ["sh.600000"]
    
    mock_lf = MagicMock()
    mock_lf.filter.return_value = mock_lf
    mock_lf.sort.return_value = mock_lf
    mock_lf.limit.return_value = mock_lf
    mock_lf.collect.return_value = pl.DataFrame({
        "timestamp": [1704067200000],
        "symbol": ["sh.600000"],
        "close": [10.0]
    })
    
    with patch("app.storage.storage_factory.StorageFactory.get_storage", return_value=mock_storage):
        with patch("polars.scan_parquet", return_value=mock_lf):
            with patch("glob.glob", return_value=["test.parquet"]):
                response = client.get(f"/api/v1/data/{table_id}?format=parquet")
    
    assert response.status_code == 200
    data = response.json()
    assert data["format"] == "parquet"

def test_query_data_limit_enforcement():
    """
    测试返回条数限制（最多1000条）
    """
    table_id = "test.query.limit"
    
    mock_storage = MagicMock()
    mock_storage.get_all_symbols.return_value = ["sh.600000"]
    
    # 创建一个返回大量数据的 mock
    large_df = pl.DataFrame({
        "timestamp": list(range(2000)),
        "symbol": ["sh.600000"] * 2000,
        "close": [10.0] * 2000
    })
    
    mock_lf = MagicMock()
    mock_lf.filter.return_value = mock_lf
    mock_lf.sort.return_value = mock_lf
    mock_lf.limit.return_value = mock_lf
    mock_lf.collect.return_value = large_df.head(1000)  # limit(1000) 应该被调用
    
    with patch("app.storage.storage_factory.StorageFactory.get_storage", return_value=mock_storage):
        with patch("polars.scan_csv", return_value=mock_lf):
            with patch("glob.glob", return_value=["test.csv"]):
                response = client.get(f"/api/v1/data/{table_id}?format=csv")
    
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1000  # 最多返回1000条

def test_query_data_combined_filters():
    """
    测试组合过滤条件（symbol + 时间范围）
    """
    table_id = "test.query.combined"
    
    mock_storage = MagicMock()
    mock_storage.get_all_symbols.return_value = ["sh.600000", "sz.000001"]
    
    mock_lf = MagicMock()
    mock_lf.filter.return_value = mock_lf
    mock_lf.sort.return_value = mock_lf
    mock_lf.limit.return_value = mock_lf
    mock_lf.collect.return_value = pl.DataFrame({
        "timestamp": [1704067200000],
        "symbol": ["sh.600000"],
        "close": [10.0]
    })
    
    with patch("app.storage.storage_factory.StorageFactory.get_storage", return_value=mock_storage):
        with patch("polars.scan_csv", return_value=mock_lf):
            with patch("glob.glob", return_value=["test.csv"]):
                response = client.get(f"/api/v1/data/{table_id}?symbol=sh.600000&start_date=2024-01-01&end_date=2024-01-02&format=csv")
    
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    # 验证 filter 被调用了3次（symbol、start_date、end_date）
    assert mock_lf.filter.call_count == 3