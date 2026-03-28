import pytest
from fastapi.testclient import TestClient
from app.gateway.api import app
from unittest.mock import patch, MagicMock
import polars as pl

client = TestClient(app)

@pytest.fixture
def mock_provider_manager():
    with patch("app.gateway.api.ProviderManager") as mock:
        manager_instance = mock.return_value
        provider = MagicMock()
        manager_instance.get_provider.return_value = provider
        provider.get_table_category.return_value = "TS" # Default to TS
        yield manager_instance, provider

def test_query_data_symbol_filter(mock_provider_manager):
    """
    测试 GET /api/v1/data/{table_id} 的 symbol 过滤
    """
    table_id = "test.query.symbol"
    _, provider = mock_provider_manager
    provider.get_table_category.return_value = "TS"
    
    mock_storage = MagicMock()
    # mock_storage.read_series 返回数据
    mock_storage.read_series.return_value = pl.DataFrame({
        "timestamp": [1704067200000],
        "symbol": ["sh.600000"],
        "close": [10.0]
    })
    
    with patch("app.storage.storage_factory.StorageFactory.get_storage", return_value=mock_storage):
        with patch("glob.glob", return_value=[f"storage_root/csv/{table_id}/year=2024"]):
            response = client.get(f"/api/v1/data/{table_id}?symbol=sh.600000&format=csv")
    
    assert response.status_code == 200
    data = response.json()
    assert data["table_id"] == table_id
    assert data["count"] == 1
    assert data["data"][0]["symbol"] == "sh.600000"

def test_query_data_timestamp_range(mock_provider_manager):
    """
    测试 GET /api/v1/data/{table_id} 的时间戳区间过滤
    """
    from app.utils.time_utils import parse_date_to_ts
    table_id = "test.query.time"
    _, provider = mock_provider_manager
    provider.get_table_category.return_value = "TS"
    
    start_ts = parse_date_to_ts("2024-01-01")
    end_ts = parse_date_to_ts("2024-01-02")
    
    mock_storage = MagicMock()
    # 确保 mock 数据在 [start_ts, end_ts] 范围内
    mock_storage.read_series.return_value = pl.DataFrame({
        "timestamp": [start_ts, end_ts],
        "symbol": ["sh.600000"] * 2,
        "close": [10.0, 10.1]
    })
    
    with patch("app.storage.storage_factory.StorageFactory.get_storage", return_value=mock_storage):
        with patch("glob.glob", return_value=[f"storage_root/csv/{table_id}/year=2024"]):
            response = client.get(f"/api/v1/data/{table_id}?symbol=sh.600000&start_date=2024-01-01&end_date=2024-01-02&format=csv")
    
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 2

def test_query_data_ts_requires_symbol(mock_provider_manager):
    """
    测试 TS 类别查询必须带 symbol
    """
    table_id = "test.query.ts_no_symbol"
    _, provider = mock_provider_manager
    provider.get_table_category.return_value = "TS"
    
    response = client.get(f"/api/v1/data/{table_id}")
    assert response.status_code == 400
    assert "TS data query requires 'symbol'" in response.json()["detail"]

def test_query_data_ev_no_symbol_ok(mock_provider_manager):
    """
    测试 EV 类别查询不带 symbol 也可以
    """
    table_id = "test.query.ev_no_symbol"
    _, provider = mock_provider_manager
    provider.get_table_category.return_value = "EV"
    
    mock_storage = MagicMock()
    mock_storage.read_event.return_value = pl.DataFrame({
        "timestamp": [1704067200000],
        "symbol": ["sh.600000"],
        "close": [10.0]
    })
    
    with patch("app.storage.storage_factory.StorageFactory.get_storage", return_value=mock_storage):
        with patch("glob.glob", return_value=[f"storage_root/csv/{table_id}/year=2024"]):
            response = client.get(f"/api/v1/data/{table_id}")
    
    assert response.status_code == 200
    assert response.json()["count"] == 1

def test_query_data_no_files(mock_provider_manager):
    """
    测试没有数据文件时返回空结果
    """
    table_id = "test.query.empty"
    _, provider = mock_provider_manager
    
    mock_storage = MagicMock()
    
    with patch("app.storage.storage_factory.StorageFactory.get_storage", return_value=mock_storage):
        with patch("glob.glob", return_value=[]):  # 没有文件
            # 对于 TS, 必须带 symbol 才能过第一关校验
            response = client.get(f"/api/v1/data/{table_id}?symbol=any&format=csv")
    
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

def test_query_data_parquet_format(mock_provider_manager):
    """
    测试 Parquet 格式查询
    """
    table_id = "test.query.parquet"
    _, provider = mock_provider_manager
    
    mock_storage = MagicMock()
    mock_storage.read_series.return_value = pl.DataFrame({
        "timestamp": [1704067200000],
        "symbol": ["sh.600000"],
        "close": [10.0]
    })
    
    with patch("app.storage.storage_factory.StorageFactory.get_storage", return_value=mock_storage):
        with patch("glob.glob", return_value=[f"storage_root/parquet/{table_id}/year=2024"]):
            response = client.get(f"/api/v1/data/{table_id}?symbol=sh.600000&format=parquet")
    
    assert response.status_code == 200
    data = response.json()
    assert data["format"] == "parquet"

def test_query_data_limit_enforcement(mock_provider_manager):
    """
    测试返回条数限制（最多1000条）
    """
    table_id = "test.query.limit"
    _, provider = mock_provider_manager
    
    # 创建一个返回大量数据的 mock
    large_df = pl.DataFrame({
        "timestamp": list(range(2000)),
        "symbol": ["sh.600000"] * 2000,
        "close": [10.0] * 2000
    })
    
    mock_storage = MagicMock()
    mock_storage.read_series.return_value = large_df
    
    with patch("app.storage.storage_factory.StorageFactory.get_storage", return_value=mock_storage):
        with patch("glob.glob", return_value=[f"storage_root/csv/{table_id}/year=1970"]):
            response = client.get(f"/api/v1/data/{table_id}?symbol=sh.600000&format=csv")
    
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1000  # 最多返回1000条