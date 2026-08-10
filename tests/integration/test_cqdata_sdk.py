"""
tests/integration/test_cqdata_sdk.py

cqdata Python SDK 端到端集成测试
模拟真实调用 import cqdata 的整套 API
"""

import pytest
import polars as pl
import cqdata
from cqdata.storage.storage_factory import StorageFactory
from cqdata.service.metadata_manager import MetadataManager
from cqdata.utils.time_utils import parse_date_to_ts, ts_to_iso


@pytest.fixture
def mock_sdk_env(temp_data_dir):
    """初始化端到端集成测试数据环境"""
    ts_table = "ashare.kline.1d.raw.baostock"
    ev_table = "ashare.dragon_tiger.eastmoney"

    ts1 = parse_date_to_ts("2024-01-02")
    ts2 = parse_date_to_ts("2024-01-03")

    df_ts = pl.DataFrame({
        "symbol": ["sh.600000", "sz.000001"],
        "timestamp": [ts1, ts2],
        "datetime": [ts_to_iso(ts1), ts_to_iso(ts2)],
        "open": [10.0, 15.0],
        "close": [10.5, 15.5]
    })

    pq_ts = StorageFactory.get_storage("parquet", str(temp_data_dir), "timeseries")
    pq_ts.write_series(ts_table, df_ts)

    meta_mgr = MetadataManager(str(temp_data_dir))
    meta_mgr.save(ts_table, "parquet", {
        "category": "timeseries",
        "schema": {
            "symbol": "String",
            "timestamp": "Int64",
            "datetime": "String",
            "open": "Float64",
            "close": "Float64"
        },
        "statistics": {
            "start_timestamp": ts1,
            "end_timestamp": ts2,
            "start_datetime": ts_to_iso(ts1),
            "end_datetime": ts_to_iso(ts2),
            "total_bars": 2
        }
    })

    return ts_table, ev_table


def test_sdk_full_flow(mock_sdk_env, temp_data_dir):
    """验证 import cqdata 的完整工作流程"""
    ts_table, _ = mock_sdk_env

    # 通过全局 settings 显式设置测试存储路径
    cqdata.settings.data_dir = str(temp_data_dir)

    # 1. 探查列表
    tables = cqdata.list_tables()
    table_ids = [t["table_id"] for t in tables]
    assert ts_table in table_ids
    assert tables[0]["category"] == "timeseries"

    # 2. 获取代码与辅助过滤元数据
    symbols = cqdata.list_symbols(ts_table)
    assert set(symbols) == {"sh.600000", "sz.000001"}

    start_dt, end_dt = cqdata.get_time_range(ts_table)
    assert "2024-01-02" in start_dt

    schema = cqdata.get_schema(ts_table)
    assert "close" in schema

    row_count = cqdata.get_row_count(ts_table)
    assert row_count == 2

    # 3. 显式切片读取并投影列
    df = cqdata.read(
        ts_table,
        symbols=["sh.600000"],
        columns=["timestamp", "close"]
    )
    assert len(df) == 1
    assert df.columns == ["timestamp", "close"]
    assert df["close"][0] == 10.5
