"""
tests/unit/test_entrypoint_python_api.py

Python SDK 接入面 (python_api.py) 单元测试。
包含 read_series, read_events, list_*, get_*, sync, configure 等接口的断言与 Mock 覆盖。
"""

import pytest
import polars as pl
import pandas as pd
from unittest.mock import patch, MagicMock

import cqdata
from cqdata.entrypoints import python_api


def test_read_series_polars_and_pandas():
    """测试 read_series 支持返回 Polars 与 Pandas 两种 DataFrame"""
    mock_pl_df = pl.DataFrame({
        "symbol": ["sh.600000"],
        "timestamp": [1700000000000],
        "close": [10.0]
    })
    mock_pd_df = pd.DataFrame({
        "symbol": ["sh.600000"],
        "timestamp": [1700000000000],
        "close": [10.0]
    })

    with patch("cqdata.service.data_reader.read_series", return_value=mock_pl_df) as mock_dr_read:
        # 1. 默认 Polars
        res_pl = python_api.read_series("ashare.kline.1d.raw.baostock", symbols="sh.600000")
        assert isinstance(res_pl, pl.DataFrame)
        assert res_pl.height == 1

        # 2. Pandas 转换
        mock_dr_read.return_value = mock_pd_df
        res_pd = python_api.read_series("ashare.kline.1d.raw.baostock", symbols="sh.600000", as_pandas=True)
        assert isinstance(res_pd, pd.DataFrame)
        assert len(res_pd) == 1


def test_read_events_polars_and_pandas():
    """测试 read_events 支持返回 Polars 与 Pandas 两种 DataFrame"""
    mock_pl_df = pl.DataFrame({
        "symbol": ["sh.600000"],
        "board_name": ["银行"]
    })
    mock_pd_df = pd.DataFrame({
        "symbol": ["sh.600000"],
        "board_name": ["银行"]
    })

    with patch("cqdata.service.data_reader.read_events", return_value=mock_pl_df) as mock_dr_read:
        res_pl = python_api.read_events("ashare.concept.eastmoney")
        assert isinstance(res_pl, pl.DataFrame)
        assert res_pl.height == 1

        mock_dr_read.return_value = mock_pd_df
        res_pd = python_api.read_events("ashare.concept.eastmoney", as_pandas=True)
        assert isinstance(res_pd, pd.DataFrame)


def test_list_and_get_metadata_functions():
    """测试 list_* 与 get_* 元数据读取助手函数正确委托给 metadata_reader"""
    with patch("cqdata.service.metadata_reader.list_series_tables", return_value=["table1"]):
        assert python_api.list_series_tables() == ["table1"]

    with patch("cqdata.service.metadata_reader.list_event_tables", return_value=["table2"]):
        assert python_api.list_event_tables() == ["table2"]

    with patch("cqdata.service.metadata_reader.list_formats", return_value=["parquet"]):
        assert python_api.list_formats("table1") == ["parquet"]

    with patch("cqdata.service.metadata_reader.list_symbols", return_value=["sh.600000"]):
        assert python_api.list_symbols("table1") == ["sh.600000"]

    with patch("cqdata.service.metadata_reader.get_time_range", return_value=("2024-01-01", "2024-01-31")):
        assert python_api.get_time_range("table1") == ("2024-01-01", "2024-01-31")

    with patch("cqdata.service.metadata_reader.get_schema", return_value={"close": "Float64"}):
        assert python_api.get_schema("table1") == {"close": "Float64"}

    with patch("cqdata.service.metadata_reader.get_row_count", return_value=500):
        assert python_api.get_row_count("table1") == 500


def test_sync_function_delegation():
    """测试 sync 快捷函数正确转发至 SyncManager"""
    with patch("cqdata.service.sync_manager.sync") as mock_sync:
        python_api.sync("ashare.kline.1d.raw.baostock", formats="parquet", start_date="2024-01-01")
        assert mock_sync.called
        kwargs = mock_sync.call_args.kwargs
        assert kwargs["table_ids"] == "ashare.kline.1d.raw.baostock"
        assert kwargs["formats"] == "parquet"
        assert kwargs["start_date"] == "2024-01-01"


def test_configure_and_get_config():
    """测试 configure 全局参数配置与 get_config 获取 Settings 实例"""
    cfg = python_api.get_config()
    assert cfg is not None

    with patch("cqdata.config.settings.Settings.configure") as mock_conf:
        python_api.configure(storage_root="/tmp/test_root")
        assert mock_conf.called

    # 验证别名 set_config
    with patch("cqdata.config.settings.Settings.configure") as mock_conf_alias:
        cqdata.set_config(storage_root="/tmp/test_root_alias")
        assert mock_conf_alias.called
