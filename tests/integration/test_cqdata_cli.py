"""
tests/integration/test_cqdata_cli.py

cqdata Typer CLI 终端命令行全量子命令与选项集成测试
"""

import pytest
from typer.testing import CliRunner
from unittest.mock import patch
from cqdata.entrypoints.cli import app
from cqdata.storage.storage_factory import StorageFactory
from cqdata.service.metadata_manager import MetadataManager
import polars as pl

runner = CliRunner(env={"COLUMNS": "200", "TERM": "dumb"})


@pytest.fixture
def mock_cli_storage(temp_data_dir):
    """初始化模拟存储用于 CLI 命令解析测试"""
    table_id = "ashare.kline.1d.raw.baostock"
    df = pl.DataFrame({
        "symbol": ["sh.600000"],
        "timestamp": [1700000000000],
        "datetime": ["2023-11-14T15:00:00.000+08:00"],
        "close": [10.0]
    })
    pq = StorageFactory.get_storage("parquet", str(temp_data_dir), "timeseries")
    pq.write_series(table_id, df)

    meta_mgr = MetadataManager(str(temp_data_dir))
    meta_mgr.save(table_id, "parquet", {
        "category": "timeseries",
        "schema": {
            "symbol": "String",
            "timestamp": "Int64",
            "datetime": "String",
            "close": "Float64"
        },
        "statistics": {
            "start_timestamp": 1700000000000,
            "end_timestamp": 1700000000000,
            "start_datetime": "2023-11-14T15:00:00.000+08:00",
            "end_datetime": "2023-11-14T15:00:00.000+08:00",
            "total_bars": 1
        }
    })
    return table_id


def test_cli_help():
    """测试 cqdata --help 命令"""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "cqdata" in result.stdout


def test_cli_tables_command(mock_cli_storage, temp_data_dir):
    """测试 cqdata tables 命令输出"""
    result = runner.invoke(app, ["tables", "-f", "parquet"])
    assert result.exit_code == 0
    assert "本地数据表概览" in result.stdout


def test_cli_info_command(mock_cli_storage, temp_data_dir):
    """测试 cqdata info 命令输出"""
    table_id = mock_cli_storage
    result = runner.invoke(app, ["info", table_id, "-f", "parquet"])
    assert result.exit_code == 0
    assert "表元数据" in result.stdout
    assert "close" in result.stdout


def test_cli_sync_command(mock_cli_storage, temp_data_dir):
    """测试 cqdata sync 命令调用参数挂载"""
    with patch("cqdata.entrypoints.cli.api_sync") as mock_api_sync:
        result = runner.invoke(app, [
            "sync",
            "-t", "ashare.kline.1d.raw.baostock",
            "-f", "parquet",
            "-s", "2024-01-01",
            "-e", "2024-01-31",
            "--limit", "10"
        ])
        assert result.exit_code == 0
        assert mock_api_sync.called
        kwargs = mock_api_sync.call_args.kwargs
        assert kwargs["table_ids"] == ["ashare.kline.1d.raw.baostock"]
        assert kwargs["formats"] == ["parquet"]
        assert kwargs["start_date"] == "2024-01-01"
        assert kwargs["end_date"] == "2024-01-31"
        assert kwargs["symbol_limit"] == 10


def test_cli_server_help():
    """测试 cqdata server --help"""
    result = runner.invoke(app, ["server", "--help"])
    assert result.exit_code == 0
    assert "port" in result.stdout.lower()


def test_cli_tdx_help():
    """测试 cqdata tdx download --help"""
    result = runner.invoke(app, ["tdx", "download", "--help"])
    assert result.exit_code == 0
    assert "vipdoc" in result.stdout.lower()


def test_cli_wizard_help():
    """测试 cqdata wizard --help"""
    result = runner.invoke(app, ["wizard", "--help"])
    assert result.exit_code == 0
