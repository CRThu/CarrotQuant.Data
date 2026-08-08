"""
tests/unit/test_accessors.py

单元测试：OOP 便捷访问层、DefaultConfig 链式继承与校验
"""

import pytest
from unittest.mock import patch, MagicMock
import polars as pl

import cqdata
from cqdata.entrypoints.accessors import DefaultConfig, AShareKline, AIndexKline, AShareConcept, AShare


def test_default_config_chain():
    """测试 DefaultConfig 三层继承链 (全局 -> 市场 -> 表)"""
    global_def = DefaultConfig(fallback_source="baostock", fallback_format="parquet")
    market_def = DefaultConfig(parent=global_def)
    table_def = DefaultConfig(parent=market_def)

    # 1. 默认探查：回退到 fallback
    assert table_def.resolve_source() == "baostock"
    assert table_def.resolve_format() == "parquet"

    # 2. 全局设置
    global_def.source = "eastmoney"
    assert table_def.resolve_source() == "eastmoney"

    # 3. 市场级覆盖全局 (小覆盖大)
    market_def.source = "tdx"
    assert table_def.resolve_source() == "tdx"

    # 4. 表级覆盖市场级
    table_def.source = "baostock"
    assert table_def.resolve_source() == "baostock"

    # 5. 重置表级，恢复继承
    table_def.source = None
    assert table_def.resolve_source() == "tdx"


def test_accessor_default_args(mock_baostock, temp_storage_root):
    """测试 OOP 表的具体 get() 方法默认参数与路径拼接"""
    with patch("cqdata.entrypoints.accessors.base.read") as mock_read:
        mock_read.return_value = pl.DataFrame({"timestamp": [1704067200000], "close": [10.0]})

        # 1. 测试 AShareKline 默认 freq="1d", adj="raw"
        df = cqdata.ashare.kline.get(symbols="sh.600000")
        assert not df.is_empty()
        mock_read.assert_called_with(
            table_id="ashare.kline.1d.raw.baostock",
            symbols="sh.600000",
            start_date=None,
            end_date=None,
            columns=None,
            format="parquet"
        )

        # 2. 测试 AIndexKline 默认 freq="1d" (固定 raw)
        cqdata.aindex.kline.get(symbols="sh.000001")
        mock_read.assert_called_with(
            table_id="aindex.kline.1d.raw.baostock",
            symbols="sh.000001",
            start_date=None,
            end_date=None,
            columns=None,
            format="parquet"
        )

        # 3. 测试 AShare 相关事件与静态表 (adj_factor, concept, industry, dragon_tiger, inst_trade)
        cqdata.ashare.adj_factor.get(symbols="sh.600000")
        mock_read.assert_called_with(table_id="ashare.adj_factor.baostock", symbols="sh.600000", start_date=None, end_date=None, columns=None, format="parquet")

        cqdata.ashare.concept.get(source="eastmoney")
        mock_read.assert_called_with(table_id="ashare.concept.eastmoney", symbols=None, start_date=None, end_date=None, columns=None, format="parquet")

        cqdata.ashare.industry.get(source="eastmoney")
        mock_read.assert_called_with(table_id="ashare.industry.eastmoney", symbols=None, start_date=None, end_date=None, columns=None, format="parquet")

        cqdata.ashare.dragon_tiger.get(source="eastmoney")
        mock_read.assert_called_with(table_id="ashare.dragon_tiger.eastmoney", symbols=None, start_date=None, end_date=None, columns=None, format="parquet")

        cqdata.ashare.inst_trade.get(source="eastmoney")
        mock_read.assert_called_with(table_id="ashare.inst_trade.eastmoney", symbols=None, start_date=None, end_date=None, columns=None, format="parquet")


def test_unsupported_table_id_error():
    """测试拼装非法或不受驱动支持的 table_id 时抛出 ValueError"""
    # 通达信驱动不支持后复权 ashare.kline.1d.adj.tdx
    with pytest.raises(ValueError, match="Unsupported table_id"):
        cqdata.ashare.kline.get(freq="1d", adj="adj", source="tdx")


def test_configure_from_yaml(tmp_path):
    """测试 cqdata.configure 指定配置文件路径加载"""
    custom_yaml = tmp_path / "custom_config.yaml"
    custom_yaml.write_text("storage_path: '/custom/storage'\ndefaults:\n  source: 'tdx'\n", encoding="utf-8")

    settings = cqdata.configure(custom_yaml)
    assert settings.storage_path == "/custom/storage"
    assert cqdata.default.resolve_source() == "tdx"

    # 恢复默认设置
    cqdata.settings.storage_path = "storage_root"
    cqdata.default.source = None
