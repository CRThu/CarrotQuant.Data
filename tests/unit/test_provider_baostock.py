"""BaostockProvider 单元测试。

验证:
1. None 日期默认值处理（与 EastMoneyProvider 统一）
2. 支持的 table_id 路由
3. 空数据防御
"""

import pytest
import polars as pl
from unittest.mock import MagicMock
from app.provider.baostock_provider import BaostockProvider


@pytest.fixture(autouse=True)
def _reset_provider_manager():
    """每个测试前后清理 ProviderManager singleton，防止泄漏。"""
    from app.provider.provider_manager import ProviderManager
    ProviderManager._instance = None
    ProviderManager._providers = {}
    yield
    ProviderManager._instance = None
    ProviderManager._providers = {}


@pytest.fixture
def provider(mock_baostock):
    """创建 BaostockProvider 实例，使用 mock_baostock 避免网络调用。"""
    return BaostockProvider()


class TestNoneDateDefaults:
    """测试 None 日期默认值处理。"""

    def test_fetch_none_start_date_defaults_to_2020(self, provider, mock_baostock):
        """start_date=None 时应默认为 2020-01-01。"""
        provider.fetch("ashare.kline.1d.raw.baostock", "sh.600000", None, "2024-01-05")
        call_args = mock_baostock.query_history_k_data_plus.call_args
        assert call_args[1]["start_date"] == "2020-01-01"

    def test_fetch_none_end_date_defaults_to_today(self, provider, mock_baostock):
        """end_date=None 时应默认为今天日期。"""
        from datetime import datetime
        provider.fetch("ashare.kline.1d.raw.baostock", "sh.600000", "2024-01-01", None)
        call_args = mock_baostock.query_history_k_data_plus.call_args
        today = datetime.now().strftime("%Y-%m-%d")
        assert call_args[1]["end_date"] == today

    def test_fetch_both_none_dates(self, provider, mock_baostock):
        """start_date 和 end_date 均为 None 时，应使用默认值。"""
        from datetime import datetime
        provider.fetch("ashare.kline.1d.raw.baostock", "sh.600000", None, None)
        call_args = mock_baostock.query_history_k_data_plus.call_args
        assert call_args[1]["start_date"] == "2020-01-01"
        today = datetime.now().strftime("%Y-%m-%d")
        assert call_args[1]["end_date"] == today

    def test_fetch_int_timestamp_converted(self, provider, mock_baostock):
        """整数时间戳应被转换为日期字符串。"""
        # 2024-01-01 00:00:00 UTC+8 = 1704038400000 ms
        provider.fetch("ashare.kline.1d.raw.baostock", "sh.600000", 1704038400000, 1704124800000)
        call_args = mock_baostock.query_history_k_data_plus.call_args
        assert isinstance(call_args[1]["start_date"], str)
        assert isinstance(call_args[1]["end_date"], str)


class TestTableRouting:
    """测试 table_id 路由。"""

    def test_unsupported_table_raises(self, provider):
        """不支持的 table_id 应抛出 ValueError。"""
        with pytest.raises(ValueError, match="not supported"):
            provider.fetch("unsupported.table", "sh.600000", "2024-01-01", "2024-01-05")


class TestEmptyDataDefense:
    """测试空数据防御。"""

    def test_empty_kline_returns_standardized_empty(self, provider):
        """空 K 线数据应返回含标准列类型的空 DataFrame，时区已归一化。"""
        df = provider.fetch("ashare.kline.1d.raw.baostock", "sh.600000", "2024-01-01", "2024-01-05")
        assert df.is_empty()
        # 验证核心列类型
        assert df.schema["symbol"] == pl.String
        assert df.schema["datetime"] == pl.String
        assert df.schema["timestamp"] == pl.Int64
        # 验证列顺序：symbol, datetime, timestamp 在最前
        front_cols = df.columns[:3]
        assert front_cols == ["symbol", "datetime", "timestamp"]
        # 验证数值列类型（经过 cast 转换）
        assert df.schema["open"] == pl.Float64
        assert df.schema["close"] == pl.Float64
        assert df.schema["volume"] == pl.Float64

    def test_empty_adj_factor_returns_standardized_empty(self, provider):
        """空复权因子数据应返回含标准列类型的空 DataFrame，时区已归一化。"""
        df = provider.fetch("ashare.adj_factor.baostock", "sh.600000", "2024-01-01", "2024-01-05")
        assert df.is_empty()
        # 验证核心列类型
        assert df.schema["symbol"] == pl.String
        assert df.schema["datetime"] == pl.String
        assert df.schema["timestamp"] == pl.Int64
        # 验证列顺序：symbol, datetime, timestamp 在最前
        front_cols = df.columns[:3]
        assert front_cols == ["symbol", "datetime", "timestamp"]
        # 验证数值列类型（经过 cast 转换）
        assert df.schema["back_adj_factor"] == pl.Float64
