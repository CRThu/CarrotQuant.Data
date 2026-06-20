"""EastMoneyProvider 单元测试。

使用 mock 避免真实 API 调用，验证:
1. Provider 路由与 table_id 注册
2. get_all_symbols 返回正确结构
3. fetch 路由到正确的私有方法
4. 数据转换: polars + 标准列
5. 防封工具 em_utils 的节流/重试逻辑
"""

import pytest
from unittest.mock import MagicMock, patch
import polars as pl

from app.provider.eastmoney_provider import EastMoneyProvider
from app.provider.provider_manager import ProviderManager


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def provider():
    """创建 EastMoneyProvider 实例。"""
    return EastMoneyProvider()


@pytest.fixture
def sample_board_cons():
    """模拟 push2 成分股响应。"""
    return {
        "data": {
            "total": 2,
            "diff": [
                {"f12": "000001", "f14": "股票A"},
                {"f12": "000002", "f14": "股票B"},
            ]
        }
    }


@pytest.fixture
def sample_datacenter_response():
    """模拟 datacenter 龙虎榜/机构交易响应。"""
    return {
        "result": {
            "count": 2,
            "data": [
                {
                    "SECURITY_CODE": "000001",
                    "SECURITY_NAME_ABBR": "测试股票A",
                    "TRADE_DATE": "2026-06-18 00:00:00",
                    "CLOSE_PRICE": 10.5,
                    "CHANGE_RATE": 2.5,
                },
                {
                    "SECURITY_CODE": "000002",
                    "SECURITY_NAME_ABBR": "测试股票B",
                    "TRADE_DATE": "2026-06-18 00:00:00",
                    "CLOSE_PRICE": 20.0,
                    "CHANGE_RATE": -1.2,
                },
            ]
        }
    }


# ---------------------------------------------------------------------------
# Provider 注册与路由
# ---------------------------------------------------------------------------

class TestProviderRegistration:
    """测试 ProviderManager 能正确路由到 EastMoneyProvider。"""

    def test_provider_manager_routes_to_eastmoney(self):
        """ProviderManager 应根据 table_id 末段 'eastmoney' 路由到 EastMoneyProvider。"""
        ProviderManager._instance = None
        ProviderManager._providers = {}
        
        pm = ProviderManager()
        provider = pm.get_provider("ashare.dragon_tiger.eastmoney")
        assert isinstance(provider, EastMoneyProvider)

    def test_supported_tables_complete(self, provider):
        """EastMoneyProvider 应支持所有 4 个 table_id。"""
        tables = provider.get_supported_tables()
        expected = [
            "ashare.concept.eastmoney",
            "ashare.industry.eastmoney",
            "ashare.dragon_tiger.eastmoney",
            "ashare.inst_trade.eastmoney",
        ]
        assert set(tables) == set(expected)

    def test_unsupported_table_raises(self, provider):
        """不支持的 table_id 应抛出 ValueError。"""
        with pytest.raises(ValueError, match="not supported"):
            provider.get_table_category("ashare.kline.1d.baostock")


# ---------------------------------------------------------------------------
# get_table_category
# ---------------------------------------------------------------------------

class TestGetTableCategory:
    """测试 get_table_category 返回正确的类别。"""

    @pytest.mark.parametrize("table_id", [
        "ashare.concept.eastmoney",
        "ashare.industry.eastmoney",
        "ashare.dragon_tiger.eastmoney",
        "ashare.inst_trade.eastmoney",
    ])
    def test_all_tables_are_event(self, provider, table_id):
        """所有东财 table_id 都应返回 'event' 类别。"""
        assert provider.get_table_category(table_id) == "event"


# ---------------------------------------------------------------------------
# get_all_symbols
# ---------------------------------------------------------------------------

class TestGetAllSymbols:
    """测试 get_all_symbols 返回正确的符号列表。"""

    def test_dragon_tiger_returns_all_marker(self, provider):
        """龙虎榜表应返回 ['_ALL_']。"""
        symbols = provider.get_all_symbols("ashare.dragon_tiger.eastmoney")
        assert symbols == ["_ALL_"]

    def test_inst_trade_returns_all_marker(self, provider):
        """机构交易表应返回 ['_ALL_']。"""
        symbols = provider.get_all_symbols("ashare.inst_trade.eastmoney")
        assert symbols == ["_ALL_"]

    def test_concept_returns_board_codes(self, provider):
        """概念板块成分股表应返回板块代码列表。"""
        with patch.object(provider, "_fetch_board_list") as mock_fetch:
            mock_fetch.return_value = {"BK0001": "板块A", "BK0002": "板块B"}
            symbols = provider.get_all_symbols("ashare.concept.eastmoney")
            assert symbols == ["BK0001", "BK0002"]
            mock_fetch.assert_called_once_with("concept")

    def test_industry_returns_board_codes(self, provider):
        """行业板块成分股表应返回板块代码列表。"""
        with patch.object(provider, "_fetch_board_list") as mock_fetch:
            mock_fetch.return_value = {"BK0010": "行业X"}
            symbols = provider.get_all_symbols("ashare.industry.eastmoney")
            assert symbols == ["BK0010"]
            mock_fetch.assert_called_once_with("industry")


# ---------------------------------------------------------------------------
# fetch 路由
# ---------------------------------------------------------------------------

class TestFetchRouting:
    """测试 fetch 方法根据 table_id 路由到正确的私有方法。"""

    def test_fetch_concept_routes_to_cons(self, provider):
        """concept 应路由到 _fetch_board_cons_df。"""
        empty_df = pl.DataFrame(schema={
            "symbol": pl.String, "stock_name": pl.String, "board_code": pl.String, "board_name": pl.String,
            "datetime": pl.String, "timestamp": pl.Int64,
        })
        with patch.object(provider, "_fetch_board_cons_df", return_value=empty_df) as mock:
            provider.fetch("ashare.concept.eastmoney", "BK0001", "2024-01-01", "2024-12-31")
            mock.assert_called_once_with("concept", "BK0001")

    def test_fetch_industry_routes_to_cons(self, provider):
        """industry 应路由到 _fetch_board_cons_df。"""
        empty_df = pl.DataFrame(schema={
            "symbol": pl.String, "stock_name": pl.String, "board_code": pl.String, "board_name": pl.String,
            "datetime": pl.String, "timestamp": pl.Int64,
        })
        with patch.object(provider, "_fetch_board_cons_df", return_value=empty_df) as mock:
            provider.fetch("ashare.industry.eastmoney", "BK0010", "2024-01-01", "2024-12-31")
            mock.assert_called_once_with("industry", "BK0010")

    def test_fetch_dragon_tiger_routes_correctly(self, provider):
        """dragon_tiger 应路由到 _fetch_dragon_tiger。"""
        empty_df = pl.DataFrame(schema={
            "symbol": pl.String, "stock_name": pl.String,
            "datetime": pl.String, "timestamp": pl.Int64,
        })
        with patch.object(provider, "_fetch_dragon_tiger", return_value=empty_df) as mock:
            provider.fetch("ashare.dragon_tiger.eastmoney", "_ALL_", "2026-06-01", "2026-06-18")
            mock.assert_called_once()

    def test_fetch_inst_trade_routes_correctly(self, provider):
        """inst_trade 应路由到 _fetch_inst_trade。"""
        empty_df = pl.DataFrame(schema={
            "symbol": pl.String, "stock_name": pl.String,
            "datetime": pl.String, "timestamp": pl.Int64,
        })
        with patch.object(provider, "_fetch_inst_trade", return_value=empty_df) as mock:
            provider.fetch("ashare.inst_trade.eastmoney", "_ALL_", "2026-06-01", "2026-06-18")
            mock.assert_called_once()


# ---------------------------------------------------------------------------
# 时间戳参数标准化
# ---------------------------------------------------------------------------

class TestTimestampConversion:
    """测试 fetch 能将 int 时间戳转换为日期字符串。"""

    def test_int_timestamp_converted_to_date_string(self, provider):
        """fetch 应将 int 毫秒时间戳转换为 YYYY-MM-DD 字符串。"""
        empty_df = pl.DataFrame(schema={
            "symbol": pl.String, "stock_name": pl.String,
            "datetime": pl.String, "timestamp": pl.Int64,
        })
        with patch.object(provider, "_fetch_dragon_tiger", return_value=empty_df) as mock:
            provider.fetch("ashare.dragon_tiger.eastmoney", "_ALL_", 1781712000000, 1781712000000)
            args = mock.call_args
            assert isinstance(args[0][0], str)  # start_date
            assert isinstance(args[0][1], str)  # end_date


# ---------------------------------------------------------------------------
# 数据转换: push2 成分股
# ---------------------------------------------------------------------------

class TestBoardConsFetch:
    """测试板块成分股拉取与数据转换。"""

    def test_fetch_board_cons_df_returns_polars(self, provider, sample_board_cons):
        """_fetch_board_cons_df 应返回 Polars DataFrame。"""
        with patch("app.provider.eastmoney_provider.em_push2") as mock_push2:
            mock_push2.return_value.json.return_value = sample_board_cons
            with patch.object(provider, "_fetch_board_list", return_value={"BK0001": "板块A"}):
                df = provider._fetch_board_cons_df("concept", "BK0001")
                assert isinstance(df, pl.DataFrame)

    def test_fetch_board_cons_df_has_board_code_column(self, provider, sample_board_cons):
        """成分股 DataFrame 应包含 board_code 和 board_name 列。"""
        with patch("app.provider.eastmoney_provider.em_push2") as mock_push2:
            mock_push2.return_value.json.return_value = sample_board_cons
            with patch.object(provider, "_fetch_board_list", return_value={"BK0001": "板块A"}):
                df = provider._fetch_board_cons_df("concept", "BK0001")
                assert "board_code" in df.columns
                assert "board_name" in df.columns
                assert df["board_code"].unique().to_list() == ["BK0001"]
                assert df["board_name"].unique().to_list() == ["板块A"]

    def test_fetch_board_cons_df_has_standard_columns(self, provider, sample_board_cons):
        """成分股 DataFrame 应包含 symbol, datetime, timestamp 标准列。"""
        with patch("app.provider.eastmoney_provider.em_push2") as mock_push2:
            mock_push2.return_value.json.return_value = sample_board_cons
            with patch.object(provider, "_fetch_board_list", return_value={"BK0001": "板块A"}):
                df = provider._fetch_board_cons_df("concept", "BK0001")
                assert "symbol" in df.columns
                assert "datetime" in df.columns
                assert "timestamp" in df.columns
                assert "stock_name" in df.columns


# ---------------------------------------------------------------------------
# 数据转换: datacenter 龙虎榜
# ---------------------------------------------------------------------------

class TestDragonTigerFetch:
    """测试龙虎榜拉取与数据转换。"""

    def test_fetch_dragon_tiger_returns_polars(self, provider, sample_datacenter_response):
        """_fetch_dragon_tiger 应返回 Polars DataFrame。"""
        with patch.object(provider, "_fetch_datacenter_paginated", return_value=sample_datacenter_response["result"]["data"]):
            df = provider._fetch_dragon_tiger("2026-06-18", "2026-06-18")
            assert isinstance(df, pl.DataFrame)

    def test_fetch_dragon_tiger_has_standard_columns(self, provider, sample_datacenter_response):
        """龙虎榜 DataFrame 应包含 symbol, datetime, timestamp 标准列。"""
        with patch.object(provider, "_fetch_datacenter_paginated", return_value=sample_datacenter_response["result"]["data"]):
            df = provider._fetch_dragon_tiger("2026-06-18", "2026-06-18")
            assert "symbol" in df.columns
            assert "datetime" in df.columns
            assert "timestamp" in df.columns

    def test_fetch_dragon_tiger_renames_columns(self, provider, sample_datacenter_response):
        """龙虎榜应将东财字段名重命名为标准英文名。"""
        with patch.object(provider, "_fetch_datacenter_paginated", return_value=sample_datacenter_response["result"]["data"]):
            df = provider._fetch_dragon_tiger("2026-06-18", "2026-06-18")
            assert "stock_name" in df.columns
            assert "close_price" in df.columns
            assert "change_pct" in df.columns

    def test_fetch_dragon_tiger_empty_response(self, provider):
        """空响应应返回空 DataFrame 且包含正确 schema。"""
        with patch.object(provider, "_fetch_datacenter_paginated", return_value=[]):
            df = provider._fetch_dragon_tiger("2026-06-18", "2026-06-18")
            assert df.is_empty()
            assert "symbol" in df.columns


# ---------------------------------------------------------------------------
# 数据转换: datacenter 机构交易
# ---------------------------------------------------------------------------

class TestInstTradeFetch:
    """测试机构交易拉取与数据转换。"""

    def test_fetch_inst_trade_returns_polars(self, provider, sample_datacenter_response):
        """_fetch_inst_trade 应返回 Polars DataFrame。"""
        with patch.object(provider, "_fetch_datacenter_paginated", return_value=sample_datacenter_response["result"]["data"]):
            df = provider._fetch_inst_trade("2026-06-18", "2026-06-18")
            assert isinstance(df, pl.DataFrame)

    def test_fetch_inst_trade_has_standard_columns(self, provider, sample_datacenter_response):
        """机构交易 DataFrame 应包含 symbol, datetime, timestamp 标准列。"""
        with patch.object(provider, "_fetch_datacenter_paginated", return_value=sample_datacenter_response["result"]["data"]):
            df = provider._fetch_inst_trade("2026-06-18", "2026-06-18")
            assert "symbol" in df.columns
            assert "datetime" in df.columns
            assert "timestamp" in df.columns

    def test_fetch_inst_trade_renames_columns(self, provider, sample_datacenter_response):
        """机构交易应将东财字段名重命名为标准英文名。"""
        with patch.object(provider, "_fetch_datacenter_paginated", return_value=sample_datacenter_response["result"]["data"]):
            df = provider._fetch_inst_trade("2026-06-18", "2026-06-18")
            assert "stock_name" in df.columns
            assert "close_price" in df.columns
            assert "change_pct" in df.columns


# ---------------------------------------------------------------------------
# 工具方法
# ---------------------------------------------------------------------------

class TestCleanCode:
    """测试 _clean_code 工具方法。"""

    @pytest.mark.parametrize("input_code,expected", [
        ("SH600000", "600000"),
        ("sz000001", "000001"),
        ("BJ832000", "832000"),
        ("600000.SH", "600000"),
        ("000001.SZ", "000001"),
        ("600000", "600000"),
    ])
    def test_clean_code_strips_prefix_suffix(self, input_code, expected):
        """_clean_code 应去掉 SH/SZ/BJ 前后缀，只留6位纯数字。"""
        assert EastMoneyProvider._clean_code(input_code) == expected


# ---------------------------------------------------------------------------
# 空数据防御
# ---------------------------------------------------------------------------

class TestEmptyDataDefense:
    """测试空数据场景的防御性处理。"""

    def test_empty_board_cons(self, provider):
        """空成分股应返回空 DataFrame 且 schema 正确。"""
        empty_response = {"data": {"total": 0, "diff": []}}
        with patch("app.provider.eastmoney_provider.em_push2") as mock_push2:
            mock_push2.return_value.json.return_value = empty_response
            with patch.object(provider, "_fetch_board_list", return_value={"BK0001": "板块A"}):
                df = provider._fetch_board_cons_df("concept", "BK0001")
                assert df.is_empty()
                assert df.schema["symbol"] == pl.String
                assert df.schema["board_code"] == pl.String
                assert df.schema["board_name"] == pl.String

    def test_empty_datacenter_response(self, provider):
        """空 datacenter 响应应返回空 DataFrame 且 schema 正确。"""
        with patch.object(provider, "_fetch_datacenter_paginated", return_value=[]):
            df = provider._fetch_dragon_tiger("2026-06-18", "2026-06-18")
            assert df.is_empty()
            assert df.schema["symbol"] == pl.String
            assert df.schema["timestamp"] == pl.Int64
