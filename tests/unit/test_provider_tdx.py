"""TDXProvider 单元测试。

验证:
1. Provider 路由与 table_id 注册
2. get_all_symbols 返回正确结构
3. fetch 路由到正确的私有方法
4. 数据转换: polars + 标准列
5. 空数据防御
6. tdxpy reader 本地解析
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
import polars as pl

from cqdata.provider.tdx_provider import TDXProvider
from cqdata.provider.tdx_utils import (
    tdx_code_to_standard,
    standard_to_tdx_code,
    read_tdx_file_from_local,
    discover_tdx_symbols_from_local,
)
from cqdata.provider.provider_manager import ProviderManager

# 通达信默认安装路径
_VIPDOC_DIR = Path(r"C:\new_tdx\vipdoc")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_provider_manager():
    """每个测试前后清理 ProviderManager singleton，防止泄漏。"""
    ProviderManager._instance = None
    ProviderManager._providers = {}
    yield
    ProviderManager._instance = None
    ProviderManager._providers = {}


@pytest.fixture
def provider():
    """创建 TDXProvider 实例 (使用已下载的测试数据)。"""
    if not _VIPDOC_DIR.exists():
        pytest.skip("vipdoc 目录不存在，跳过测试")
    return TDXProvider(mode="local", vipdoc_dir=str(_VIPDOC_DIR))


def _skip_if_no_vipdoc():
    if not _VIPDOC_DIR.exists():
        pytest.skip("vipdoc 目录不存在，跳过测试")


# ---------------------------------------------------------------------------
# Provider 注册与路由
# ---------------------------------------------------------------------------

class TestProviderRegistration:
    """测试 ProviderManager 能正确路由到 TDXProvider。"""

    def test_provider_manager_routes_to_tdx(self):
        ProviderManager._instance = None
        ProviderManager._providers = {}
        pm = ProviderManager()
        p = pm.get_provider("ashare.kline.1d.raw.tdx")
        assert isinstance(p, TDXProvider)

    def test_supported_tables_complete(self, provider):
        tables = provider.get_supported_tables()
        expected = [
            "ashare.kline.1d.raw.tdx",
            "ashare.kline.5m.raw.tdx",
            "ashare.kline.1m.raw.tdx",
            "aindex.kline.1d.raw.tdx",
            "aindex.kline.5m.raw.tdx",
            "aindex.kline.1m.raw.tdx",
        ]
        assert set(tables) == set(expected)

    def test_unsupported_table_raises(self, provider):
        with pytest.raises(ValueError, match="not supported"):
            provider.get_table_category("ashare.kline.1d.baostock")

    def test_fetch_unsupported_table_raises(self, provider):
        with pytest.raises(ValueError, match="not supported"):
            provider.fetch("ashare.kline.1d.baostock", "sh.600000", "2024-01-01", "2024-12-31")


# ---------------------------------------------------------------------------
# get_table_category
# ---------------------------------------------------------------------------

class TestGetTableCategory:
    @pytest.mark.parametrize("table_id", [
        "ashare.kline.1d.raw.tdx",
        "ashare.kline.5m.raw.tdx",
        "ashare.kline.1m.raw.tdx",
        "aindex.kline.1d.raw.tdx",
        "aindex.kline.5m.raw.tdx",
        "aindex.kline.1m.raw.tdx",
    ])
    def test_all_tables_are_timeseries(self, provider, table_id):
        assert provider.get_table_category(table_id) == "timeseries"


# ---------------------------------------------------------------------------
# get_sort_keys
# ---------------------------------------------------------------------------

class TestGetSortKeys:
    def test_sort_keys_returns_timestamp(self, provider):
        assert provider.get_sort_keys("ashare.kline.1d.raw.tdx") == ["timestamp"]


# ---------------------------------------------------------------------------
# 代码格式转换
# ---------------------------------------------------------------------------

class TestTdxCodeConversion:
    @pytest.mark.parametrize("tdx_code,standard", [
        ("sh600000", "sh.600000"),
        ("sz000001", "sz.000001"),
        ("bj832000", "bj.832000"),
    ])
    def test_tdx_code_to_standard(self, tdx_code, standard):
        assert tdx_code_to_standard(tdx_code) == standard

    @pytest.mark.parametrize("standard,tdx_code", [
        ("sh.600000", "sh600000"),
        ("sz.000001", "sz000001"),
        ("bj.832000", "bj832000"),
    ])
    def test_standard_to_tdx_code(self, standard, tdx_code):
        assert standard_to_tdx_code(standard) == tdx_code


# ---------------------------------------------------------------------------
# tdxpy reader 本地解析
# ---------------------------------------------------------------------------

class TestTdxpyReader:
    """测试 tdxpy reader 解析本地 vipdoc 文件。"""

    def test_read_daily_file(self):
        _skip_if_no_vipdoc()
        records = read_tdx_file_from_local(_VIPDOC_DIR, "sh600000", freq="1d")
        assert len(records) > 0
        r = records[0]
        assert "date" in r
        assert "open" in r
        assert "high" in r
        assert "low" in r
        assert "close" in r
        assert "volume" in r
        assert "amount" in r

    def test_read_daily_file_date_format(self):
        _skip_if_no_vipdoc()
        records = read_tdx_file_from_local(_VIPDOC_DIR, "sh600000", freq="1d")
        assert len(records) > 0
        assert records[0]["date"] == "1999-11-10"

    def test_read_daily_file_values(self):
        _skip_if_no_vipdoc()
        records = read_tdx_file_from_local(_VIPDOC_DIR, "sh600000", freq="1d")
        assert len(records) > 0
        r = records[0]
        assert r["open"] == 29.50
        assert r["high"] == 29.80
        assert r["low"] == 27.00
        assert r["close"] == 27.75

    def test_read_minute_file(self):
        _skip_if_no_vipdoc()
        records = read_tdx_file_from_local(_VIPDOC_DIR, "sh600000", freq="1m")
        assert len(records) > 0
        r = records[0]
        assert "datetime" in r
        assert "date" in r
        assert "time" in r
        assert "open" in r
        assert "close" in r

    def test_read_minute_file_datetime_format(self):
        _skip_if_no_vipdoc()
        records = read_tdx_file_from_local(_VIPDOC_DIR, "sh600000", freq="1m")
        assert len(records) > 0
        assert records[0]["datetime"] == "2025-01-02 09:31:00"
        assert records[0]["date"] == "2025-01-02"
        assert records[0]["time"] == "09:31:00"

    def test_read_minute_file_values(self):
        _skip_if_no_vipdoc()
        records = read_tdx_file_from_local(_VIPDOC_DIR, "sh600000", freq="1m")
        assert len(records) > 0
        r = records[0]
        assert r["open"] == 10.30
        assert r["high"] == 10.42
        assert r["low"] == 10.29
        assert r["close"] == 10.38

    def test_read_nonexistent_file_returns_empty(self):
        _skip_if_no_vipdoc()
        records = read_tdx_file_from_local(_VIPDOC_DIR, "sh999999", freq="5m")
        assert records == []

    def test_read_unsupported_freq_raises(self):
        _skip_if_no_vipdoc()
        with pytest.raises(ValueError, match="不支持的频率"):
            read_tdx_file_from_local(_VIPDOC_DIR, "sh600000", freq="2m")


class TestDiscoverSymbolsLocal:
    """测试从本地 vipdoc 发现证券代码。"""

    def test_discover_all_symbols(self):
        _skip_if_no_vipdoc()
        symbols = discover_tdx_symbols_from_local(_VIPDOC_DIR)
        assert len(symbols) > 0
        assert all(len(s) >= 4 for s in symbols)

    def test_discover_sh_symbols(self):
        _skip_if_no_vipdoc()
        symbols = discover_tdx_symbols_from_local(_VIPDOC_DIR, market="sh")
        assert len(symbols) > 0
        assert all(s.startswith("sh") for s in symbols)


# ---------------------------------------------------------------------------
# fetch 数据验证
# ---------------------------------------------------------------------------

class TestFetchData:
    def test_fetch_daily_returns_polars(self, provider):
        df = provider.fetch("ashare.kline.1d.raw.tdx", "sh.600000", "2024-01-01", "2024-01-10")
        assert isinstance(df, pl.DataFrame)

    def test_fetch_daily_has_standard_columns(self, provider):
        df = provider.fetch("ashare.kline.1d.raw.tdx", "sh.600000", "2024-01-01", "2024-01-10")
        for col in ["symbol", "datetime", "timestamp", "open", "high", "low", "close", "volume", "amount"]:
            assert col in df.columns

    def test_fetch_daily_column_types(self, provider):
        df = provider.fetch("ashare.kline.1d.raw.tdx", "sh.600000", "2024-01-01", "2024-01-10")
        assert df.schema["symbol"] == pl.String
        assert df.schema["datetime"] == pl.String
        assert df.schema["timestamp"] == pl.Int64
        assert df.schema["open"] == pl.Float64
        assert df.schema["close"] == pl.Float64

    def test_fetch_daily_symbol_column(self, provider):
        df = provider.fetch("ashare.kline.1d.raw.tdx", "sh.600000", "2024-01-01", "2024-01-10")
        if not df.is_empty():
            assert (df["symbol"] == "sh.600000").all()

    def test_fetch_index_daily(self, provider):
        df = provider.fetch("aindex.kline.1d.raw.tdx", "sh.000001", "2024-01-01", "2024-01-10")
        assert isinstance(df, pl.DataFrame)
        if not df.is_empty():
            assert (df["symbol"] == "sh.000001").all()

    def test_fetch_empty_date_range(self, provider):
        df = provider.fetch("ashare.kline.1d.raw.tdx", "sh.600000", "2000-01-01", "2000-01-01")
        assert df.is_empty()
        assert list(df.columns) == ["symbol", "datetime", "timestamp", "open", "high", "low", "close", "volume", "amount"]

    def test_fetch_none_dates_use_defaults(self, provider):
        df = provider.fetch("ashare.kline.1d.raw.tdx", "sh.600000", None, None)
        assert isinstance(df, pl.DataFrame)

    def test_fetch_int_timestamp_converted(self, provider):
        df = provider.fetch("ashare.kline.1d.raw.tdx", "sh.600000", 1704038400000, 1704211200000)
        assert isinstance(df, pl.DataFrame)


# ---------------------------------------------------------------------------
# _safe_tcp_call 异常处理与 None 拦截测试
# ---------------------------------------------------------------------------

class TestSafeTcpCall:
    """测试 _safe_tcp_call 的网络故障拦截与异常重试逻辑。"""

    def test_safe_tcp_call_success(self):
        from cqdata.provider.tdx_utils import _safe_tcp_call
        mock_fn = MagicMock(return_value=[{"date": "2024-01-01"}])
        res = _safe_tcp_call(mock_fn)
        assert res == [{"date": "2024-01-01"}]

    def test_safe_tcp_call_empty_list_allowed(self):
        from cqdata.provider.tdx_utils import _safe_tcp_call
        mock_fn = MagicMock(return_value=[])
        res = _safe_tcp_call(mock_fn)
        assert res == []

    def test_safe_tcp_call_none_reconnects_and_raises(self):
        from cqdata.provider.tdx_utils import _safe_tcp_call
        mock_fn = MagicMock(return_value=None)
        with patch("cqdata.provider.tdx_utils._reconnect_tdx_api") as mock_reconnect:
            with pytest.raises(Exception):
                _safe_tcp_call(mock_fn)
            assert mock_reconnect.called

    def test_safe_tcp_call_reraises_runtime_error_on_disconnect(self):
        from cqdata.provider.tdx_utils import _safe_tcp_call
        mock_fn = MagicMock(side_effect=RuntimeError("TDX 服务器全部不可用，请检查网络"))
        with pytest.raises(RuntimeError, match="检查网络"):
            _safe_tcp_call(mock_fn)

    def test_safe_tcp_call_cached_api_none_reconnects_properly(self):
        import cqdata.provider.tdx_utils as tdx_utils
        tdx_utils._cached_api = None
        mock_api = MagicMock()
        mock_api.get_security_bars.return_value = [{"date": "2024-01-01"}]

        def _mock_connect():
            tdx_utils._cached_api = mock_api
            return mock_api

        with patch("cqdata.provider.tdx_utils._connect_tdx_api", side_effect=_mock_connect) as mock_connect:
            fn = lambda: tdx_utils._cached_api.get_security_bars(0, 0, "600000", 0, 10)
            res = tdx_utils._safe_tcp_call(fn)
            assert mock_connect.called
            assert res == [{"date": "2024-01-01"}]


# ---------------------------------------------------------------------------
# TDX Provider kwargs 与 Local 模式探针测试
# ---------------------------------------------------------------------------

class TestTDXProviderKwargs:
    """测试 ProviderManager 正确传递 provider_kwargs 并实例化 TDXProvider。"""

    def test_provider_manager_passes_kwargs_to_tdx(self):
        pm = ProviderManager()
        provider = pm.get_provider("ashare.kline.1d.raw.tdx", mode="local", vipdoc_dir="C:/custom_test_path")
        assert isinstance(provider, TDXProvider)
        assert provider._mode == "local"
        assert str(provider._vipdoc_dir).replace("\\", "/") == "C:/custom_test_path"


class TestTDXLocalModeMock:
    """测试 TDX Local 模式下的探针与数据解析。"""

    def test_local_fetch_empty_dir_returns_empty_kline(self, tmp_path):
        provider = TDXProvider(mode="local", vipdoc_dir=str(tmp_path))
        df = provider.fetch("ashare.kline.1d.raw.tdx", "sh.600000", "2024-01-01", "2024-01-10")
        assert isinstance(df, pl.DataFrame)
        assert df.is_empty()
        assert list(df.columns) == ["symbol", "datetime", "timestamp", "open", "high", "low", "close", "volume", "amount"]


