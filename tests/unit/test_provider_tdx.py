"""TDXProvider 单元测试。

验证:
1. Provider 路由与 table_id 注册
2. get_all_symbols 返回正确结构
3. fetch 路由到正确的私有方法
4. 数据转换: polars + 标准列
5. 空数据防御
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
import polars as pl

from app.provider.tdx_provider import TDXProvider
from app.provider.tdx_utils import (
    parse_tdx_day_data,
    parse_tdx_min_data,
    tdx_code_to_standard,
    standard_to_tdx_code,
    discover_tdx_symbols,
)
from app.provider.provider_manager import ProviderManager


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
    data_dir = Path(__file__).resolve().parents[2] / "tdx_data"
    if not (data_dir / "hsjday.zip").exists():
        pytest.skip("TDX 数据文件不存在，跳过测试")
    return TDXProvider(data_dir=str(data_dir))


@pytest.fixture
def sample_day_binary():
    """构造最小的日线二进制数据 (1条记录)。"""
    import struct
    # 日期: 2024-01-01 = 20240101
    date_int = 20240101
    open_price = int(10.50 * 100)   # 1050
    high_price = int(11.00 * 100)   # 1100
    low_price = int(10.00 * 100)    # 1000
    close_price = int(10.80 * 100)  # 1080
    amount = 1000000.0
    volume = 100000
    reserved = 0

    record = struct.pack('<IIIIIfII',
                         date_int, open_price, high_price, low_price,
                         close_price, amount, volume, reserved)
    return record


@pytest.fixture
def sample_min_binary():
    """构造最小的分钟线二进制数据 (1条记录)。"""
    import struct
    # 日期: 2024-01-01 = 20240101
    date_int = 20240101
    # 时间: 09:30:00 = 93000
    time_int = 93000
    open_price = int(10.50 * 100)
    high_price = int(11.00 * 100)
    low_price = int(10.00 * 100)
    close_price = int(10.80 * 100)
    amount = float(100000.0)
    volume = 10000

    record = struct.pack('<IIIIIIfI',
                         date_int, time_int, open_price, high_price,
                         low_price, close_price, amount, volume)
    return record


# ---------------------------------------------------------------------------
# Provider 注册与路由
# ---------------------------------------------------------------------------

class TestProviderRegistration:
    """测试 ProviderManager 能正确路由到 TDXProvider。"""

    def test_provider_manager_routes_to_tdx(self):
        """ProviderManager 应根据 table_id 末段 'tdx' 路由到 TDXProvider。"""
        ProviderManager._instance = None
        ProviderManager._providers = {}

        pm = ProviderManager()
        p = pm.get_provider("ashare.kline.1d.tdx")
        assert isinstance(p, TDXProvider)

    def test_supported_tables_complete(self, provider):
        """TDXProvider 应支持所有注册的 table_id。"""
        tables = provider.get_supported_tables()
        expected = [
            "ashare.kline.1d.tdx",
            "ashare.kline.5m.tdx",
            "ashare.kline.1m.tdx",
            "aindex.kline.1d.tdx",
            "aindex.kline.5m.tdx",
            "aindex.kline.1m.tdx",
        ]
        assert set(tables) == set(expected)

    def test_unsupported_table_raises(self, provider):
        """不支持的 table_id 应抛出 ValueError。"""
        with pytest.raises(ValueError, match="not supported"):
            provider.get_table_category("ashare.kline.1d.baostock")

    def test_fetch_unsupported_table_raises(self, provider):
        """fetch 不支持的 table_id 应抛出 ValueError。"""
        with pytest.raises(ValueError, match="not supported"):
            provider.fetch("ashare.kline.1d.baostock", "sh.600000", "2024-01-01", "2024-12-31")


# ---------------------------------------------------------------------------
# get_table_category
# ---------------------------------------------------------------------------

class TestGetTableCategory:
    """测试 get_table_category 返回正确的类别。"""

    @pytest.mark.parametrize("table_id", [
        "ashare.kline.1d.tdx",
        "ashare.kline.5m.tdx",
        "ashare.kline.1m.tdx",
        "aindex.kline.1d.tdx",
        "aindex.kline.5m.tdx",
        "aindex.kline.1m.tdx",
    ])
    def test_all_tables_are_timeseries(self, provider, table_id):
        """所有 TDX table_id 都应返回 'timeseries' 类别。"""
        assert provider.get_table_category(table_id) == "timeseries"


# ---------------------------------------------------------------------------
# get_sort_keys
# ---------------------------------------------------------------------------

class TestGetSortKeys:
    """测试 get_sort_keys 返回正确的排序列。"""

    def test_sort_keys_returns_timestamp(self, provider):
        """TDX 时序表应返回 ['timestamp'] 作为排序键。"""
        assert provider.get_sort_keys("ashare.kline.1d.tdx") == ["timestamp"]


# ---------------------------------------------------------------------------
# 工具方法
# ---------------------------------------------------------------------------

class TestTdxCodeConversion:
    """测试代码格式转换。"""

    @pytest.mark.parametrize("tdx_code,standard", [
        ("sh600000", "sh.600000"),
        ("sz000001", "sz.000001"),
        ("bj832000", "bj.832000"),
    ])
    def test_tdx_code_to_standard(self, tdx_code, standard):
        """tdx_code_to_standard 应正确转换。"""
        assert tdx_code_to_standard(tdx_code) == standard

    @pytest.mark.parametrize("standard,tdx_code", [
        ("sh.600000", "sh600000"),
        ("sz.000001", "sz000001"),
        ("bj.832000", "bj832000"),
    ])
    def test_standard_to_tdx_code(self, standard, tdx_code):
        """standard_to_tdx_code 应正确转换。"""
        assert standard_to_tdx_code(standard) == tdx_code


# ---------------------------------------------------------------------------
# 二进制解析
# ---------------------------------------------------------------------------

class TestParseBinary:
    """测试二进制数据解析。"""

    def test_parse_day_data(self, sample_day_binary):
        """parse_tdx_day_data 应正确解析日线二进制数据。"""
        records = parse_tdx_day_data(sample_day_binary)
        assert len(records) == 1
        r = records[0]
        assert r['date'] == '2024-01-01'
        assert r['open'] == 10.50
        assert r['high'] == 11.00
        assert r['low'] == 10.00
        assert r['close'] == 10.80
        assert r['volume'] == 100000
        assert r['amount'] == 1000000.0

    def test_parse_min_data(self, sample_min_binary):
        """parse_tdx_min_data 应正确解析分钟线二进制数据。"""
        records = parse_tdx_min_data(sample_min_binary, freq=1)
        assert len(records) == 1
        r = records[0]
        assert r['date'] == '2024-01-01'
        assert r['time'] == '09:30:00'
        assert r['datetime'] == '2024-01-01 09:30:00'
        assert r['open'] == 10.50
        assert r['close'] == 10.80

    def test_parse_empty_data(self):
        """空数据应返回空列表。"""
        assert parse_tdx_day_data(b'') == []
        assert parse_tdx_min_data(b'', freq=1) == []


# ---------------------------------------------------------------------------
# fetch 数据验证
# ---------------------------------------------------------------------------

class TestFetchData:
    """测试 fetch 方法返回正确的数据格式。"""

    def test_fetch_daily_returns_polars(self, provider):
        """fetch 日线应返回 Polars DataFrame。"""
        df = provider.fetch("ashare.kline.1d.tdx", "sh.600000", "2024-01-01", "2024-01-10")
        assert isinstance(df, pl.DataFrame)

    def test_fetch_daily_has_standard_columns(self, provider):
        """fetch 日线应包含 symbol, datetime, timestamp 标准列。"""
        df = provider.fetch("ashare.kline.1d.tdx", "sh.600000", "2024-01-01", "2024-01-10")
        assert "symbol" in df.columns
        assert "datetime" in df.columns
        assert "timestamp" in df.columns
        assert "open" in df.columns
        assert "high" in df.columns
        assert "low" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns
        assert "amount" in df.columns

    def test_fetch_daily_column_types(self, provider):
        """fetch 日线列类型应正确。"""
        df = provider.fetch("ashare.kline.1d.tdx", "sh.600000", "2024-01-01", "2024-01-10")
        assert df.schema["symbol"] == pl.String
        assert df.schema["datetime"] == pl.String
        assert df.schema["timestamp"] == pl.Int64
        assert df.schema["open"] == pl.Float64
        assert df.schema["close"] == pl.Float64

    def test_fetch_daily_symbol_column(self, provider):
        """fetch 日线 symbol 列应与请求的 symbol 一致。"""
        df = provider.fetch("ashare.kline.1d.tdx", "sh.600000", "2024-01-01", "2024-01-10")
        if not df.is_empty():
            assert (df["symbol"] == "sh.600000").all()

    def test_fetch_index_daily(self, provider):
        """fetch 指数日线应返回正确数据。"""
        df = provider.fetch("aindex.kline.1d.tdx", "sh.000001", "2024-01-01", "2024-01-10")
        assert isinstance(df, pl.DataFrame)
        assert "symbol" in df.columns
        if not df.is_empty():
            assert (df["symbol"] == "sh.000001").all()

    def test_fetch_empty_date_range(self, provider):
        """fetch 不存在的日期范围应返回空 DataFrame 且 schema 保持完整。"""
        df = provider.fetch("ashare.kline.1d.tdx", "sh.600000", "2000-01-01", "2000-01-01")
        assert df.is_empty()
        assert list(df.columns) == ["symbol", "datetime", "timestamp", "open", "high", "low", "close", "volume", "amount"]

    def test_fetch_none_dates_use_defaults(self, provider):
        """start_date/end_date 为 None 时应使用默认值。"""
        # 不应抛出异常
        df = provider.fetch("ashare.kline.1d.tdx", "sh.600000", None, None)
        assert isinstance(df, pl.DataFrame)

    def test_fetch_int_timestamp_converted(self, provider):
        """整数时间戳应被转换为日期字符串。"""
        # 2024-01-01 00:00:00 UTC+8 = 1704038400000 ms
        df = provider.fetch("ashare.kline.1d.tdx", "sh.600000", 1704038400000, 1704211200000)
        assert isinstance(df, pl.DataFrame)
