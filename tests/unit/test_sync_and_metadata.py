import pytest
import polars as pl
from pathlib import Path
from unittest.mock import MagicMock, patch
from app.service.sync_manager import SyncManager
from app.service.metadata_manager import MetadataManager
from app.storage.csv_storage import CSVStorage
from app.storage.parquet_storage import ParquetStorage
from app.provider.base import BaseProvider
from app.utils.time_utils import parse_date_to_ts, align_to_day_end


class FakeProvider(BaseProvider):
    """模拟数据源驱动，用于元数据一致性测试"""

    def __init__(self, symbols=None, data_map=None):
        self._symbols = symbols or ["sh.600000", "sz.000001"]
        self._data_map = data_map or {}

    def get_supported_tables(self):
        return ["test.table"]

    def get_all_symbols(self, table_id):
        return self._symbols

    def get_table_category(self, table_id):
        return "timeseries"

    def get_sort_keys(self, table_id):
        return ["timestamp"]

    def fetch(self, table_id, symbol, start_date, end_date, **kwargs):
        if symbol in self._data_map:
            return self._data_map[symbol]
        ts_list = [start_date, end_date]
        return pl.DataFrame({
            "timestamp": ts_list,
            "datetime": ["2024-01-01T00:00:00.000+08:00", "2024-01-02T00:00:00.000+08:00"],
            "symbol": [symbol] * 2,
            "close": [10.0, 11.0]
        })


class FakeEVProvider(BaseProvider):
    """模拟事件类型数据源驱动"""

    def __init__(self, symbols=None, data_df=None):
        self._symbols = symbols or ["sh.600000"]
        self._data_df = data_df

    def get_supported_tables(self):
        return ["test.ev.table"]

    def get_all_symbols(self, table_id):
        return self._symbols

    def get_table_category(self, table_id):
        return "event"

    def get_sort_keys(self, table_id):
        return ["timestamp", "symbol"]

    def fetch(self, table_id, symbol, start_date, end_date, **kwargs):
        if self._data_df is not None:
            return self._data_df
        return pl.DataFrame({
            "timestamp": [start_date, end_date],
            "datetime": ["2024-01-01T00:00:00.000+08:00", "2024-01-02T00:00:00.000+08:00"],
            "symbol": [symbol] * 2,
            "back_adj_factor": [1.0, 1.1]
        })


def test_metadata_and_sync_flow_csv(temp_storage_root):
    """
    测试元数据与同步流程的闭环集成（CSV 格式）
    通过 SyncManager 完整流程写入数据，验证物理统计与元数据的一致性
    """
    table_id = "test.metadata.sync.csv"

    with patch("app.config.settings.settings.STORAGE_ROOT", str(temp_storage_root)):
        sync_mgr = SyncManager()
        fake_provider = FakeProvider()

        with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=fake_provider):
            sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-02")

            # 验证元数据已创建
            metadata = sync_mgr.metadata_mgr.load(table_id, "csv")
            assert metadata, "元数据应该已通过 _update_metadata 创建"
            assert "statistics" in metadata, "元数据应包含 statistics 字段"

            stats = metadata["statistics"]
            assert stats["total_bars"] > 0, "total_bars 应大于 0"
            assert stats["symbol_count"] == 2, "symbol_count 应为 2"
            assert stats["start_timestamp"] > 0, "start_timestamp 应大于 0"
            assert stats["end_timestamp"] > 0, "end_timestamp 应大于 0"
            assert "start_datetime" in stats, "应包含 start_datetime"
            assert "end_datetime" in stats, "应包含 end_datetime"
            assert "time_steps" in stats, "TS 类型应包含 time_steps"

            # 验证物理统计与元数据一致
            storage = CSVStorage(str(temp_storage_root / "csv"))
            assert storage.get_total_bars(table_id) == stats["total_bars"]
            assert len(storage.get_all_symbols(table_id)) == stats["symbol_count"]


def test_metadata_and_sync_flow_parquet(temp_storage_root):
    """
    测试元数据与同步流程的闭环集成（Parquet 格式）
    """
    table_id = "test.metadata.sync.parquet"

    with patch("app.config.settings.settings.STORAGE_ROOT", str(temp_storage_root)):
        sync_mgr = SyncManager()
        fake_provider = FakeProvider(symbols=["sh.600000", "sz.000001", "sz.000002"])

        with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=fake_provider):
            sync_mgr.sync(table_id, "parquet", "2024-01-01", "2024-01-02")

            metadata = sync_mgr.metadata_mgr.load(table_id, "parquet")
            assert metadata, "元数据应该已创建"
            stats = metadata["statistics"]
            assert stats["total_bars"] == 6, f"3 symbols * 2 rows = 6, got {stats['total_bars']}"
            assert stats["symbol_count"] == 3
            assert "time_steps" in stats

            # 验证物理统计与元数据一致
            storage = ParquetStorage(str(temp_storage_root / "parquet"))
            assert storage.get_total_bars(table_id) == stats["total_bars"]
            assert len(storage.get_all_symbols(table_id)) == stats["symbol_count"]
            time_range = storage.get_global_time_range(table_id)
            assert stats["start_timestamp"] == time_range[0]
            assert stats["end_timestamp"] == time_range[1]


def test_metadata_physical_stats_consistency(temp_storage_root):
    """
    测试元数据与物理统计的一致性
    """
    table_id = "test.metadata.consistency"

    with patch("app.config.settings.settings.STORAGE_ROOT", str(temp_storage_root)):
        sync_mgr = SyncManager()
        fake_provider = FakeProvider()

        with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=fake_provider):
            sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-02")

            metadata = sync_mgr.metadata_mgr.load(table_id, "csv")
            stats = metadata["statistics"]

            # 重新通过物理巡检获取统计
            storage = CSVStorage(str(temp_storage_root / "csv"))
            physical_total = storage.get_total_bars(table_id)
            physical_symbols = storage.get_all_symbols(table_id)
            physical_timestamps = storage.get_unique_timestamps(table_id)

            assert stats["total_bars"] == physical_total, "元数据 total_bars 应与物理统计完全一致"
            assert stats["symbol_count"] == len(physical_symbols), "元数据 symbol_count 应与物理统计完全一致"
            assert stats["time_steps"] == len(physical_timestamps), "元数据 time_steps 应与物理统计完全一致"


def test_metadata_schema_validation(temp_storage_root):
    """
    测试元数据 schema 字段的正确性
    """
    table_id = "test.metadata.schema"

    with patch("app.config.settings.settings.STORAGE_ROOT", str(temp_storage_root)):
        sync_mgr = SyncManager()
        fake_provider = FakeProvider()

        with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=fake_provider):
            sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-02")

            metadata = sync_mgr.metadata_mgr.load(table_id, "csv")
            assert "schema" in metadata, "元数据应包含 schema 字段"

            schema = metadata["schema"]
            assert "timestamp" in schema, "schema 应包含 timestamp 字段"
            assert "symbol" in schema, "schema 应包含 symbol 字段"
            assert "datetime" in schema, "schema 应包含 datetime 字段"
            assert "close" in schema, "schema 应包含 close 字段"

            # 验证 schema 类型映射正确
            assert schema["timestamp"] == "Int64"
            assert schema["symbol"] == "String"
            assert schema["close"] == "Float64"


def test_metadata_time_range_accuracy(temp_storage_root):
    """
    测试元数据时间范围的准确性
    """
    table_id = "test.metadata.time_range"
    start_ts = parse_date_to_ts("2024-01-01")
    end_ts = parse_date_to_ts("2024-01-02")

    with patch("app.config.settings.settings.STORAGE_ROOT", str(temp_storage_root)):
        sync_mgr = SyncManager()
        fake_provider = FakeProvider()

        with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=fake_provider):
            sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-02")

            metadata = sync_mgr.metadata_mgr.load(table_id, "csv")
            stats = metadata["statistics"]

            assert stats["start_timestamp"] == start_ts, \
                f"元数据 start_timestamp 应为 {start_ts}，实际 {stats['start_timestamp']}"
            assert stats["end_timestamp"] == align_to_day_end(end_ts), \
                f"元数据 end_timestamp 应为 {align_to_day_end(end_ts)}，实际 {stats['end_timestamp']}"


def test_metadata_empty_storage(temp_storage_root):
    """
    测试空存储的元数据处理
    """
    table_id = "test.metadata.empty"

    metadata_mgr = MetadataManager(str(temp_storage_root))
    metadata = metadata_mgr.load(table_id, "csv")

    assert metadata == {}, "空存储的元数据应该返回空字典"


def test_metadata_multiple_formats_consistency(temp_storage_root):
    """
    测试多格式元数据的一致性
    同样的数据同步到 CSV 和 Parquet，验证物理统计应该一致
    """
    table_id = "test.metadata.multi_format"

    with patch("app.config.settings.settings.STORAGE_ROOT", str(temp_storage_root)):
        sync_mgr = SyncManager()
        fake_provider = FakeProvider()

        with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=fake_provider):
            sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-02")
            sync_mgr.sync(table_id, "parquet", "2024-01-01", "2024-01-02")

            csv_metadata = sync_mgr.metadata_mgr.load(table_id, "csv")
            parquet_metadata = sync_mgr.metadata_mgr.load(table_id, "csv")

            csv_stats = csv_metadata["statistics"]
            parquet_stats = parquet_metadata["statistics"]

            assert csv_stats["total_bars"] == parquet_stats["total_bars"], \
                "CSV 和 Parquet 的 total_bars 应该一致"
            assert csv_stats["symbol_count"] == parquet_stats["symbol_count"], \
                "CSV 和 Parquet 的 symbol 列表应该一致"
