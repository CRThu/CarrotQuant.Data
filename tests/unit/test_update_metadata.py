import pytest
import polars as pl
from pathlib import Path
from unittest.mock import MagicMock, patch
from app.service.sync_manager import SyncManager
from app.service.metadata_manager import MetadataManager
from app.storage.csv_storage import CSVStorage
from app.storage.parquet_storage import ParquetStorage


class FakeTSProvider:
    """模拟 TS 类型 Provider"""

    def __init__(self, symbols=None):
        self._symbols = symbols or ["sh.600000"]

    def get_supported_tables(self):
        return ["test.table"]

    def get_all_symbols(self, table_id):
        return self._symbols

    def get_table_category(self, table_id):
        return "timeseries"

    def fetch(self, table_id, symbol, start_date, end_date, **kwargs):
        return pl.DataFrame({
            "timestamp": [start_date, end_date],
            "datetime": ["2024-01-01T00:00:00.000+08:00", "2024-01-02T00:00:00.000+08:00"],
            "symbol": [symbol] * 2,
            "close": [10.0, 11.0]
        })


class FakeEVProvider:
    """模拟 EV 类型 Provider"""

    def __init__(self, symbols=None):
        self._symbols = symbols or ["sh.600000"]

    def get_supported_tables(self):
        return ["test.ev.table"]

    def get_all_symbols(self, table_id):
        return self._symbols

    def get_table_category(self, table_id):
        return "event"

    def fetch(self, table_id, symbol, start_date, end_date, **kwargs):
        return pl.DataFrame({
            "timestamp": [start_date, end_date],
            "datetime": ["2024-01-01T00:00:00.000+08:00", "2024-01-02T00:00:00.000+08:00"],
            "symbol": [symbol] * 2,
            "back_adj_factor": [1.0, 1.1]
        })


class FakeEmptyProvider:
    """模拟返回空数据的 Provider"""

    def get_supported_tables(self):
        return ["test.empty"]

    def get_all_symbols(self, table_id):
        return ["sh.600000"]

    def get_table_category(self, table_id):
        return "timeseries"

    def fetch(self, table_id, symbol, start_date, end_date, **kwargs):
        return pl.DataFrame({
            "timestamp": [],
            "datetime": [],
            "symbol": [],
            "close": []
        }).cast({"timestamp": pl.Int64, "datetime": pl.String, "symbol": pl.String, "close": pl.Float64})


class TestUpdateMetadataTS:
    """测试 _update_metadata 的 TS 类型逻辑"""

    def test_ts_metadata_has_symbol_count_and_time_steps(self, temp_storage_root):
        """TS 类型元数据应包含 symbol_count 和 time_steps"""
        table_id = "test.update.ts.stats"

        with patch("app.config.settings.settings.STORAGE_ROOT", str(temp_storage_root)):
            sync_mgr = SyncManager()
            provider = FakeTSProvider(symbols=["sh.600000", "sz.000001"])

            with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=provider):
                sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-02")

                metadata = sync_mgr.metadata_mgr.load(table_id, "csv")
                stats = metadata["statistics"]

                assert stats["symbol_count"] == 2
                assert "time_steps" in stats
                assert stats["time_steps"] >= 1

    def test_ts_metadata_structure(self, temp_storage_root):
        """TS 类型元数据应包含完整的元信息字段"""
        table_id = "test.update.ts.structure"

        with patch("app.config.settings.settings.STORAGE_ROOT", str(temp_storage_root)):
            sync_mgr = SyncManager()
            provider = FakeTSProvider()

            with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=provider):
                sync_mgr.sync(table_id, "parquet", "2024-01-01", "2024-01-02")

                metadata = sync_mgr.metadata_mgr.load(table_id, "parquet")

                assert metadata["table_id"] == table_id
                assert metadata["category"] == "timeseries"
                assert metadata["format"] == "parquet"
                assert "partition" in metadata
                assert "layout" in metadata
                assert "schema" in metadata
                assert "statistics" in metadata

                stats = metadata["statistics"]
                assert "start_timestamp" in stats
                assert "end_timestamp" in stats
                assert "start_datetime" in stats
                assert "end_datetime" in stats
                assert "total_bars" in stats
                assert "symbol_count" in stats
                assert "time_steps" in stats


class TestUpdateMetadataEV:
    """测试 _update_metadata 的 EV 类型逻辑"""

    def test_ev_metadata_no_symbol_count_no_time_steps(self, temp_storage_root):
        """EV 类型元数据不应包含 symbol_count 和 time_steps"""
        table_id = "test.update.ev.structure"

        with patch("app.config.settings.settings.STORAGE_ROOT", str(temp_storage_root)):
            sync_mgr = SyncManager()
            provider = FakeEVProvider()

            with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=provider):
                sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-02")

                metadata = sync_mgr.metadata_mgr.load(table_id, "csv")
                stats = metadata["statistics"]

                assert metadata["category"] == "event"
                assert "symbol_count" not in stats, "EV 类型不应包含 symbol_count"
                assert "time_steps" not in stats, "EV 类型不应包含 time_steps"
                assert "total_bars" in stats
                assert "start_timestamp" in stats
                assert "end_timestamp" in stats

    def test_ev_metadata_structure(self, temp_storage_root):
        """EV 类型元数据基础字段验证"""
        table_id = "test.update.ev.basic"

        with patch("app.config.settings.settings.STORAGE_ROOT", str(temp_storage_root)):
            sync_mgr = SyncManager()
            provider = FakeEVProvider()

            with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=provider):
                sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-02")

                metadata = sync_mgr.metadata_mgr.load(table_id, "csv")
                assert metadata["table_id"] == table_id
                assert metadata["category"] == "event"


class TestUpdateMetadataSilent:
    """测试 _update_metadata 静默拦截逻辑"""

    def test_initial_sync_no_data_no_metadata(self, temp_storage_root):
        """场景 A：初次同步无数据 → 不创建 metadata.json"""
        table_id = "test.update.silent.initial"

        with patch("app.config.settings.settings.STORAGE_ROOT", str(temp_storage_root)):
            sync_mgr = SyncManager()
            provider = FakeEmptyProvider()

            with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=provider):
                sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-02")

                metadata = sync_mgr.metadata_mgr.load(table_id, "csv")
                assert metadata == {}, "初次同步无数据不应创建元数据"

                # 验证文件不存在
                path = sync_mgr.metadata_mgr._get_metadata_path(table_id, "csv")
                assert not path.exists(), "metadata.json 文件不应存在"

    def test_incremental_sync_no_data_keeps_existing(self, temp_storage_root):
        """场景 B：增量同步无数据但有现有元数据 → 保持不变"""
        table_id = "test.update.silent.incremental"

        with patch("app.config.settings.settings.STORAGE_ROOT", str(temp_storage_root)):
            sync_mgr = SyncManager()

            # 手动注入数据和元数据
            storage = CSVStorage(str(temp_storage_root / "csv"))
            test_df = pl.DataFrame({
                "timestamp": [1704067200000],
                "datetime": ["2024-01-01T00:00:00.000+08:00"],
                "symbol": ["sh.600000"],
                "close": [10.0]
            })
            storage.write_series(table_id, test_df)

            initial_metadata = {
                "table_id": table_id,
                "format": "csv",
                "category": "timeseries",
                "schema": {k: str(v) for k, v in test_df.schema.items()},
                "statistics": {"total_bars": 1, "symbol_count": 1}
            }
            sync_mgr.metadata_mgr.save(table_id, "csv", initial_metadata)

            # 记录初始状态
            metadata_path = sync_mgr.metadata_mgr._get_metadata_path(table_id, "csv")
            initial_mtime = metadata_path.stat().st_mtime
            initial_content = metadata_path.read_text(encoding="utf-8")

            # Mock 返回空数据
            empty_provider = FakeEmptyProvider()
            empty_provider._symbols = ["sh.600000"]

            with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=empty_provider):
                # 增量同步请求未来日期（无数据）
                sync_mgr.sync(table_id, "csv", "2099-01-01", "2099-01-02")

                # 验证元数据未变
                assert metadata_path.stat().st_mtime == initial_mtime
                assert metadata_path.read_text(encoding="utf-8") == initial_content

    def test_no_new_data_with_existing_metadata_skips_update(self, temp_storage_root):
        """无新数据写入且已有元数据 → 跳过元数据更新"""
        table_id = "test.update.silent.no_new"

        with patch("app.config.settings.settings.STORAGE_ROOT", str(temp_storage_root)):
            sync_mgr = SyncManager()

            # 手动注入数据和元数据
            storage = CSVStorage(str(temp_storage_root / "csv"))
            test_df = pl.DataFrame({
                "timestamp": [1704067200000],
                "datetime": ["2024-01-01T00:00:00.000+08:00"],
                "symbol": ["sh.600000"],
                "close": [10.0]
            })
            storage.write_series(table_id, test_df)

            initial_metadata = {
                "table_id": table_id,
                "format": "csv",
                "category": "timeseries",
                "schema": {k: str(v) for k, v in test_df.schema.items()},
                "statistics": {"total_bars": 1, "symbol_count": 1, "version": "old"}
            }
            sync_mgr.metadata_mgr.save(table_id, "csv", initial_metadata)

            metadata_path = sync_mgr.metadata_mgr._get_metadata_path(table_id, "csv")
            initial_mtime = metadata_path.stat().st_mtime

            # Mock Provider 返回空数据（相同日期范围，无新数据需要拉取）
            empty_provider = FakeEmptyProvider()
            with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=empty_provider):
                sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-01")

                # 验证元数据未变
                assert metadata_path.stat().st_mtime == initial_mtime


class TestUpdateMetadataForceRefresh:
    """测试 _update_metadata force_refresh 逻辑"""

    def test_force_refresh_updates_metadata(self, temp_storage_root):
        """force_refresh=True 时应强制更新元数据"""
        table_id = "test.update.force"

        with patch("app.config.settings.settings.STORAGE_ROOT", str(temp_storage_root)):
            sync_mgr = SyncManager()
            provider = FakeTSProvider()

            with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=provider):
                # 第一次同步
                sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-02")
                first_metadata = sync_mgr.metadata_mgr.load(table_id, "csv")

                # force_refresh 同步
                sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-02", force_refresh=True)
                second_metadata = sync_mgr.metadata_mgr.load(table_id, "csv")

                # 元数据应存在且包含正确统计
                assert second_metadata, "force_refresh 后元数据应存在"
                assert second_metadata["statistics"]["total_bars"] > 0


class TestUpdateMetadataSchema:
    """测试 _update_metadata schema 提取逻辑"""

    def test_schema_extracted_from_last_success_df(self, temp_storage_root):
        """schema 应从最后一次成功的 DataFrame 中提取"""
        table_id = "test.update.schema"

        with patch("app.config.settings.settings.STORAGE_ROOT", str(temp_storage_root)):
            sync_mgr = SyncManager()
            provider = FakeTSProvider()

            with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=provider):
                sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-02")

                metadata = sync_mgr.metadata_mgr.load(table_id, "csv")
                schema = metadata.get("schema", {})

                assert "timestamp" in schema
                assert "symbol" in schema
                assert "close" in schema
                assert schema["timestamp"] == "Int64"
                assert schema["symbol"] == "String"
                assert schema["close"] == "Float64"

    def test_schema_preserves_across_updates(self, temp_storage_root):
        """多次更新后 schema 应保持一致"""
        table_id = "test.update.schema.preserve"

        with patch("app.config.settings.settings.STORAGE_ROOT", str(temp_storage_root)):
            sync_mgr = SyncManager()
            provider = FakeTSProvider()

            with patch.object(sync_mgr.provider_mgr, 'get_provider', return_value=provider):
                sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-02")
                first_schema = sync_mgr.metadata_mgr.load(table_id, "csv")["schema"]

                sync_mgr.sync(table_id, "csv", "2024-01-01", "2024-01-05")
                second_schema = sync_mgr.metadata_mgr.load(table_id, "csv")["schema"]

                assert first_schema == second_schema, "schema 应跨更新保持一致"
