import json
import pytest
from pathlib import Path
from app.service.metadata_manager import MetadataManager


class TestMetadataManagerLoad:
    """测试 MetadataManager 加载逻辑"""

    def test_load_nonexistent_returns_empty(self, temp_storage_root):
        """加载不存在的元数据应返回空字典"""
        mgr = MetadataManager(str(temp_storage_root))
        result = mgr.load("nonexistent.table", "csv")
        assert result == {}

    def test_load_existing_returns_dict(self, temp_storage_root):
        """加载存在的元数据应返回字典"""
        mgr = MetadataManager(str(temp_storage_root))
        mgr.save("test.table", "csv", {"table_id": "test.table", "format": "csv"})

        result = mgr.load("test.table", "csv")
        assert result["table_id"] == "test.table"
        assert result["format"] == "csv"

    def test_load_corrupt_json_returns_empty(self, temp_storage_root):
        """加载损坏的 JSON 应返回空字典"""
        mgr = MetadataManager(str(temp_storage_root))
        path = mgr._get_metadata_path("test.table", "csv")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("not valid json {{{", encoding="utf-8")

        result = mgr.load("test.table", "csv")
        assert result == {}

    def test_load_empty_file_returns_empty(self, temp_storage_root):
        """加载空文件应返回空字典"""
        mgr = MetadataManager(str(temp_storage_root))
        path = mgr._get_metadata_path("test.table", "csv")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

        result = mgr.load("test.table", "csv")
        assert result == {}


class TestMetadataManagerSave:
    """测试 MetadataManager 保存逻辑"""

    def test_save_creates_file(self, temp_storage_root):
        """保存应创建元数据文件"""
        mgr = MetadataManager(str(temp_storage_root))
        mgr.save("test.table", "csv", {"key": "value"})

        path = mgr._get_metadata_path("test.table", "csv")
        assert path.exists(), "metadata.json 应被创建"

    def test_save_atomic_write(self, temp_storage_root):
        """保存应为原子化操作（不留 .tmp 文件）"""
        mgr = MetadataManager(str(temp_storage_root))
        mgr.save("test.table", "csv", {"key": "value"})

        path = mgr._get_metadata_path("test.table", "csv")
        tmp_path = path.with_suffix(".tmp")
        assert not tmp_path.exists(), ".tmp 文件应已被清理"

    def test_save_content_correct(self, temp_storage_root):
        """保存内容应与传入一致"""
        mgr = MetadataManager(str(temp_storage_root))
        data = {
            "table_id": "test.table",
            "category": "timeseries",
            "format": "csv",
            "statistics": {"total_bars": 100, "symbol_count": 10}
        }
        mgr.save("test.table", "csv", data)

        result = mgr.load("test.table", "csv")
        assert result == data

    def test_save_overwrites_existing(self, temp_storage_root):
        """保存应覆盖已有元数据"""
        mgr = MetadataManager(str(temp_storage_root))
        mgr.save("test.table", "csv", {"version": 1})
        mgr.save("test.table", "csv", {"version": 2})

        result = mgr.load("test.table", "csv")
        assert result["version"] == 2

    def test_save_creates_parent_dirs(self, temp_storage_root):
        """保存应自动创建父目录"""
        mgr = MetadataManager(str(temp_storage_root))
        mgr.save("deeply.nested.table", "csv", {"key": "value"})

        path = mgr._get_metadata_path("deeply.nested.table", "csv")
        assert path.exists()


class TestMetadataManagerPath:
    """测试元数据路径生成"""

    def test_get_metadata_path_format(self, temp_storage_root):
        """路径应符合 storage_root/{format}/{table_id}/metadata.json 格式"""
        mgr = MetadataManager(str(temp_storage_root))
        path = mgr._get_metadata_path("ashare.kline.1d.adj.baostock", "csv")
        expected = temp_storage_root / "csv" / "ashare.kline.1d.adj.baostock" / "metadata.json"
        assert path == expected

    def test_get_metadata_path_parquet(self, temp_storage_root):
        """Parquet 格式路径验证"""
        mgr = MetadataManager(str(temp_storage_root))
        path = mgr._get_metadata_path("ashare.kline.1d.adj.baostock", "parquet")
        expected = temp_storage_root / "parquet" / "ashare.kline.1d.adj.baostock" / "metadata.json"
        assert path == expected

    def test_save_load_roundtrip_csv(self, temp_storage_root):
        """CSV 格式的保存加载往返测试"""
        mgr = MetadataManager(str(temp_storage_root))
        data = {
            "table_id": "ashare.kline.1d.adj.baostock",
            "category": "timeseries",
            "format": "csv",
            "schema": {"timestamp": "Int64", "symbol": "String", "close": "Float64"},
            "statistics": {
                "start_timestamp": 1704067200000,
                "end_timestamp": 1704153600000,
                "total_bars": 2,
                "symbol_count": 1,
                "time_steps": 2
            }
        }
        mgr.save("ashare.kline.1d.adj.baostock", "csv", data)
        loaded = mgr.load("ashare.kline.1d.adj.baostock", "csv")
        assert loaded == data

    def test_save_load_roundtrip_parquet(self, temp_storage_root):
        """Parquet 格式的保存加载往返测试"""
        mgr = MetadataManager(str(temp_storage_root))
        data = {
            "table_id": "ashare.kline.1d.adj.baostock",
            "category": "timeseries",
            "format": "parquet",
            "statistics": {"total_bars": 100}
        }
        mgr.save("ashare.kline.1d.adj.baostock", "parquet", data)
        loaded = mgr.load("ashare.kline.1d.adj.baostock", "parquet")
        assert loaded == data
