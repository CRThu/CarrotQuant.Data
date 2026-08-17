import json
import os
from pathlib import Path

from cq.data.config.settings import settings

class MetadataManager:
    """元数据 IO 管理类，负责 metadata.json 的读取和原子化保存 (包含 version: 1 版本控制)"""

    def __init__(self, data_dir: str = None):
        target_dir = data_dir or settings.data_dir
        self.data_dir = Path(target_dir)

    def _get_metadata_path(self, table_id: str, format: str) -> Path:
        """元数据路径：data_dir/{format}/{table_id}/metadata.json"""
        return self.data_dir / format / table_id / "metadata.json"

    def load(self, table_id: str, format: str) -> dict:
        """加载元数据，若不存在则返回全空结构，自动补齐 version 默认版本号"""
        path = self._get_metadata_path(table_id, format)
        if not path.exists():
            return {}
        
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict) and data and "version" not in data:
                    data["version"] = 1
                return data
        except (json.JSONDecodeError, IOError):
            return {}

    def save(self, table_id: str, format: str, metadata: dict):
        """原子化保存：先写 .tmp，再 replace 到主路径 (自动注入 version: 1)"""
        path = self._get_metadata_path(table_id, format)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        # 显式版本控制
        if "version" not in metadata:
            metadata["version"] = 1

        tmp_path = path.with_suffix(".tmp")
        
        # 写入临时文件
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno()) # 确保物理落盘

        # 原子替换
        os.replace(tmp_path, path)
