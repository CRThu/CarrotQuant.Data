import json
import os
from pathlib import Path

class MetadataManager:
    """元数据 IO 管理类，负责 metadata.json 的读取和原子化保存"""

    def __init__(self, storage_root: str = "storage_root"):
        self.storage_root = Path(storage_root)

    def _get_metadata_path(self, table_id: str, format: str) -> Path:
        """元数据路径：storage_root/{format}/{table_id}/metadata.json"""
        return self.storage_root / format / table_id / "metadata.json"

    def load(self, table_id: str, format: str) -> dict:
        """加载元数据，若不存在则返回全空结构"""
        path = self._get_metadata_path(table_id, format)
        if not path.exists():
            return {}
        
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}

    def save(self, table_id: str, format: str, metadata: dict):
        """原子化保存：先写 .tmp，再 replace 到主路径"""
        path = self._get_metadata_path(table_id, format)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        tmp_path = path.with_suffix(".tmp")
        
        # 写入临时文件
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno()) # 确保物理落盘

        # 原子替换
        os.replace(tmp_path, path)
