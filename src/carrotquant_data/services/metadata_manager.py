import json
import os
from pathlib import Path
from typing import Optional
from ..models.metadata import TableMetadata

class MetadataManager:
    def __init__(self, storage_root: str):
        self.storage_root = Path(storage_root)

    def get_table_dir(self, table_id: str) -> Path:
        return self.storage_root / table_id

    def get_metadata_path(self, table_id: str) -> Path:
        return self.get_table_dir(table_id) / "metadata.json"

    def load_metadata(self, table_id: str) -> Optional[TableMetadata]:
        path = self.get_metadata_path(table_id)
        if not path.exists():
            return None
        
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return TableMetadata(**data)

    def save_metadata(self, metadata: TableMetadata):
        table_dir = self.get_table_dir(metadata.table_id)
        table_dir.mkdir(parents=True, exist_ok=True)
        
        path = self.get_metadata_path(metadata.table_id)
        with open(path, "w", encoding="utf-8") as f:
            # Use json.dump for better formatting and datetime handling (via pydantic's model_dump)
            f.write(metadata.model_dump_json(indent=4))
        
        # Ensure data directory exists
        (table_dir / "data").mkdir(exist_ok=True)

    def update_sync_status(self, table_id: str, last_date: str):
        metadata = self.load_metadata(table_id)
        if metadata:
            metadata.last_data_date = last_date
            # metadata.last_sync_time = datetime.now() # handled in service
            self.save_metadata(metadata)
