from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from enum import Enum

class TableType(str, Enum):
    TS = "TS"          # 时序数据 (Time Series)
    EVENT = "EVENT"    # 事件数据 (Alternative Data)

class StorageFormat(str, Enum):
    PARQUET = "parquet"
    CSV = "csv"

class ColumnDefinition(BaseModel):
    name: str
    dtype: str
    description: Optional[str] = None

class TableMetadata(BaseModel):
    table_id: str
    table_name: str
    table_type: TableType
    storage_format: StorageFormat = StorageFormat.PARQUET
    is_partitioned: bool = False
    partition_cols: List[str] = Field(default_factory=list)
    columns: List[ColumnDefinition] = Field(default_factory=list)
    primary_keys: List[str] = Field(default_factory=list)
    last_sync_time: Optional[datetime] = None
    last_data_date: Optional[str] = None  # e.g., "2024-05-20"
    extra_info: Dict[str, Any] = Field(default_factory=dict)
