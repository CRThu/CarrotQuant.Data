import typer
from loguru import logger
from carrotquant_data.services.sync_manager import SyncManager
from carrotquant_data.models.metadata import TableMetadata, TableType, StorageFormat, ColumnDefinition
from carrotquant_data.services.metadata_manager import MetadataManager
import os

app = typer.Typer(help="CarrotQuant Data Center CLI")

# Default storage root
STORAGE_ROOT = os.getenv("CQ_STORAGE_ROOT", "./storage_root")

@app.command()
def sync(table_id: str = typer.Argument(..., help="Table ID to sync")):
    """同步指定表的数据"""
    manager = SyncManager(STORAGE_ROOT)
    manager.sync_table(table_id)

@app.command()
def init_table(
    table_id: str, 
    name: str, 
    type: str = "TS", 
    fmt: str = "parquet"
):
    """初始化一张新表的元数据"""
    mgr = MetadataManager(STORAGE_ROOT)
    
    metadata = TableMetadata(
        table_id=table_id,
        table_name=name,
        table_type=TableType(type),
        storage_format=StorageFormat(fmt)
    )
    
    mgr.save_metadata(metadata)
    logger.info(f"Initialized table {table_id} at {mgr.get_table_dir(table_id)}")

if __name__ == "__main__":
    app()
