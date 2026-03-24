import typer
from typing import List, Optional
from loguru import logger
from ..service.sync_manager import SyncManager

app = typer.Typer(help="CarrotQuant.Data CLI 同步工具")

@app.command()
def sync(
    tables: str = typer.Option(..., "--tables", "-t", help="同步的 table_id，逗号分隔 (例如 'ashare.kline.1d.adj.baostock')"),
    formats: str = typer.Option("csv,parquet", "--formats", "-f", help="同步的格式，逗号分隔 (例如 'csv,parquet')"),
    start_date: Optional[str] = typer.Option(None, "--start", "-s", help="起始日期 (YYYY-MM-DD)"),
    end_date: Optional[str] = typer.Option(None, "--end", "-e", help="结束日期 (YYYY-MM-DD)"),
    force: bool = typer.Option(False, "--force", help="是否强制全量刷新水位线"),
    batch_size: int = typer.Option(100, "--batch", help="批量聚合长度")
):
    """
    启动数据同步流程
    """
    table_list = [t.strip() for t in tables.split(",") if t.strip()]
    format_list = [f.strip() for f in formats.split(",") if f.strip()]
    
    if not table_list:
        logger.error("No tables specified for sync.")
        raise typer.Exit(code=1)
        
    logger.info(f"[*] Starting CLI sync for {len(table_list)} tables: {table_list}")
    logger.info(f"[*] Formats: {format_list}")
    
    try:
        sync_mgr = SyncManager()
        sync_mgr.sync(
            table_ids=table_list,
            formats=format_list,
            start_date=start_date,
            end_date=end_date,
            force_refresh=force,
            batch_size=batch_size
        )
        logger.info("[+] CLI Sync completed successfully.")
    except Exception as e:
        logger.error(f"[!] CLI Sync failed: {e}")
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()
