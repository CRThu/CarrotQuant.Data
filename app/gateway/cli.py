import sys
import tempfile
import zipfile
from pathlib import Path
from typing import List, Optional
from urllib.request import urlretrieve

import typer
import uvicorn
from loguru import logger

from app.service.sync_manager import SyncManager
from app.utils.logger_utils import setup_logger

setup_logger(log_level="INFO", log_file_prefix="sync_task")

app = typer.Typer(help="CarrotQuant.Data CLI 数据同步工具", no_args_is_help=True)

# TDX 默认配置
TDX_DEFAULT_VIPDOC = r"C:\new_tdx\vipdoc"
TDX_DAILY_URL = "https://data.tdx.com.cn/vipdoc/hsjday.zip"
_TDX_EXTRACT_PREFIXES = ("sh/lday/", "sz/lday/", "bj/lday/")


@app.command()
def server(
    host: str = typer.Option("0.0.0.0", "--host", "-h", help="监听地址"),
    port: int = typer.Option(8000, "--port", "-p", help="监听端口"),
    reload: bool = typer.Option(True, "--reload", help="是否开启热重载")
):
    """启动 API 服务"""
    logger.info(f"[*] Starting API Server on {host}:{port} (reload={reload})")
    uvicorn.run("app.gateway.api:app", host=host, port=port, reload=reload)


@app.command()
def sync(
    tables: str = typer.Option(..., "--tables", "-t", help="同步的 table_id，逗号分隔"),
    formats: str = typer.Option("csv,parquet", "--formats", "-f", help="同步的格式，逗号分隔"),
    start_date: Optional[str] = typer.Option(None, "--start", "-s", help="起始日期 (YYYY-MM-DD)"),
    end_date: Optional[str] = typer.Option(None, "--end", "-e", help="结束日期 (YYYY-MM-DD)"),
    force: bool = typer.Option(False, "--force", help="是否强制全量刷新水位线"),
    batch_size: int = typer.Option(100, "--batch", help="批量聚合长度"),
    symbol_limit: Optional[int] = typer.Option(None, "--limit", help="限制同步的证券数量"),
    output_dir: Optional[str] = typer.Option(None, "--output", "-o", help="自定义存储根目录"),
    use_local: bool = typer.Option(False, "--local", help="使用本地 vipdoc 模式 (默认 online)"),
    tdx_vipdoc: str = typer.Option(TDX_DEFAULT_VIPDOC, "--tdx-vipdoc", help="TDX vipdoc 目录路径"),
):
    """启动数据同步流程"""
    table_list = [t.strip() for t in tables.split(",") if t.strip()]
    format_list = [f.strip() for f in formats.split(",") if f.strip()]

    if not table_list:
        logger.error("No tables specified for sync.")
        raise typer.Exit(code=1)

    logger.info(f"[*] Starting CLI sync for {len(table_list)} tables: {table_list}")
    logger.info(f"[*] Formats: {format_list}")

    tdx_kwargs = {"mode": "local" if use_local else "online", "vipdoc_dir": tdx_vipdoc}

    try:
        sync_mgr = SyncManager(storage_root=output_dir)
        sync_mgr.sync(
            table_ids=table_list,
            formats=format_list,
            start_date=start_date,
            end_date=end_date,
            force_refresh=force,
            batch_size=batch_size,
            symbol_limit=symbol_limit,
            provider_kwargs=tdx_kwargs,
        )
        logger.info("[+] CLI Sync completed successfully.")
    except Exception as e:
        logger.error(f"[!] CLI Sync failed: {e}")
        raise typer.Exit(code=1)


@app.command("tdx")
def tdx_cmd(
    action: str = typer.Argument(..., help="操作: download (下载日线ZIP并导入vipdoc)"),
    vipdoc_dir: str = typer.Option(TDX_DEFAULT_VIPDOC, "--vipdoc", "-v", help="vipdoc 目录路径"),
):
    """通达信数据工具"""
    if action == "download":
        tdx_download(vipdoc_dir)
    else:
        logger.error(f"未知操作: {action}")
        raise typer.Exit(code=1)


def tdx_download(vipdoc_dir: str):
    """下载 hsjday.zip 并仅提取 lday 文件夹到 vipdoc (不覆盖分钟线)。"""
    vipdoc = Path(vipdoc_dir)
    vipdoc.mkdir(parents=True, exist_ok=True)

    logger.info("[1/3] 下载 hsjday.zip ...")
    with tempfile.TemporaryDirectory() as tmp_dir:
        zip_path = Path(tmp_dir) / "hsjday.zip"
        urlretrieve(TDX_DAILY_URL, str(zip_path))
        logger.info("  下载完成: %s", zip_path)

        logger.info("[2/3] 解压 lday 文件夹到 %s ...", vipdoc)
        extracted = 0
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for info in zf.infolist():
                if any(info.filename.startswith(p) for p in _TDX_EXTRACT_PREFIXES):
                    zf.extract(info, str(vipdoc))
                    extracted += 1
        logger.info("  解压完成: %d 个文件", extracted)

    logger.info("[3/3] 验证 vipdoc 目录 ...")
    sh = len(list((vipdoc / "sh" / "lday").glob("*.day"))) if (vipdoc / "sh" / "lday").exists() else 0
    sz = len(list((vipdoc / "sz" / "lday").glob("*.day"))) if (vipdoc / "sz" / "lday").exists() else 0
    bj = len(list((vipdoc / "bj" / "lday").glob("*.day"))) if (vipdoc / "bj" / "lday").exists() else 0
    logger.info("  沪市日线: %d, 深市日线: %d, 北交所日线: %d", sh, sz, bj)

    logger.info("完成! 使用方式:")
    logger.info("  uv run -m app.gateway.cli sync -t ashare.kline.1d.raw.tdx --local --tdx-vipdoc '%s'", vipdoc_dir)


if __name__ == "__main__":
    app()
