"""通达信数据导入脚本。

三种导入模式:
  1. zip:     下载 hsjday.zip / hsmin5.zip 并解析
  2. local:   读取本地通达信安装目录的 vipdoc 文件夹
  3. online:  通过 mootdx TCP 在线下载全部数据

用法:
    uv run scripts/download_tdx.py --mode zip                    # 下载ZIP (默认)
    uv run scripts/download_tdx.py --mode local --vipdoc C:/new_tdx/vipdoc  # 本地导入
    uv run scripts/download_tdx.py --mode online                 # 在线下载
    uv run scripts/download_tdx.py --mode zip --freq 1d,5m       # 指定频率
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from loguru import logger


def mode_zip(args):
    """zip 模式: 下载ZIP文件。"""
    from app.provider.tdx_utils import (
        TDX_DAILY_URL, TDX_MIN5_URL,
        download_tdx_file, discover_tdx_symbols,
    )

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("模式: ZIP 下载")
    logger.info("输出目录: %s", output_dir)

    freqs = args.freq.split(",")
    daily_zip = None
    min5_zip = None

    if "1d" in freqs:
        logger.info("\n[下载] 日线 (hsjday.zip) ...")
        daily_zip = download_tdx_file(TDX_DAILY_URL, str(output_dir))
        syms = discover_tdx_symbols(daily_zip)
        sh = len([s for s in syms if s.startswith("sh")])
        sz = len([s for s in syms if s.startswith("sz")])
        bj = len([s for s in syms if s.startswith("bj")])
        logger.info("  %d 个代码 (沪:%d 深:%d 北:%d)", len(syms), sh, sz, bj)

    if "5m" in freqs:
        logger.info("\n[下载] 5分钟线 (hsmin5.zip) ...")
        min5_zip = download_tdx_file(TDX_MIN5_URL, str(output_dir))
        syms = discover_tdx_symbols(min5_zip)
        logger.info("  %d 个代码", len(syms))

    logger.info("\n完成! 文件在 %s", output_dir)


def mode_local(args):
    """local 模式: 从本地vipdoc导入。"""
    from app.provider.tdx_utils import (
        discover_tdx_symbols_from_local, read_tdx_file_from_local,
    )

    vipdoc_dir = Path(args.vipdoc)
    if not vipdoc_dir.exists():
        logger.error("vipdoc 目录不存在: %s", vipdoc_dir)
        sys.exit(1)

    logger.info("模式: 本地 vipdoc 导入")
    logger.info("vipdoc 路径: %s", vipdoc_dir)

    # 发现代码
    symbols = discover_tdx_symbols_from_local(vipdoc_dir)
    sh = len([s for s in symbols if s.startswith("sh")])
    sz = len([s for s in symbols if s.startswith("sz")])
    bj = len([s for s in symbols if s.startswith("bj")])
    logger.info("发现 %d 个代码 (沪:%d 深:%d 北:%d)", len(symbols), sh, sz, bj)

    # 采样验证
    freqs = args.freq.split(",")
    sample_codes = symbols[:3] if len(symbols) > 3 else symbols
    for code in sample_codes:
        for freq in freqs:
            records = read_tdx_file_from_local(vipdoc_dir, code, freq=freq)
            if records:
                logger.info("  %s %s: %d条, %s ~ %s",
                    code, freq, len(records),
                    records[0]["date"],
                    records[-1]["date"],
                )
            else:
                logger.info("  %s %s: 无数据", code, freq)

    logger.info("\n本地数据可用，TDXProvider 使用 mode='local' 即可加载")


def mode_online(args):
    """online 模式: 通过mootdx TCP在线下载。"""
    from app.provider.tdx_utils import (
        fetch_bars_online, fetch_stock_list_online,
    )
    import polars as pl

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("模式: 在线下载 (mootdx TCP)")
    logger.info("输出目录: %s", output_dir)

    freqs = args.freq.split(",")
    symbols_limit = args.limit

    # 获取股票列表
    logger.info("\n[1/3] 获取股票列表 ...")
    sh_codes = fetch_stock_list_online(market="sh")
    sz_codes = fetch_stock_list_online(market="sz")
    all_codes = sh_codes + sz_codes
    # 过滤个股
    stock_codes = [c for c in all_codes if c[2:].startswith(('6', '0', '3'))]
    logger.info("  沪市: %d, 深市: %d, 个股: %d", len(sh_codes), len(sz_codes), len(stock_codes))

    if symbols_limit:
        stock_codes = stock_codes[:symbols_limit]
        logger.info("  限制为前 %d 个", symbols_limit)

    # 采样下载
    logger.info("\n[2/3] 采样下载 (前3个) ...")
    for code in stock_codes[:3]:
        symbol = f"{code[:2]}.{code[2:]}"
        for freq in freqs:
            df = fetch_bars_online(symbol, freq=freq)
            if not df.is_empty():
                logger.info("  %s %s: %d条, %s ~ %s",
                    symbol, freq, len(df),
                    df["date"].min(), df["date"].max(),
                )

    # 保存采样数据
    logger.info("\n[3/3] 保存采样数据 ...")
    sample = stock_codes[:3]
    for code in sample:
        symbol = f"{code[:2]}.{code[2:]}"
        for freq in freqs:
            df = fetch_bars_online(symbol, freq=freq)
            if not df.is_empty():
                out_path = output_dir / f"{code}_{freq}.csv"
                df.write_csv(str(out_path))
                logger.info("  保存: %s", out_path)

    logger.info("\n完成!")
    logger.info("提示: 在线模式数据量大，建议对全市场使用 zip 模式批量下载")
    logger.info("      或使用 TDXProvider(mode='online') 逐只增量获取")


def main():
    parser = argparse.ArgumentParser(
        description="通达信数据导入",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
三种导入模式:
  zip     下载ZIP文件 (推荐首次全量导入)
  local   从本地通达信vipdoc目录导入
  online  通过mootdx TCP在线下载 (适合增量)

示例:
  %(prog)s --mode zip
  %(prog)s --mode local --vipdoc C:/new_tdx/vipdoc
  %(prog)s --mode online --freq 1d,5m --limit 100
""",
    )
    parser.add_argument(
        "--mode", "-m",
        choices=["zip", "local", "online"],
        default="zip",
        help="导入模式 (默认: zip)",
    )
    parser.add_argument(
        "--freq", "-f",
        type=str,
        default="1d",
        help="数据频率，逗号分隔 (默认: 1d，可选: 1d,5m,1m)",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default="./tdx_data",
        help="输出目录 (zip/online 模式使用，默认: ./tdx_data)",
    )
    parser.add_argument(
        "--vipdoc",
        type=str,
        default=None,
        help="本地通达信 vipdoc 目录路径 (local 模式必填)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="限制下载的证券数量 (online 模式，用于测试)",
    )

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("通达信数据导入")
    logger.info("模式: %s | 频率: %s", args.mode.upper(), args.freq)
    logger.info("=" * 60)

    if args.mode == "local" and not args.vipdoc:
        logger.error("local 模式需要指定 --vipdoc 参数")
        sys.exit(1)

    if args.mode == "zip":
        mode_zip(args)
    elif args.mode == "local":
        mode_local(args)
    elif args.mode == "online":
        mode_online(args)


if __name__ == "__main__":
    main()
