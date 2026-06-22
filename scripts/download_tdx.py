"""通达信数据下载脚本。

从 TDX 服务器下载 hsjday.zip 并自动解压到 vipdoc 目录，
供 TDXProvider(mode='local') 使用。

用法:
    uv run scripts/download_tdx.py                         # 下载到默认 vipdoc 目录
    uv run scripts/download_tdx.py --output C:/new_tdx/vipdoc  # 指定输出目录
    uv run scripts/download_tdx.py --freq 1d,5m            # 指定频率 (5m/1m 仅从本地 vipdoc 获取)
"""

import argparse
import sys
import tempfile
import zipfile
from pathlib import Path
from urllib.request import urlretrieve

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from loguru import logger

# TDX 数据源 URL
TDX_DAILY_URL = "https://data.tdx.com.cn/vipdoc/hsjday.zip"

# 需要提取的目录前缀
_EXTRACT_PREFIXES = ("sh/lday/", "sz/lday/", "bj/lday/")


def download_and_extract(output_dir: Path):
    """下载 hsjday.zip 并解压到 vipdoc 目录。"""
    output_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmp_dir:
        zip_path = Path(tmp_dir) / "hsjday.zip"

        logger.info("正在下载: %s", TDX_DAILY_URL)
        urlretrieve(TDX_DAILY_URL, str(zip_path))
        logger.info("下载完成: %s", zip_path)

        logger.info("正在解压到: %s", output_dir)
        extracted = 0
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for info in zf.infolist():
                if any(info.filename.startswith(p) for p in _EXTRACT_PREFIXES):
                    zf.extract(info, str(output_dir))
                    extracted += 1

        logger.info("解压完成: %d 个文件", extracted)


def verify_vipdoc(vipdoc_dir: Path):
    """验证 vipdoc 目录结构并统计代码数量。"""
    sh_count = 0
    sz_count = 0
    bj_count = 0

    for market_dir in ["sh", "sz", "bj"]:
        lday = vipdoc_dir / market_dir / "lday"
        if lday.exists():
            count = len(list(lday.glob("*.day")))
            if market_dir == "sh":
                sh_count = count
            elif market_dir == "sz":
                sz_count = count
            else:
                bj_count = count

    total = sh_count + sz_count + bj_count
    logger.info("vipdoc 目录统计:")
    logger.info("  沪市 (sh): %d 个代码", sh_count)
    logger.info("  深市 (sz): %d 个代码", sz_count)
    logger.info("  北交所 (bj): %d 个代码", bj_count)
    logger.info("  合计: %d 个代码", total)

    if total == 0:
        logger.warning("vipdoc 目录为空，请检查下载是否成功")
        return False

    # 采样验证
    sample_files = list((vipdoc_dir / "sh" / "lday").glob("*.day"))[:3]
    for f in sample_files:
        data = f.read_bytes()
        from app.provider.tdx_utils import parse_tdx_day_data
        records = parse_tdx_day_data(data)
        if records:
            logger.info("  采样 %s: %d 条, %s ~ %s",
                f.stem, len(records),
                records[0]["date"], records[-1]["date"])

    return True


def main():
    parser = argparse.ArgumentParser(
        description="通达信数据下载",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s                                     # 下载到默认 vipdoc 目录
  %(prog)s --output C:/new_tdx/vipdoc          # 指定输出目录

下载后使用:
  TDXProvider(mode='local', vipdoc_dir='vipdoc')
""",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default="vipdoc",
        help="vipdoc 输出目录 (默认: vipdoc)",
    )

    args = parser.parse_args()

    output_dir = Path(args.output)

    logger.info("=" * 60)
    logger.info("通达信数据下载")
    logger.info("输出目录: %s", output_dir)
    logger.info("=" * 60)

    download_and_extract(output_dir)

    logger.info("")
    verify_vipdoc(output_dir)

    logger.info("")
    logger.info("完成! 使用方式:")
    logger.info("  TDXProvider(mode='local', vipdoc_dir='%s')", output_dir)


if __name__ == "__main__":
    main()
