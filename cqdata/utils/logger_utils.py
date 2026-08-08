import sys
import os
import datetime
from loguru import logger
from pathlib import Path
from typing import Optional, Union


def setup_logger(
    log_level: Optional[str] = None,
    log_file_prefix: str = "sync",
    log_dir: Optional[Union[str, Path]] = None
):
    """
    配置 loguru 日志，同时输出到控制台和文件。

    Args:
        log_level: 日志级别 (如 'INFO', 'DEBUG')，若为 None 从 settings 读取
        log_file_prefix: 日志文件前缀
        log_dir: 日志目录，若为 None 从 settings 读取
    """
    from cqdata.config.settings import settings

    if log_level is None:
        log_level = getattr(settings, "log_level", "INFO")

    if log_dir is None:
        log_dir = getattr(settings, "log_dir", "logs")

    log_dir_path = Path(log_dir)
    if not log_dir_path.is_absolute():
        # 相对路径以当前工作目录为参照
        log_dir_path = Path.cwd() / log_dir_path

    # 生成带时间戳的文件名
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{log_file_prefix}_{timestamp}.log"
    log_path = log_dir_path / log_filename

    # 确保日志目录存在
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # 移除默认的配置 (默认只输出到 stderr)
    logger.remove()

    # 添加控制台输出
    logger.add(
        sys.stderr, 
        level=log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )

    # 添加文件输出
    logger.add(
        str(log_path),
        rotation="100 MB",     # 日志文件非常大时才轮转
        compression="zip",     # 压缩旧日志
        level=log_level,
        encoding="utf-8",
        enqueue=True,          # 线程安全
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}"
    )

    return logger


class SuppressOutput:
    """
    上下文管理器，用于静默 stdout 和 stderr。
    常用于屏蔽 baostock 等库产生的非 log 打印。
    """
    def __enter__(self):
        self._stdout = sys.stdout
        self._stderr = sys.stderr
        sys.stdout = open(os.devnull, 'w')
        sys.stderr = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stderr.close()
        sys.stdout = self._stdout
        sys.stderr = self._stderr
