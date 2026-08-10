"""
cqdata/config/settings.py

全局配置管理模块。
纯 Python 实现轻量 Settings，支持显式配置加载与程序化动态修改：
1. 环境变量 CQDATA_CONFIG_PATH / CQDATA_DATA_DIR (显式指定)
2. 显式 configure(config_path) 加载 YAML 配置文件
3. 显式修改 cqdata.settings 属性
"""

import os
from pathlib import Path
from typing import Optional, Union, Dict, Any
import yaml


class Settings:
    """
    全局 Settings 类 (纯 Python 实现，无 pydantic-settings 依赖)
    """

    def __init__(self):
        # 核心配置字段默认值
        self.data_dir: str = "data"
        self.log_dir: str = "logs"
        self.log_level: str = "INFO"
        self.defaults: Dict[str, Any] = {}

        # 初始化时自动加载配置
        self._load_initial_config()

    def _load_initial_config(self) -> None:
        """
        按优先级规则初始化加载配置：
        1. 内置默认值
        2. 环境变量 CQDATA_CONFIG_PATH 显式指定配置文件
        3. 环境变量 CQDATA_DATA_DIR 显式覆盖 data_dir
        """
        # 1. 环境变量 CQDATA_CONFIG_PATH 显式指定 YAML 配置文件
        config_path_env = os.getenv("CQDATA_CONFIG_PATH")
        if config_path_env:
            path = Path(config_path_env)
            if path.exists():
                self.load_from_file(path)

        # 2. 环境变量 CQDATA_DATA_DIR 显式覆盖 data_dir
        if os.getenv("CQDATA_DATA_DIR"):
            self.data_dir = os.getenv("CQDATA_DATA_DIR")

    def load_from_file(self, config_path: Union[str, Path]) -> "Settings":
        """
        显式从指定 YAML 配置文件加载配置
        """
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(path, "r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f)

        if isinstance(config_data, dict):
            if "data_dir" in config_data:
                self.data_dir = str(config_data["data_dir"])

            if "log_dir" in config_data:
                self.log_dir = str(config_data["log_dir"])

            if "log_level" in config_data:
                self.log_level = str(config_data["log_level"]).upper()

            if "defaults" in config_data and isinstance(config_data["defaults"], dict):
                self.defaults = config_data["defaults"]
                self._update_accessor_defaults()

        return self

    def configure(self, config_path: Union[str, Path]) -> "Settings":
        """
        从指定 YAML 配置文件加载全局参数。

        示例:
            cqdata.configure("./config.yaml")
        """
        self.load_from_file(config_path)
        self._refresh_logger()
        return self

    def _refresh_logger(self) -> None:
        """配置变更后安全刷新 loggerHandler"""
        try:
            from cqdata.utils.logger_utils import setup_logger
            setup_logger(log_level=self.log_level, log_dir=self.log_dir)
        except Exception:
            pass

    def _update_accessor_defaults(self) -> None:
        """更新全局 accessor defaults 链"""
        try:
            from cqdata.entrypoints.accessors import default
            default.update_from_dict(self.defaults)
        except (ImportError, AttributeError):
            pass


# 全局 Settings 单例实例
settings = Settings()
