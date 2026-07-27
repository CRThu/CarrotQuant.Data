"""
cqdata/config/settings.py

全局配置管理模块。
基于 Pydantic-Settings，支持多层级配置加载与程序化动态配置：
1. 环境变量 CQDATA_STORAGE_ROOT
2. 环境变量 CQDATA_CONFIG 指定的 YAML 文件
3. 当前工作目录下的 ./config/config.yaml 或 ./config.yaml
4. 用户主目录下的 ~/.cqdata/config.yaml
"""

import os
from pathlib import Path
from typing import Optional, Union
import yaml
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CQDATA_", extra="ignore")

    # 存储根目录，默认 "storage_root"
    STORAGE_ROOT: str = "storage_root"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._load_config()

    def _load_config(self):
        # 1. 如果设置了 CQDATA_STORAGE_ROOT 环境变量，直接生效并返回
        if os.getenv("CQDATA_STORAGE_ROOT"):
            self.STORAGE_ROOT = os.getenv("CQDATA_STORAGE_ROOT")
            return

        # 2. 依次按优先级搜索候选 YAML 配置文件
        candidates = []
        if os.getenv("CQDATA_CONFIG"):
            candidates.append(Path(os.getenv("CQDATA_CONFIG")))

        cwd = Path.cwd()
        candidates.extend([
            cwd / "config" / "config.yaml",
            cwd / "config.yaml",
            Path.home() / ".cqdata" / "config.yaml"
        ])

        for config_path in candidates:
            if config_path.exists():
                try:
                    self.load_from_file(config_path)
                    break
                except Exception:
                    pass

    def load_from_file(self, config_path: Union[str, Path]) -> "Settings":
        """显式从指定 YAML 配置文件加载配置"""
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        with open(path, "r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f)
            if config_data and "storage_root" in config_data:
                self.STORAGE_ROOT = str(config_data["storage_root"])
        return self

    def configure(
        self,
        storage_root: Optional[Union[str, Path]] = None,
        config_file: Optional[Union[str, Path]] = None,
        **kwargs
    ) -> "Settings":
        """
        程序化全局配置入口。
        
        示例:
            settings.configure(storage_root="/path/to/storage")
        """
        if config_file:
            self.load_from_file(config_file)
        if storage_root is not None:
            self.STORAGE_ROOT = str(storage_root)

        for k, v in kwargs.items():
            attr = k.upper()
            if hasattr(self, attr):
                setattr(self, attr, v)

        return self


settings = Settings()
