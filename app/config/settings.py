import os
from pathlib import Path
import yaml
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # 项目根目录
    PROJECT_ROOT: Path = Path(__file__).parent.parent.parent
    
    # 存储根目录，默认 storage_root
    STORAGE_ROOT: str = "storage_root"
    
    # TDX 配置
    TDX_MODE: str = "zip"           # zip / local / online
    TDX_DATA_DIR: str = "tdx_data"  # ZIP 下载目录
    TDX_VIPDOC_DIR: str = ""        # 本地 vipdoc 目录路径
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._load_from_yaml()

    def _load_from_yaml(self):
        config_path = self.PROJECT_ROOT / "config" / "config.yaml"
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                config_data = yaml.safe_load(f)
                if config_data:
                    if "storage_root" in config_data:
                        self.STORAGE_ROOT = config_data["storage_root"]
                    # TDX 配置
                    tdx = config_data.get("tdx", {})
                    if "mode" in tdx:
                        self.TDX_MODE = tdx["mode"]
                    if "data_dir" in tdx:
                        self.TDX_DATA_DIR = tdx["data_dir"]
                    if "vipdoc_dir" in tdx:
                        self.TDX_VIPDOC_DIR = tdx["vipdoc_dir"]

settings = Settings()
