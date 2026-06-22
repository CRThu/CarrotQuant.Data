import os
from pathlib import Path
import yaml
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # 项目根目录
    PROJECT_ROOT: Path = Path(__file__).parent.parent.parent
    
    # 存储根目录，默认 storage_root
    STORAGE_ROOT: str = "storage_root"
    
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

settings = Settings()
