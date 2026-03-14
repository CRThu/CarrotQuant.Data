from typing import Dict, Any, Optional
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
import yaml
import os

class SourceConfig(BaseModel):
    enabled: bool = True
    timeout: int = 30
    extra: Dict[str, Any] = {}

class AppConfig(BaseSettings):
    storage_root: str = "./storage_root"
    sources: Dict[str, Any] = {}

    @classmethod
    def load_from_yaml(cls, yaml_path: str):
        if not os.path.exists(yaml_path):
            return cls()
        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            return cls(sources=data)

# Singleton instance
config_path = os.path.join(os.getcwd(), "config", "sources.yaml")
settings = AppConfig.load_from_yaml(config_path)
