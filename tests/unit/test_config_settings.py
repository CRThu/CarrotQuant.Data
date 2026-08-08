import os
import pytest
from pathlib import Path
from cqdata.config.settings import Settings


def test_settings_default_fallback(monkeypatch):
    """测试无任何配置或环境变量时全自动回退至默认 'storage_root'"""
    monkeypatch.delenv("CQDATA_STORAGE_PATH", raising=False)
    monkeypatch.delenv("CQDATA_CONFIG_PATH", raising=False)
    s = Settings()
    assert s.storage_path == "storage_root"
    assert s.log_dir == "logs"
    assert s.log_level == "INFO"


def test_settings_env_var_override(monkeypatch):
    """测试通过环境变量 CQDATA_STORAGE_PATH 覆盖 storage_path"""
    monkeypatch.setenv("CQDATA_STORAGE_PATH", "/tmp/env_storage_root")
    s = Settings()
    assert s.storage_path == "/tmp/env_storage_root"


def test_settings_custom_config_yaml_env(tmp_path, monkeypatch):
    """测试通过 CQDATA_CONFIG_PATH 环境变量指定自定义 YAML 文件路径"""
    custom_yaml = tmp_path / "custom_config.yaml"
    custom_yaml.write_text("storage_path: '/custom/path/from/yaml'\n", encoding="utf-8")

    monkeypatch.delenv("CQDATA_STORAGE_PATH", raising=False)
    monkeypatch.setenv("CQDATA_CONFIG_PATH", str(custom_yaml))

    s = Settings()
    assert s.storage_path == "/custom/path/from/yaml"


def test_cqdata_configure_from_file_and_property_assignment(tmp_path):
    """测试通过 cqdata.configure 显式配置文件加载与 cqdata.settings 直接属性修改"""
    import cqdata
    config_file = tmp_path / "test_cfg.yaml"
    config_file.write_text("storage_path: '/path/from/yaml'\n", encoding="utf-8")

    cqdata.configure(config_file)
    assert cqdata.settings.storage_path == "/path/from/yaml"

    # 直接修改属性
    cqdata.settings.storage_path = "storage_root"
    assert cqdata.settings.storage_path == "storage_root"


def test_configure_non_existent_file_raises():
    """测试 cqdata.configure 传入不存在的 YAML 文件路径时抛出 FileNotFoundError"""
    import cqdata
    with pytest.raises(FileNotFoundError, match="Config file not found"):
        cqdata.configure("non_existent_path_12345.yaml")
