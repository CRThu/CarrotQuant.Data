import os
import pytest
from pathlib import Path
from cq.data.config.settings import Settings


def test_settings_default_fallback(monkeypatch):
    """测试无任何配置或环境变量时全自动回退至默认 'data'"""
    monkeypatch.delenv("CQDATA_DATA_DIR", raising=False)
    monkeypatch.delenv("CQDATA_CONFIG_PATH", raising=False)
    s = Settings()
    assert s.data_dir == "data"
    assert s.log_dir == "logs"
    assert s.log_level == "INFO"


def test_settings_env_var_override(monkeypatch):
    """测试通过环境变量 CQDATA_DATA_DIR 覆盖 data_dir"""
    monkeypatch.setenv("CQDATA_DATA_DIR", "/tmp/env_data_dir")
    s = Settings()
    assert s.data_dir == "/tmp/env_data_dir"


def test_settings_custom_config_yaml_env(tmp_path, monkeypatch):
    """测试通过 CQDATA_CONFIG_PATH 环境变量指定自定义 YAML 文件路径"""
    custom_yaml = tmp_path / "custom_config.yaml"
    custom_yaml.write_text("data_dir: '/custom/path/from/yaml'\n", encoding="utf-8")

    monkeypatch.delenv("CQDATA_DATA_DIR", raising=False)
    monkeypatch.setenv("CQDATA_CONFIG_PATH", str(custom_yaml))

    s = Settings()
    assert s.data_dir == "/custom/path/from/yaml"


def test_cqdata_configure_from_file_and_property_assignment(tmp_path):
    """测试通过 cq.data.configure 显式配置文件加载与 cq.data.settings 直接属性修改"""
    import cq.data
    config_file = tmp_path / "test_cfg.yaml"
    config_file.write_text("data_dir: '/path/from/yaml'\n", encoding="utf-8")

    cq.data.configure(config_file)
    assert cq.data.settings.data_dir == "/path/from/yaml"

    # 直接修改属性
    cq.data.settings.data_dir = "data"
    assert cq.data.settings.data_dir == "data"


def test_configure_non_existent_file_raises():
    """测试 cq.data.configure 传入不存在的 YAML 文件路径时抛出 FileNotFoundError"""
    import cq.data
    with pytest.raises(FileNotFoundError, match="Config file not found"):
        cq.data.configure("non_existent_path_12345.yaml")
