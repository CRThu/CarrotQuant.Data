import os
import pytest
from pathlib import Path
from cqdata.config.settings import Settings


def test_settings_env_var_override(monkeypatch):
    """测试通过环境变量 CQDATA_STORAGE_ROOT 覆盖 STORAGE_ROOT"""
    monkeypatch.setenv("CQDATA_STORAGE_ROOT", "/tmp/env_storage_root")
    s = Settings()
    assert s.STORAGE_ROOT == "/tmp/env_storage_root"


def test_settings_custom_config_yaml_env(tmp_path, monkeypatch):
    """测试通过 CQDATA_CONFIG 环境变量指定自定义 YAML 文件路径"""
    custom_yaml = tmp_path / "custom_config.yaml"
    custom_yaml.write_text("storage_root: '/custom/path/from/yaml'\n", encoding="utf-8")

    monkeypatch.delenv("CQDATA_STORAGE_ROOT", raising=False)
    monkeypatch.setenv("CQDATA_CONFIG", str(custom_yaml))

    s = Settings()
    assert s.STORAGE_ROOT == "/custom/path/from/yaml"


def test_settings_cwd_config_yaml(tmp_path, monkeypatch):
    """测试当前工作目录下 ./config/config.yaml 的解析"""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    yaml_file = config_dir / "config.yaml"
    yaml_file.write_text("storage_root: './my_local_storage'\n", encoding="utf-8")

    monkeypatch.delenv("CQDATA_STORAGE_ROOT", raising=False)
    monkeypatch.delenv("CQDATA_CONFIG", raising=False)
    monkeypatch.chdir(tmp_path)

    s = Settings()
    assert s.STORAGE_ROOT == "./my_local_storage"


def test_cqdata_configure_programmatic():
    """测试通过 cqdata.configure / set_config 程序化配置"""
    import cqdata
    cqdata.configure(storage_root="/programmatic/storage/path")
    assert cqdata.get_config().STORAGE_ROOT == "/programmatic/storage/path"

    cqdata.set_config(storage_root="storage_root")
    assert cqdata.get_config().STORAGE_ROOT == "storage_root"

