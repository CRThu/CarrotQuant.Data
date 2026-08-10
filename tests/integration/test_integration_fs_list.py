"""
tests/integration/test_integration_fs_list.py

本地文件系统探查 API (/api/v1/filesystem/list) 的集成测试。
验证真实 HTTP TestClient 与系统实际物理路径探查。
"""

import pytest
from pathlib import Path
from fastapi.testclient import TestClient
from cqdata.entrypoints.rest_api import app

client = TestClient(app)


def test_integration_filesystem_list_default_path():
    """测试无参数请求，默认探查数据根目录"""
    response = client.get("/api/v1/filesystem/list")
    assert response.status_code == 200
    data = response.json()
    assert "path" in data
    assert "exists" in data
    assert "items" in data
    assert isinstance(data["items"], list)


def test_integration_filesystem_list_specific_dir(tmp_path):
    """测试探查指定存在的文件夹及其子元素与时间戳格式"""
    folder = tmp_path / "vipdoc_test"
    folder.mkdir()
    (folder / "sh").mkdir()
    (folder / "sz").mkdir()
    (folder / "info.json").write_text('{"name": "test"}')

    response = client.get("/api/v1/filesystem/list", params={"path": str(folder)})
    assert response.status_code == 200
    data = response.json()

    assert data["path"] == str(folder.resolve())
    assert data["exists"] is True
    assert data["is_dir"] is True
    assert data["total"] == 3

    items_map = {item["name"]: item for item in data["items"]}
    assert "sh" in items_map
    assert items_map["sh"]["is_dir"] is True

    assert "info.json" in items_map
    assert items_map["info.json"]["is_dir"] is False
    assert items_map["info.json"]["size"] > 0
    assert "T" in items_map["info.json"]["updated_at"]
