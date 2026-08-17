"""
tests/unit/test_script_wizard.py

交互向导脚本 scripts/wizard.py 的单元测试。
包含 discover_supported_tables 与 start_wizard 命令行向导交互流程的 Mock 测试。
"""

import pytest
from unittest.mock import patch, MagicMock
from scripts import wizard


def test_discover_supported_tables():
    """测试动态扫描 provider 驱动获取可用数据表列表"""
    tables = wizard.discover_supported_tables()
    assert isinstance(tables, list)
    assert len(tables) > 0
    assert "ashare.kline.1d.raw.baostock" in tables
    assert "ashare.adj_factor.baostock" in tables


def test_start_wizard_cancel():
    """测试向导在用户拒绝确认时正常退出不抛出异常"""
    inputs = [
        "1",           # 切换选中第1项
        "",            # 回车确认选择跳出表循环
        "2024-01-01",  # 起始日期
        "2024-01-31",  # 结束日期
        "parquet",     # 存储格式
        "n",           # 不强制全量
        "50",          # batch_size
        "n",           # 取消确认
    ]
    with patch("builtins.input", side_effect=inputs), patch("builtins.print"):
        wizard.start_wizard()


def test_start_wizard_execute():
    """测试向导完整确认并启动子进程 sync 指令挂载"""
    inputs = [
        "1",           # 切换选中第1项
        "",            # 回车确认选择跳出表循环
        "2024-01-01",  # 起始日期
        "2024-01-31",  # 结束日期
        "parquet",     # 存储格式
        "n",           # 不强制全量
        "50",          # batch_size
        "y",           # 确认启动
        "",            # 按回车键退出
    ]

    mock_process = MagicMock()
    mock_process.stdout = ["Running sync..."]
    mock_process.returncode = 0

    with patch("builtins.input", side_effect=inputs), \
         patch("builtins.print"), \
         patch("subprocess.Popen", return_value=mock_process) as mock_popen:
        wizard.start_wizard()
        assert mock_popen.called
        cmd = mock_popen.call_args[0][0]
        assert "cq.data.entrypoints.cli" in cmd
        assert "sync" in cmd
        assert "--tables" in cmd
        assert "--formats" in cmd
        assert "parquet" in cmd
