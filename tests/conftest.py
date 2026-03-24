import pytest
import os
import sys
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

# 将当前根目录加入系统路径
sys.path.append(os.getcwd())

from app.config.settings import settings

@pytest.fixture
def temp_storage_root(tmp_path):
    """
    创建一个临时的存储根目录，并自动清理。
    """
    original_root = settings.STORAGE_ROOT
    settings.STORAGE_ROOT = str(tmp_path)
    yield tmp_path
    settings.STORAGE_ROOT = original_root

@pytest.fixture
def mock_baostock():
    """
    Mock Baostock 核心驱动，避免网络 IO。
    """
    with patch("app.provider.baostock_provider.bs") as mock_bs:
        mock_bs.login.return_value = MagicMock(error_code="0")
        # 默认模拟 query_stock_basic
        mock_rs_basic = MagicMock()
        mock_rs_basic.error_code = "0"
        mock_rs_basic.next.side_effect = [True, False]
        mock_rs_basic.get_row_data.return_value = ["sh.600000", "浦发银行", "1999-11-10", "", "1", "1"]
        mock_bs.query_stock_basic.return_value = mock_rs_basic
        
        # 默认模拟 query_history_k_data_plus
        mock_rs_k = MagicMock()
        mock_rs_k.error_code = "0"
        mock_rs_k.next.return_value = False # 默认空数据
        mock_bs.query_history_k_data_plus.return_value = mock_rs_k
        
        yield mock_bs