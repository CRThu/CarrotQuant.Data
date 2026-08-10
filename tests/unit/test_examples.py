"""
tests/unit/test_examples.py

验证 examples/ 目录下所有 Python 示例脚本的语法正确性与核心 API 调度逻辑。
"""

import pytest
import importlib.util
from pathlib import Path
from unittest.mock import patch, MagicMock
import polars as pl


@pytest.mark.parametrize("example_name", [
    "01_quickstart.py",
    "02_sync_data.py",
    "03_read_series.py",
    "04_read_events.py",
    "05_export_pandas.py",
    "06_metadata_inspection.py"
])
def test_example_scripts_import_and_execution(example_name, temp_data_dir):
    """测试 examples 脚本能被成功加载与运行 (Mock 模拟数据 IO)"""
    example_path = Path(__file__).parent.parent.parent / "examples" / example_name
    assert example_path.exists(), f"Example script {example_name} not found"

    mock_df = pl.DataFrame({
        "timestamp": [1704092400000],
        "datetime": ["2024-01-01T15:00:00.000+08:00"],
        "symbol": ["sh.600000"],
        "board_code": ["BK0001"],
        "board_name": ["银行"],
        "stock_name": ["浦发银行"],
        "close": [10.0],
        "volume": [100000.0]
    })

    with patch("cqdata.sync"), \
         patch("cqdata.read", return_value=mock_df), \
         patch("cqdata.entrypoints.accessors.base.read", return_value=mock_df), \
         patch("cqdata.list_tables", return_value=[{"table_id": "ashare.kline.1d.raw.baostock", "category": "timeseries"}]), \
         patch("cqdata.list_formats", return_value=["parquet"]), \
         patch("cqdata.list_symbols", return_value=["sh.600000"]), \
         patch("cqdata.get_time_range", return_value=("2024-01-01", "2024-06-30")), \
         patch("cqdata.get_schema", return_value={"close": "Float64"}), \
         patch("cqdata.get_row_count", return_value=100):

        spec = importlib.util.spec_from_file_location(example_name.replace(".py", ""), example_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # 如果定义了 main 函数则执行 main()
        if hasattr(module, "main"):
            try:
                module.main()
            except ModuleNotFoundError:
                pass
