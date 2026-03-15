import os
import sys
from pathlib import Path

# 将当前根目录加入系统路径
sys.path.append(os.getcwd())

import polars as pl
import shutil
from app.storage.csv_storage import CSVStorage

def test_csv_storage_append():
    test_root = "test_storage_root"
    # 清理旧测试数据
    if os.path.exists(test_root):
        shutil.rmtree(test_root)
    
    storage = CSVStorage(storage_root=f"{test_root}/csv")
    table_id = "ashare.1d.adj.baostock"
    symbol = "sh.600000"
    
    # 1. 模拟第一次写入: 2024-01-01 到 2024-01-05
    df1 = pl.DataFrame({
        "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
        "symbol": [symbol] * 5,
        "close": [10.0, 10.1, 10.2, 10.3, 10.4]
    })
    
    print("--- 执行第一次 Append ---")
    storage.append(table_id, df1)
    
    # 验证目录结构
    expected_path = Path(test_root) / "csv" / table_id / "year=2024" / f"{symbol}.csv"
    assert expected_path.exists(), f"文件未生成: {expected_path}"
    
    # 2. 模拟第二次写入: 2024-01-04 到 2024-01-08 (包含重叠)
    df2 = pl.DataFrame({
        "date": ["2024-01-04", "2024-01-05", "2024-01-06", "2024-01-07", "2024-01-08"],
        "symbol": [symbol] * 5,
        "close": [10.3, 10.4, 10.5, 10.6, 10.7]
    })
    
    print("--- 执行第二次 Append (重叠 01-04, 01-05) ---")
    storage.append(table_id, df2)
    
    # 3. 验证去重结果
    result_df = storage.read(table_id, symbol, 2024)
    print(f"合并去重后的数据量: {len(result_df)}")
    
    # 预期从 01-01 到 01-08 共 8 条数据
    assert len(result_df) == 8, f"数据去重失败，总数应该是 8，实际是 {len(result_df)}"
    assert result_df["date"].is_unique().all(), "Date 列不唯一"
    
    # 4. 验证跨年分片
    df3 = pl.DataFrame({
        "date": ["2025-01-01"],
        "symbol": [symbol],
        "close": [11.0]
    })
    print("--- 执行跨年 Append (2025) ---")
    storage.append(table_id, df3)
    
    path_2025 = Path(test_root) / "csv" / table_id / "year=2025" / f"{symbol}.csv"
    assert path_2025.exists(), "2025 年目录未生成"
    
    print("测试通过！")

if __name__ == "__main__":
    test_csv_storage_append()
