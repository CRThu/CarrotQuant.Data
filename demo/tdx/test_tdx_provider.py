"""TDX Provider 集成测试 Demo。

验证:
1. TDXProvider 初始化
2. get_all_symbols() 发现代码
3. fetch() 获取日线数据
4. 数据标准化验证
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.provider.tdx_provider import TDXProvider


def test_provider():
    """测试 TDX Provider。"""
    print("=" * 60)
    print("TDX Provider 集成测试")
    print("=" * 60)

    # 1. 初始化
    print("\n1. 初始化 TDXProvider")
    provider = TDXProvider(mode="local", vipdoc_dir="./vipdoc")
    print(f"   支持的表: {provider.get_supported_tables()}")

    # 2. 发现个股代码
    print("\n2. 发现个股代码 (ashare.kline.1d.tdx)")
    symbols = provider.get_all_symbols("ashare.kline.1d.tdx")
    print(f"   发现 {len(symbols)} 个代码")
    print(f"   前10个: {symbols[:10]}")

    # 3. 获取日线数据
    print("\n3. 获取 sh.600000 日线数据 (最近10天)")
    df = provider.fetch(
        "ashare.kline.1d.tdx",
        "sh.600000",
        "2026-06-01",
        "2026-06-18"
    )
    print(f"   返回类型: {type(df)}")
    print(f"   列名: {df.columns}")
    print(f"   数据行数: {len(df)}")
    if not df.is_empty():
        print(f"   Schema:")
        for col in df.columns:
            print(f"     {col}: {df.schema[col]}")
        print(f"   前5行:")
        print(df.head())

    # 4. 获取指数日线数据
    print("\n4. 获取 sh.000001 (上证指数) 日线数据")
    df_idx = provider.fetch(
        "aindex.kline.1d.tdx",
        "sh.000001",
        "2026-06-01",
        "2026-06-18"
    )
    print(f"   数据行数: {len(df_idx)}")
    if not df_idx.is_empty():
        print(f"   前5行:")
        print(df_idx.head())

    # 5. 空数据测试
    print("\n5. 空数据测试 (不存在的日期范围)")
    df_empty = provider.fetch(
        "ashare.kline.1d.tdx",
        "sh.600000",
        "2000-01-01",
        "2000-01-01"
    )
    print(f"   数据行数: {len(df_empty)}")
    print(f"   是否为空: {df_empty.is_empty()}")
    if df_empty.is_empty():
        print(f"   Schema 保持完整: {list(df_empty.columns)}")

    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)


if __name__ == "__main__":
    test_provider()
