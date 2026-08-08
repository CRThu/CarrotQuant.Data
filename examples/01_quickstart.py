"""
examples/01_quickstart.py

CarrotQuant.Data Python SDK 快速上手示例
演示基于 OOP 便捷层与统一 read 函数的基础数据同步与读取流程。
"""

import cqdata


def main():
    table_id = "ashare.kline.1d.raw.baostock"

    print("=== 1. 触发数据同步 ===")
    cqdata.sync(
        table_ids=table_id,
        formats="parquet",
        start_date="2024-01-01",
        end_date="2024-01-10",
        symbol_limit=2,
        force_refresh=True  # 示例强刷 2 支股票样本数据
    )
    print("同步完成！")

    print("\n=== 2. 读取数据 (使用 OOP 便捷访问层) ===")
    # 自动推导 freq="1d", adj="raw", source="baostock"
    df_oop = cqdata.ashare.kline.get(
        symbols=["sh.600000"],
        start_date="2024-01-01",
        end_date="2024-01-10"
    )
    print("\n[OOP 便捷读取结果 Preview]:")
    print(df_oop.head(5))

    print("\n=== 3. 读取数据 (使用经典统一 cqdata.read 入口) ===")
    df_read = cqdata.read(
        table_id=table_id,
        start_date="2024-01-01",
        end_date="2024-01-10"
    )
    print("\n[read() 切片读取结果 Preview]:")
    print(df_read.head(5))


if __name__ == "__main__":
    main()
