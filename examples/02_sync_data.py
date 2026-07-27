"""
examples/02_sync_data.py

CarrotQuant.Data Python SDK 数据同步示例
演示不同数据源、存储格式（Parquet/CSV）以及参数调优的同步操作。
"""

import cqdata


def main():
    # 示例 1: 同步 Baostock 日线 K 线 (时序数据，落地为 Parquet 大表)
    print("=== 1. 同步 Baostock 时序 K 线表 ===")
    cqdata.sync(
        table_ids="ashare.kline.1d.raw.baostock",
        formats="parquet",
        start_date="2024-01-01",
        end_date="2024-01-15",
        symbol_limit=2,
        force_refresh=True
    )

    # 示例 2: 同步 东方财富 概念板块 (事件数据，落地为 CSV)
    print("\n=== 2. 同步 东方财富概念板块表 (Event 数据) ===")
    cqdata.sync(
        table_ids="ashare.concept.eastmoney",
        formats="csv",
        symbol_limit=2,  # 限制同步 2 个概念板块样本
        force_refresh=True
    )

    print("\n数据同步任务全部执行完成！")


if __name__ == "__main__":
    main()
