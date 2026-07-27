"""
examples/03_read_series.py

CarrotQuant.Data Python SDK 时序数据读取示例
演示切片读取【不复权 (raw)】与【后复权 (adj)】时序 K 线数据。
"""

import cqdata


def main():
    # 1. 读取【不复权 (raw)】日线 K 线
    raw_table = "ashare.kline.1d.raw.baostock"
    print(f"=== 1. 读取不复权 K 线 (table: {raw_table}) ===")
    df_raw = cqdata.read_series(
        table_id=raw_table,
        start_date="2024-01-01",
        end_date="2024-01-10",
        columns=["timestamp", "datetime", "symbol", "close", "volume"]
    )
    print(df_raw.head(5))

    # 2. 读取【后复权 (adj)】日线 K 线
    adj_table = "ashare.kline.1d.adj.baostock"
    print(f"\n=== 2. 读取后复权 K 线 (table: {adj_table}) ===")
    df_adj = cqdata.read_series(
        table_id=adj_table,
        start_date="2024-01-01",
        end_date="2024-01-10",
        columns=["timestamp", "datetime", "symbol", "close", "volume"]
    )
    print(df_adj.head(5))


if __name__ == "__main__":
    main()
