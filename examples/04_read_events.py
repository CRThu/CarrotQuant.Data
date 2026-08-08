"""
examples/04_read_events.py

CarrotQuant.Data Python SDK 事件数据读取示例
演示切片读取【事件/静态 (Event)】数据 (如概念板块、龙虎榜、机构交易)。
"""

import cqdata


def main():
    ev_table = "ashare.concept.eastmoney"
    print(f"=== 读取事件/板块数据 (table: {ev_table}) ===")

    df_events = cqdata.read(
        table_id=ev_table,
        columns=["board_code", "board_name", "symbol", "stock_name"]
    )

    print("读取结果 Preview (Polars DataFrame):")
    print(df_events.head(5))


if __name__ == "__main__":
    main()
