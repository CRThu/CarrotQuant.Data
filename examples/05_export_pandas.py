"""
examples/05_export_pandas.py

CarrotQuant.Data Python SDK 导出 Pandas DataFrame 示例
演示如何将读取到的 Polars 数据自动转换为 Pandas DataFrame 以对接现有三方库。
"""

import cqdata


def main():
    table_id = "ashare.kline.1d.raw.baostock"
    print(f"=== 转换读取为 Pandas DataFrame (table: {table_id}) ===")

    # 设置 as_pandas=True
    df_pandas = cqdata.read_series(
        table_id=table_id,
        as_pandas=True
    )

    print(f"返回对象类型: {type(df_pandas)}")
    print(df_pandas.head(3))


if __name__ == "__main__":
    main()
