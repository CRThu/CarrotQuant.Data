"""
examples/05_export_pandas.py

CarrotQuant.Data Python SDK 导出 Pandas DataFrame 示例
演示如何将读取到的 Polars 数据自动转换为 Pandas DataFrame 以对接现有三方库。
"""

import cq.data


def main():
    table_id = "ashare.kline.1d.raw.baostock"
    print(f"=== 转换读取为 Pandas DataFrame (table: {table_id}) ===")

    # 调用 Polars 的 .to_pandas() 方法
    df_pandas = cq.data.read(
        table_id=table_id
    ).to_pandas()

    print(f"返回对象类型: {type(df_pandas)}")
    print(df_pandas.head(3))


if __name__ == "__main__":
    main()
