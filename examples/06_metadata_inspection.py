"""
examples/06_metadata_inspection.py

CarrotQuant.Data Python SDK 元数据与存储状态盘点示例
演示如何查看本地表清单、股票代码列表、存储格式、时间跨度、Schema 字段及数据条数。
"""

import cqdata


def main():
    table_id = "ashare.kline.1d.raw.baostock"

    print("=== 1. 盘点本地数据表 ===")
    print("已存在的数据表清单:", cqdata.list_tables())

    print(f"\n=== 2. 盘点表 [{table_id}] 的元信息 ===")
    print("存储格式:", cqdata.list_formats(table_id))
    print("股票代码列表:", cqdata.list_symbols(table_id))
    
    start_dt, end_dt = cqdata.get_time_range(table_id)
    print(f"数据覆盖时间跨度: {start_dt} ~ {end_dt}")
    print("物理总存储行数:", cqdata.get_row_count(table_id))

    print("\n=== 3. 查看字段 Schema 定义 ===")
    schema = cqdata.get_schema(table_id)
    for col_name, col_type in schema.items():
        print(f"  - {col_name}: {col_type}")


if __name__ == "__main__":
    main()
