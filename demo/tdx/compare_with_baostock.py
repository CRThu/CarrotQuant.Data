"""TDX 数据与 Baostock 数据对比验证。

验证目标:
1. TDX 日线数据解析正确性
2. 与 Baostock 数据一致性
"""

import sys
import zipfile
import struct
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def parse_tdx_day_file(data: bytes) -> list[dict]:
    """解析通达信日线二进制文件。"""
    records = []
    record_size = 32
    num_records = len(data) // record_size

    for i in range(num_records):
        offset = i * record_size
        record = data[offset:offset + record_size]

        date_int = struct.unpack('<I', record[0:4])[0]
        year = date_int // 10000
        month = (date_int % 10000) // 100
        day = date_int % 100

        open_price = struct.unpack('<I', record[4:8])[0] / 100.0
        high_price = struct.unpack('<I', record[8:12])[0] / 100.0
        low_price = struct.unpack('<I', record[12:16])[0] / 100.0
        close_price = struct.unpack('<I', record[16:20])[0] / 100.0
        amount = struct.unpack('<f', record[20:24])[0]
        volume = struct.unpack('<I', record[24:28])[0]

        records.append({
            'date': f"{year:04d}-{month:02d}-{day:02d}",
            'open': round(open_price, 2),
            'high': round(high_price, 2),
            'low': round(low_price, 2),
            'close': round(close_price, 2),
            'volume': volume,
            'amount': round(amount, 2),
        })

    return records


def get_baostock_data(symbol: str, start_date: str, end_date: str):
    """从 Baostock 获取日线数据。"""
    import baostock as bs

    lg = bs.login()
    if lg.error_code != '0':
        raise RuntimeError(f"Baostock login failed: {lg.error_msg}")

    fields = "date,code,open,high,low,close,volume,amount"
    rs = bs.query_history_k_data_plus(
        symbol, fields,
        start_date=start_date, end_date=end_date,
        frequency='d', adjustflag='3'  # raw
    )

    data_list = []
    while (rs.error_code == '0') and rs.next():
        data_list.append(rs.get_row_data())

    bs.logout()

    result = []
    for row in data_list:
        result.append({
            'date': row[0],
            'symbol': row[1],
            'open': float(row[2]) if row[2] else 0,
            'high': float(row[3]) if row[3] else 0,
            'low': float(row[4]) if row[4] else 0,
            'close': float(row[5]) if row[5] else 0,
            'volume': float(row[6]) if row[6] else 0,
            'amount': float(row[7]) if row[7] else 0,
        })

    return result


def compare_data(tdx_records: list[dict], bs_records: list[dict], symbol: str):
    """对比 TDX 和 Baostock 数据。"""
    # 按日期索引
    tdx_by_date = {r['date']: r for r in tdx_records}
    bs_by_date = {r['date']: r for r in bs_records}

    # 找共同日期
    common_dates = sorted(set(tdx_by_date.keys()) & set(bs_by_date.keys()))
    print(f"\n  {symbol}: TDX {len(tdx_records)} 条, Baostock {len(bs_records)} 条, 共同日期 {len(common_dates)} 条")

    if not common_dates:
        print("  无共同日期可比较")
        return

    # 采样比较 (取前5个、中间5个、后5个)
    sample_dates = common_dates[:5] + common_dates[len(common_dates)//2:len(common_dates)//2+5] + common_dates[-5:]
    sample_dates = sorted(set(sample_dates))

    price_diff_count = 0
    max_diff = 0

    for date in sample_dates:
        tdx_r = tdx_by_date[date]
        bs_r = bs_by_date[date]

        # 比较收盘价
        diff = abs(tdx_r['close'] - bs_r['close'])
        max_diff = max(max_diff, diff)
        if diff > 0.01:
            price_diff_count += 1
            print(f"  {date}: TDX close={tdx_r['close']}, BS close={bs_r['close']}, diff={diff:.4f}")

    print(f"  价格差异 > 0.01 的天数: {price_diff_count}/{len(sample_dates)}")
    print(f"  最大价差: {max_diff:.4f}")

    if price_diff_count == 0:
        print("  ✓ 数据一致")
    else:
        print("  ✗ 存在差异")


def main():
    zip_path = Path(__file__).parent / "data" / "hsjday.zip"
    if not zip_path.exists():
        print(f"请先运行 explore_tdx.py 下载数据")
        return

    # 测试股票列表
    test_stocks = [
        ("sh600000", "sh.600000", "sh/lday/sh600000.day"),   # 浦发银行
        ("sh600036", "sh.600036", "sh/lday/sh600036.day"),   # 招商银行
        ("sz000001", "sz.000001", "sz/lday/sz000001.day"),   # 平安银行
        ("sz000858", "sz.000858", "sz/lday/sz000858.day"),   # 五粮液
    ]

    with zipfile.ZipFile(zip_path, 'r') as zf:
        for tdx_code, bs_code, tdx_path in test_stocks:
            if tdx_path not in zf.namelist():
                print(f"  {tdx_path} 不存在，跳过")
                continue

            # 解析 TDX 数据
            data = zf.read(tdx_path)
            tdx_records = parse_tdx_day_file(data)

            # 获取 Baostock 数据 (取最近100天)
            if tdx_records:
                last_date = tdx_records[-1]['date']
                # 取最后100条记录的日期范围
                start_date = tdx_records[-100]['date'] if len(tdx_records) >= 100 else tdx_records[0]['date']

                try:
                    bs_records = get_baostock_data(bs_code, start_date, last_date)
                    compare_data(tdx_records, bs_records, bs_code)
                except Exception as e:
                    print(f"  {bs_code} Baostock 获取失败: {e}")

    # 指数对比
    print("\n=== 指数对比 ===")
    index_test = [
        ("sh000001", "sh.000001", "sh/lday/sh000001.day"),   # 上证指数
    ]

    with zipfile.ZipFile(zip_path, 'r') as zf:
        for tdx_code, bs_code, tdx_path in index_test:
            if tdx_path not in zf.namelist():
                continue

            data = zf.read(tdx_path)
            tdx_records = parse_tdx_day_file(data)

            if tdx_records:
                last_date = tdx_records[-1]['date']
                start_date = tdx_records[-100]['date'] if len(tdx_records) >= 100 else tdx_records[0]['date']

                try:
                    bs_records = get_baostock_data(bs_code, start_date, last_date)
                    compare_data(tdx_records, bs_records, bs_code)
                except Exception as e:
                    print(f"  {bs_code} Baostock 获取失败: {e}")


if __name__ == "__main__":
    print("=" * 60)
    print("TDX 数据与 Baostock 数据对比验证")
    print("=" * 60)
    main()
