"""探索通达信日线数据格式。

功能:
1. 解析 hsjday.zip 中的 .day 二进制文件
2. 验证解析结果
"""

import os
import sys
import zipfile
import struct
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def parse_tdx_day_file(data: bytes) -> list[dict]:
    """解析通达信日线二进制文件。

    TDX 日线格式 (32字节/条):
    - 4 bytes: 日期 (YYYYMMDD, little-endian uint32)
    - 4 bytes: 开盘价 * 100 (little-endian uint32)
    - 4 bytes: 最高价 * 100 (little-endian uint32)
    - 4 bytes: 最低价 * 100 (little-endian uint32)
    - 4 bytes: 收盘价 * 100 (little-endian uint32)
    - 4 bytes: 成交额 (little-endian float32)
    - 4 bytes: 成交量 (little-endian uint32)
    - 4 bytes: 保留 (little-endian uint32)
    """
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


def explore_zip():
    """探索 hsjday.zip 结构并解析示例数据。"""
    zip_path = Path(__file__).parent / "data" / "hsjday.zip"
    if not zip_path.exists():
        print(f"文件不存在: {zip_path}")
        return

    with zipfile.ZipFile(zip_path, 'r') as zf:
        file_list = zf.namelist()
        print(f"ZIP 文件包含 {len(file_list)} 个文件")

        # 统计目录结构
        sh_lday = [f for f in file_list if f.startswith('sh/lday/sh') and f.endswith('.day')]
        sz_lday = [f for f in file_list if f.startswith('sz/lday/sz') and f.endswith('.day')]
        bj_lday = [f for f in file_list if f.startswith('bj/lday/bj') and f.endswith('.day')]
        print(f"  sh/lday (上证): {len(sh_lday)} 个文件")
        print(f"  sz/lday (深证): {len(sz_lday)} 个文件")
        print(f"  bj/lday (北交): {len(bj_lday)} 个文件")

        # 解析 sh600000 (浦发银行) 日线
        target = 'sh/lday/sh600000.day'
        if target in file_list:
            print(f"\n解析 {target}:")
            data = zf.read(target)
            records = parse_tdx_day_file(data)
            print(f"  总记录数: {len(records)}")
            if records:
                print(f"  第一条: {records[0]}")
                print(f"  最后一条: {records[-1]}")
                print(f"  前5条:")
                for r in records[:5]:
                    print(f"    {r}")
                print(f"  后5条:")
                for r in records[-5:]:
                    print(f"    {r}")

        # 解析 sh000001 (上证指数) 日线
        target_idx = 'sh/lday/sh000001.day'
        if target_idx in file_list:
            print(f"\n解析 {target_idx}:")
            data = zf.read(target_idx)
            records = parse_tdx_day_file(data)
            print(f"  总记录数: {len(records)}")
            if records:
                print(f"  第一条: {records[0]}")
                print(f"  最后一条: {records[-1]}")

        # 解析 sz000001 (平安银行) 日线
        target_sz = 'sz/lday/sz000001.day'
        if target_sz in file_list:
            print(f"\n解析 {target_sz}:")
            data = zf.read(target_sz)
            records = parse_tdx_day_file(data)
            print(f"  总记录数: {len(records)}")
            if records:
                print(f"  第一条: {records[0]}")
                print(f"  最后一条: {records[-1]}")


if __name__ == "__main__":
    explore_zip()
