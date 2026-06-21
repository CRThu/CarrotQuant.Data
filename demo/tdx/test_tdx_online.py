"""TDX 在线接口能力全面测试。"""
from tdxpy.hq import TdxHq_API
import pandas as pd

SERVERS = [
    ("119.147.212.81", 7709),
    ("218.75.126.9", 7709),
    ("115.238.56.198", 7709),
    ("124.160.88.183", 7709),
    ("60.12.136.250", 7709),
    ("218.108.98.244", 7709),
    ("218.108.47.69", 7709),
    ("180.153.39.51", 7709),
    ("110.41.147.114", 7709),
    ("8.129.13.54", 7709),
]


def connect():
    api = TdxHq_API()
    for ip, port in SERVERS:
        try:
            r = api.connect(ip, port)
            if r:
                print("连接成功: %s:%d" % (ip, port))
                return api
        except Exception:
            continue
    print("所有服务器连接失败")
    return None


def test_bars(api):
    """测试K线数据获取能力。"""
    # category: 0=5分钟, 1=15分钟, 2=30分钟, 3=60分钟, 4=日线, 5=周线, 6=月线, 7=1分钟
    # market: 0=深圳, 1=上海
    # offset最大800

    categories = [
        (4, "日线"),
        (0, "5分钟线"),
        (7, "1分钟线"),
        (3, "60分钟线"),
        (2, "30分钟线"),
        (1, "15分钟线"),
        (5, "周线"),
        (6, "月线"),
    ]

    print("\n=== K线数据获取能力 ===")
    for cat, name in categories:
        try:
            data = api.get_security_bars(cat, 1, "600000", 0, 800)
            if data and len(data) > 0:
                first = data[0]
                last = data[-1]
                print("  %s (cat=%d): %d条, %s ~ %s" % (
                    name, cat, len(data),
                    first.get("datetime", "N/A"),
                    last.get("datetime", "N/A"),
                ))
            else:
                print("  %s (cat=%d): 无数据" % (name, cat))
        except Exception as e:
            print("  %s (cat=%d): 失败 - %s" % (name, cat, str(e)[:60]))


def test_offset(api):
    """测试偏移获取，确认最大历史深度。"""
    print("\n=== 偏移获取测试 (日线) ===")
    # 800条为一个批次，通过偏移获取更早数据
    offsets = [0, 800, 1600, 2400, 3200, 4000, 4800]
    for off in offsets:
        try:
            data = api.get_security_bars(4, 1, "600000", off, 800)
            if data and len(data) > 0:
                first = data[0]
                last = data[-1]
                print("  offset=%d: %d条, %s ~ %s" % (
                    off, len(data),
                    first.get("datetime", "N/A"),
                    last.get("datetime", "N/A"),
                ))
            else:
                print("  offset=%d: 无数据" % off)
                break
        except Exception as e:
            print("  offset=%d: 失败 - %s" % (off, str(e)[:60]))
            break


def test_xdxr(api):
    """测试除权除息信息 (复权因子)。"""
    print("\n=== 除权除息信息 (xdxr) ===")
    try:
        xdxr = api.get_xdxr_info(1, "600000")
        if xdxr and len(xdxr) > 0:
            print("  获取 %d 条除权除息记录" % len(xdxr))
            print("  字段: %s" % list(xdxr[0].keys()) if xdxr else "无")
            # 显示前3条
            for i, rec in enumerate(xdxr[:3]):
                print("  [%d] %s" % (i, rec))
        else:
            print("  无除权除息记录")
    except Exception as e:
        print("  获取失败: %s" % str(e)[:80])


def test_finance(api):
    """测试财务信息。"""
    print("\n=== 财务信息 ===")
    try:
        fin = api.get_finance_info(1, "600000")
        if fin:
            print("  字段: %s" % list(fin.keys()))
            print("  内容: %s" % fin)
        else:
            print("  无财务信息")
    except Exception as e:
        print("  获取失败: %s" % str(e)[:80])


def test_index(api):
    """测试指数数据。"""
    print("\n=== 指数数据 ===")
    # 上证指数: market=1, code=000001
    try:
        data = api.get_index_bars(4, 1, "000001", 0, 800)
        if data and len(data) > 0:
            print("  上证指数日线: %d条, %s ~ %s" % (
                len(data),
                data[0].get("datetime", "N/A"),
                data[-1].get("datetime", "N/A"),
            ))
        else:
            print("  无数据")
    except Exception as e:
        print("  失败: %s" % str(e)[:80])


def test_stock_list(api):
    """测试股票列表获取。"""
    print("\n=== 股票列表 ===")
    try:
        # 获取上海市场股票列表
        stocks = api.get_security_list(1, 0)
        if stocks and len(stocks) > 0:
            print("  上海市场: %d 只股票" % len(stocks))
            print("  字段: %s" % list(stocks[0].keys()))
            print("  示例: %s" % stocks[0])
        else:
            print("  无数据")
    except Exception as e:
        print("  失败: %s" % str(e)[:80])


if __name__ == "__main__":
    print("=" * 60)
    print("TDX 在线接口能力全面测试")
    print("=" * 60)

    api = connect()
    if api is None:
        exit(1)

    try:
        test_bars(api)
        test_offset(api)
        test_xdxr(api)
        test_finance(api)
        test_index(api)
        test_stock_list(api)
    finally:
        api.disconnect()

    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)
