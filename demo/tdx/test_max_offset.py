"""测试TDX在线接口各频率最大offset回溯范围。"""
from tdxpy.hq import TdxHq_API

SERVERS = [
    ("218.75.126.9", 7709),
    ("119.147.212.81", 7709),
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


def test_max_offset(api, category, name, symbol="600000", market=1):
    """通过二分法测试某频率最大offset。"""
    print("\n--- %s (category=%d, symbol=%s) ---" % (name, category, symbol))

    # 先确认offset=0有数据
    data = api.get_security_bars(category, market, symbol, 0, 800)
    if not data or len(data) == 0:
        print("  offset=0 无数据")
        return

    print("  offset=0: %d条, %s ~ %s" % (
        len(data), data[0]["datetime"], data[-1]["datetime"]))

    # 逐步增大offset找边界
    step = 800
    max_offset = 0
    last_data = data

    for off in range(step, 50000, step):
        data = api.get_security_bars(category, market, symbol, off, 800)
        if not data or len(data) == 0:
            max_offset = off - step
            break
        last_data = data
        max_offset = off

    if last_data:
        print("  最大offset=%d: %d条, %s ~ %s" % (
            max_offset, len(last_data),
            last_data[0]["datetime"], last_data[-1]["datetime"]))

        # 精确找边界
        low = max_offset
        high = max_offset + step
        while low < high:
            mid = (low + high + 1) // 2
            data = api.get_security_bars(category, market, symbol, mid, 800)
            if data and len(data) > 0:
                low = mid
                last_data = data
            else:
                high = mid - 1

        if last_data:
            print("  精确最大offset=%d: %s ~ %s" % (
                low, last_data[0]["datetime"], last_data[-1]["datetime"]))
    else:
        print("  无法确定最大offset")


if __name__ == "__main__":
    print("=" * 60)
    print("TDX 各频率最大offset回溯测试")
    print("=" * 60)

    api = connect()
    if api is None:
        exit(1)

    try:
        # 测试日线 (个股)
        test_max_offset(api, 4, "日线 (sh.600000)")
        # 测试日线 (指数)
        test_max_offset(api, 4, "日线 (sh.000001 上证指数)", symbol="000001", market=1)
        # 测试5分钟线
        test_max_offset(api, 0, "5分钟线 (sh.600000)")
        # 测试1分钟线
        test_max_offset(api, 7, "1分钟线 (sh.600000)")
    finally:
        api.disconnect()

    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)
