"""mootdx 在线接口能力测试。"""
from mootdx.quotes import Quotes

print("=== mootdx 在线接口能力测试 ===")

client = Quotes.factory(market="std")
print("连接成功!")

# 日线
print("\n=== 1. 日线数据 (category=4) ===")
df = client.bars(symbol="600000", category=4, offset=800)
print("获取 %d 条" % len(df))
print("最早: %s" % df.iloc[0]["datetime"])
print("最新: %s" % df.iloc[-1]["datetime"])
print("列名: %s" % list(df.columns))

# 5分钟线
print("\n=== 2. 5分钟线 (category=0) ===")
df5 = client.bars(symbol="600000", category=0, offset=800)
print("获取 %d 条" % len(df5))
print("最早: %s" % df5.iloc[0]["datetime"])
print("最新: %s" % df5.iloc[-1]["datetime"])

# 1分钟线
print("\n=== 3. 1分钟线 (category=7) ===")
df1 = client.bars(symbol="600000", category=7, offset=800)
print("获取 %d 条" % len(df1))
print("最早: %s" % df1.iloc[0]["datetime"])
print("最新: %s" % df1.iloc[-1]["datetime"])

# 除权除息 (复权因子)
print("\n=== 4. 除权除息信息 (xdxr) ===")
xdxr = client.xdxr(symbol="600000")
if xdxr is not None and not xdxr.empty:
    print("获取 %d 条除权除息记录" % len(xdxr))
    print("列名: %s" % list(xdxr.columns))
    print(xdxr.head(3).to_string())
else:
    print("无除权除息记录")

# 财务信息
print("\n=== 5. 财务信息 ===")
fin = client.finance(symbol="600000")
if fin is not None:
    print(fin)
