"""行业板块 Demo

从东财 push2 接口拉取沪深京所有行业板块 + 板块成分股数据。
公共模块: em_utils.py (em_push2)

接口: stock_board_industry_name_em / stock_board_industry_cons_em (akshare同款)
目标地址: https://quote.eastmoney.com/center/boardlist.html#industry_board
浏览器测试: https://push2.eastmoney.com/webguest/api/qt/clist/get?np=1&fltt=1&invt=2&fs=m:90+t:2+f:!50&fields=f12,f14&fid=f3&pn=1&pz=5&po=1&dect=1&ut=fa5fd1943c7b386f172d6893dbfba10b

Usage:
    # 获取全部行业板块
    uv run industry_board_em.py

    # 某个板块的成分股
    uv run industry_board_em.py --cons BK0733

    # 按板块名称查成分股
    uv run industry_board_em.py --cons "汽车整车"

    # 导出CSV
    uv run industry_board_em.py --csv industry_board.csv
    uv run industry_board_em.py --cons BK0733 --csv cons.csv
"""

from __future__ import annotations

import argparse
import math
import re
from datetime import datetime

import pandas as pd

from demo.em.em_utils import em_push2

# ---------------------------------------------------------------------------
# 常量 — 与 akshare 完全一致
# ---------------------------------------------------------------------------

# 板块列表字段 (fs=m:90 t:2 f:!50 = 行业板块)
_BOARD_FIELDS = (
    "f12,"     # 板块代码
    "f14"      # 板块名称
    # "f13,"    # 市场 (0=深圳 1=上海)
    # "f2,"     # 最新价
    # "f3,"     # 涨跌幅
    # "f4,"     # 涨跌额
    # "f8,"     # 换手率
    # "f11,"    # 量比
    # "f15,"    # 最高
    # "f16,"    # 最低
    # "f17,"    # 今开
    # "f18,"    # 昨收
    # "f20,"    # 总市值
    # "f21,"    # 流通市值
    # "f22,"    # 涨速
    # "f24,"    # 60日涨跌幅
    # "f25,"    # 年初至今涨跌幅
    # "f33,"    # 委比
    # "f62,"    # 主力净流入
    # "f104,"   # 上涨家数
    # "f105,"   # 下跌家数
    # "f107,"   # 板块类型
    # "f124,"   # 领涨股所属板块
    # "f128,"   # 领涨股票
    # "f136"    # 领涨股票-涨跌幅
    # "f140,"   # 领涨股票代码
    # "f141"    # 领涨股票所属板块
    # "f207,"   # 领涨股-涨跌幅
    # "f208,"   # 领涨股-涨速
    # "f209,"   # 领涨股-60日涨跌幅
    # "f222"    # 领涨股-年初至今涨跌幅
)

_BOARD_FIELD_MAP = {
    "f12": "板块代码",
    "f14": "板块名称",
}

# 成分股字段 (fs=b:{板块代码} f:!50 = 板块成分股)
_CONS_FIELDS = (
    "f12,"     # 代码
    "f14"      # 名称
    # "f2,"     # 最新价
    # "f3,"     # 涨跌幅
    # "f4,"     # 涨跌额
    # "f5,"     # 成交量
    # "f6,"     # 成交额
    # "f7,"     # 振幅
    # "f8,"     # 换手率
    # "f9,"     # 市盈率-动态
    # "f10,"    # 量比
    # "f13,"    # 市场 (0=深圳 1=上海)
    # "f15,"    # 最高
    # "f16,"    # 最低
    # "f17,"    # 今开
    # "f18,"    # 昨收
    # "f20,"    # 总市值
    # "f21,"    # 流通市值
    # "f22,"    # 涨速
    # "f23,"    # 市净率
    # "f24,"    # 60日涨跌幅
    # "f25,"    # 年初至今涨跌幅
    # "f33,"    # 委比
    # "f62,"    # 主力净流入
    # "f115,"   # 市盈率(TTM)
    # "f128,"   # 领涨股票
    # "f136"    # 领涨股票-涨跌幅
)

_CONS_FIELD_MAP = {
    "f12": "代码",
    "f14": "名称",
}


# ---------------------------------------------------------------------------
# 板块列表
# ---------------------------------------------------------------------------

def fetch_industry_boards() -> pd.DataFrame:
    """拉取所有行业板块（akshare stock_board_industry_name_em 同款）。

    fs=m:90 t:2 f:!50 = 行业板块。
    """
    params = {
        "pn": "1", "pz": "100", "po": "1", "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2", "invt": "2", "fid": "f12",
        "fs": "m:90 t:2 f:!50",
        "fields": _BOARD_FIELDS,
    }

    r = em_push2(params=params, timeout=15)
    data_json = r.json()
    diff = data_json["data"]["diff"] or []
    total = data_json["data"]["total"]
    if not diff:
        return pd.DataFrame(columns=["序号", "板块代码", "板块名称"])
    per_page = len(diff)
    total_page = math.ceil(total / per_page)

    all_rows = list(diff)

    for page in range(2, total_page + 1):
        params["pn"] = str(page)
        r = em_push2(params=params, timeout=15)
        data_json = r.json()
        all_rows.extend(data_json["data"]["diff"] or [])

    mapped = [
        {_BOARD_FIELD_MAP[k]: v for k, v in row.items() if k in _BOARD_FIELD_MAP}
        for row in all_rows
    ]
    df = pd.DataFrame(mapped)
    df.insert(0, "序号", range(1, len(df) + 1))
    return df


# ---------------------------------------------------------------------------
# 板块成分股
# ---------------------------------------------------------------------------

def _resolve_board_code(boards_df: pd.DataFrame, symbol: str) -> str:
    """将板块名称或代码解析为板块代码 (BK开头)。"""
    if re.match(r"^BK\d+", symbol):
        return symbol
    match = boards_df[boards_df["板块名称"] == symbol]
    if match.empty:
        raise ValueError(f"找不到板块 '{symbol}'")
    return match.iloc[0]["板块代码"]


def fetch_board_cons(board_code: str) -> pd.DataFrame:
    """拉取指定板块的成分股（akshare stock_board_industry_cons_em 同款）。

    fs=b:{board_code} f:!50 = 板块成分股。
    """
    params = {
        "pn": "1", "pz": "100", "po": "1", "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2", "invt": "2", "fid": "f12",
        "fs": f"b:{board_code} f:!50",
        "fields": _CONS_FIELDS,
    }

    r = em_push2(params=params, timeout=15)
    data_json = r.json()
    diff = data_json["data"]["diff"] or []
    total = data_json["data"]["total"]
    if not diff:
        return pd.DataFrame(columns=["序号", "代码", "名称"])
    per_page = len(diff)
    total_page = math.ceil(total / per_page)

    all_rows = list(diff)

    for page in range(2, total_page + 1):
        params["pn"] = str(page)
        r = em_push2(params=params, timeout=15)
        data_json = r.json()
        all_rows.extend(data_json["data"]["diff"] or [])

    mapped = [
        {_CONS_FIELD_MAP[k]: v for k, v in row.items() if k in _CONS_FIELD_MAP}
        for row in all_rows
    ]
    df = pd.DataFrame(mapped)
    df.insert(0, "序号", range(1, len(df) + 1))

    return df


# ---------------------------------------------------------------------------
# 输出格式化
# ---------------------------------------------------------------------------

def format_board_output(df: pd.DataFrame) -> str:
    if df.empty:
        return "未查询到行业板块数据。"

    lines = [
        "# 行业板块",
        f"# 数量: {len(df)} 个板块",
        f"# 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        f"{'序号':>4} | {'板块代码':<10} | {'板块名称':<14}",
        "-" * 35,
    ]

    for _, row in df.iterrows():
        seq = int(row.get("序号", 0))
        code = str(row.get("板块代码", ""))
        name = str(row.get("板块名称", ""))
        lines.append(f"{seq:>4} | {code:<10} | {name:<14}")

    return "\n".join(lines)


def format_cons_output(df: pd.DataFrame, board_name: str, board_code: str) -> str:
    if df.empty:
        return f"板块 {board_name}({board_code}) 无成分股数据。"

    lines = [
        f"# {board_name}({board_code}) 成分股",
        f"# 数量: {len(df)} 只",
        f"# 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        f"{'序号':>4} | {'代码':<8} | {'名称':<10}",
        "-" * 28,
    ]

    for _, row in df.iterrows():
        seq = int(row.get("序号", 0))
        code = str(row.get("代码", ""))
        name = str(row.get("名称", ""))
        lines.append(f"{seq:>4} | {code:<8} | {name:<10}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="行业板块 Demo")
    parser.add_argument("--cons", default=None,
                        help="查询板块成分股，传入板块代码(BK0733)或名称(汽车整车)")
    parser.add_argument("--csv", default=None, help="导出 CSV 路径")
    args = parser.parse_args()

    print("拉取行业板块列表...")
    boards_df = fetch_industry_boards()
    print()

    if boards_df.empty:
        print("未获取到数据，请检查网络连接。")
        return

    if args.cons:
        try:
            board_code = _resolve_board_code(boards_df, args.cons)
        except ValueError as e:
            print(f"错误: {e}")
            return

        board_name_match = boards_df[boards_df["板块代码"] == board_code]
        board_name = board_name_match.iloc[0]["板块名称"] if not board_name_match.empty else board_code

        print(f"拉取 {board_name}({board_code}) 成分股...")
        cons_df = fetch_board_cons(board_code)
        print()

        print(format_cons_output(cons_df, board_name, board_code))

        if args.csv and not cons_df.empty:
            cons_df.to_csv(args.csv, index=False, encoding="utf-8-sig")
            print(f"\n已导出到: {args.csv}")
    else:
        print(format_board_output(boards_df))

        if args.csv and not boards_df.empty:
            boards_df.to_csv(args.csv, index=False, encoding="utf-8-sig")
            print(f"\n已导出到: {args.csv}")


if __name__ == "__main__":
    main()
