"""机构买卖每日统计 Demo

从东财数据中心批量拉取机构席位交易数据。
公共模块: em_utils.py (em_get / em_datacenter)

支持按月分批拉取（by_month=True），自动分页（每页500条）。

API: https://datacenter-web.eastmoney.com/api/data/v1/get
报表: RPT_ORGANIZATION_TRADE_DETAILSNEW

Usage:
    # 单日
    uv run python institutional_trade_daily_em.py --start 2026-06-18 --end 2026-06-18

    # 跨月批量（自动按月分批）
    uv run python institutional_trade_daily_em.py --start 2026-04-01 --end 2026-06-18

    # 指定股票
    uv run python institutional_trade_daily_em.py --code 000858 --start 2026-06-01 --end 2026-06-18

    # 导出CSV
    uv run python institutional_trade_daily_em.py --csv inst_data.csv
"""

from __future__ import annotations

import argparse
import calendar
from datetime import datetime

import pandas as pd

from em_utils import em_get, DATACENTER_URL

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

_REPORT_NAME = "RPT_ORGANIZATION_TRADE_DETAILSNEW"
_PAGE_SIZE = 500

_INST_COLUMNS = (
    # akshare 同款
    "ACCUM_AMOUNT,"        # akshare | 市场总成交额
    "BUY_AMT,"             # akshare | 机构买入总额
    "BUY_TIMES,"           # akshare | 买方机构数
    "CHANGE_RATE,"         # akshare | 涨跌幅
    "CLOSE_PRICE,"         # akshare | 收盘价
    "EXPLANATION,"         # akshare | 上榜原因
    "FREECAP,"             # akshare | 流通市值
    "NET_BUY_AMT,"         # akshare | 机构买入净额
    "RATIO,"               # akshare | 机构净买额占总成交额比
    "SECURITY_CODE,"       # akshare | 股票代码
    "SECURITY_NAME_ABBR,"  # akshare | 股票简称
    "SELL_AMT,"            # akshare | 机构卖出总额
    "SELL_TIMES,"          # akshare | 卖方机构数
    "TRADE_DATE,"          # akshare | 上榜日期
    "TURNOVERRATE,"        # akshare | 换手率
    # extra - 天数
    "D1_CLOSE_ADJCHRATE,"  # extra  | 上榜后1日涨跌幅
    "D2_CLOSE_ADJCHRATE,"  # extra  | 上榜后2日涨跌幅
    "D3_CLOSE_ADJCHRATE,"  # extra  | 上榜后3日涨跌幅
    "D5_CLOSE_ADJCHRATE,"  # extra  | 上榜后5日涨跌幅
    "D10_CLOSE_ADJCHRATE"  # extra  | 上榜后10日涨跌幅
    # --- 以下无用，已注释 ---
    # "SECUCODE,"            # 无用 | 代码全称，akshare丢弃
    # "BUY_COUNT,"           # 无用 | 非机构席位数，网页版无此列
    # "SELL_COUNT,"          # 无用 | 非机构席位数，网页版无此列
    # "MARKET,"              # 无用 | 深市/沪市/北交所
    # "TRADE_MARKET_CODE,"   # 无用 | 交易市场代码
    # "SECURITY_TYPE_CODE"   # 无用 | 全为同一值
)


# ---------------------------------------------------------------------------
# 数据获取
# ---------------------------------------------------------------------------

def _clean_code(code: str) -> str:
    """去掉 SH/SZ/BJ 前后缀，只留6位纯数字。"""
    clean = code.strip().upper()
    for prefix in ("SH", "SZ", "BJ"):
        if clean.startswith(prefix):
            clean = clean[len(prefix):]
    for suffix in (".SH", ".SZ", ".BJ"):
        if clean.endswith(suffix):
            clean = clean[:-len(suffix)]
    return clean


def _fetch_one_page(
    filter_str: str,
    page: int = 1,
) -> tuple[list[dict], int]:
    """拉取一页机构交易数据，返回 (rows, total_count)。"""
    params = {
        "reportName": _REPORT_NAME,
        "columns": _INST_COLUMNS,
        "filter": filter_str,
        "pageNumber": str(page),
        "pageSize": str(_PAGE_SIZE),
        "sortColumns": "TRADE_DATE,SECURITY_CODE",
        "sortTypes": "1,1",
        "source": "WEB",
        "client": "WEB",
    }
    r = em_get(DATACENTER_URL, params=params, timeout=15)
    d = r.json()
    result = d.get("result") or {}
    return result.get("data") or [], result.get("count", 0)


def _fetch_paginated(filter_str: str) -> list[dict]:
    """自动分页拉取，直到拿完所有数据。"""
    all_rows: list[dict] = []
    page = 1

    while True:
        data, total = _fetch_one_page(filter_str, page)
        if not data:
            break

        all_rows.extend(data)
        print(f"  page {page}: got {len(data)} rows (cumulative: {len(all_rows)}/{total})")
        if len(all_rows) >= total:
            break
        page += 1

    return all_rows


def fetch_inst_trade_batch(
    start_date: str = "2000-01-01",
    end_date: str | None = None,
    code: str | None = None,
    by_month: bool = True,
) -> pd.DataFrame:
    """批量拉取机构买卖每日统计。

    Args:
        start_date: 起始日期 (YYYY-MM-DD)
        end_date:   截止日期 (YYYY-MM-DD)，默认当天
        code:       指定股票代码（可选），不传则拉全市场
        by_month:   True=按月分批拉取（默认），False=一次性拉全部

    Returns:
        pd.DataFrame
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    code_filter = ""
    if code:
        code_filter = f'(SECURITY_CODE="{_clean_code(code)}")'

    if code:
        all_rows = _fetch_all_at_once(start_date, end_date, code_filter)
    elif by_month:
        all_rows = _fetch_by_month(start_date, end_date, code_filter)
    else:
        all_rows = _fetch_all_at_once(start_date, end_date, code_filter)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    if "TRADE_DATE" in df.columns:
        df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"]).dt.strftime("%Y-%m-%d")

    num_cols = [
        "CLOSE_PRICE", "CHANGE_RATE", "BUY_TIMES", "SELL_TIMES",
        "BUY_AMT", "SELL_AMT", "NET_BUY_AMT", "ACCUM_AMOUNT",
        "RATIO", "TURNOVERRATE", "FREECAP",
        "D1_CLOSE_ADJCHRATE", "D2_CLOSE_ADJCHRATE", "D3_CLOSE_ADJCHRATE",
        "D5_CLOSE_ADJCHRATE", "D10_CLOSE_ADJCHRATE",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    rename_map = {
        "SECURITY_CODE": "代码",
        "SECURITY_NAME_ABBR": "名称",
        "CLOSE_PRICE": "收盘价",
        "CHANGE_RATE": "涨跌幅",
        "BUY_TIMES": "买方机构数",
        "SELL_TIMES": "卖方机构数",
        "BUY_AMT": "机构买入总额",
        "SELL_AMT": "机构卖出总额",
        "NET_BUY_AMT": "机构买入净额",
        "ACCUM_AMOUNT": "市场总成交额",
        "RATIO": "机构净买额占总成交额比",
        "TURNOVERRATE": "换手率",
        "FREECAP": "流通市值",
        "EXPLANATION": "上榜原因",
        "TRADE_DATE": "上榜日期",
        "D1_CLOSE_ADJCHRATE": "上榜后1日",
        "D2_CLOSE_ADJCHRATE": "上榜后2日",
        "D3_CLOSE_ADJCHRATE": "上榜后3日",
        "D5_CLOSE_ADJCHRATE": "上榜后5日",
        "D10_CLOSE_ADJCHRATE": "上榜后10日",
    }
    df = df.rename(columns=rename_map)

    return df


def _fetch_by_month(
    start_date: str,
    end_date: str,
    code_filter: str,
) -> list[dict]:
    """按月分批拉取。"""
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    all_rows: list[dict] = []
    cur = start_dt.replace(day=1)

    while cur <= end_dt:
        month_start = max(cur, start_dt).strftime("%Y-%m-%d")
        last_day = calendar.monthrange(cur.year, cur.month)[1]
        month_end_dt = min(cur.replace(day=last_day), end_dt)
        month_end = month_end_dt.strftime("%Y-%m-%d")

        filter_str = (
            f"(TRADE_DATE>='{month_start}')"
            f"(TRADE_DATE<='{month_end}')"
            f"{code_filter}"
        )

        print(f"[{month_start} ~ {month_end}]")
        month_rows = _fetch_paginated(filter_str)
        all_rows.extend(month_rows)

        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)

    return all_rows


def _fetch_all_at_once(
    start_date: str,
    end_date: str,
    code_filter: str,
) -> list[dict]:
    """一次性拉取全部数据（自动分页）。"""
    filter_str = (
        f"(TRADE_DATE>='{start_date}')"
        f"(TRADE_DATE<='{end_date}')"
        f"{code_filter}"
    )
    print(f"[{start_date} ~ {end_date}]")
    return _fetch_paginated(filter_str)


# ---------------------------------------------------------------------------
# 输出格式化
# ---------------------------------------------------------------------------

def format_output(df: pd.DataFrame) -> str:
    """将 DataFrame 格式化为可读文本输出。"""
    if df.empty:
        return "未查询到机构交易数据。"

    lines = [
        "# 机构买卖每日统计",
        f"# 数据量: {len(df)} 条",
        f"# 时间范围: {df['上榜日期'].min()} ~ {df['上榜日期'].max()}",
        f"# 查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        f"{'代码':<8} | {'名称':<8} | {'上榜日期':<10} | "
        f"{'买次数':>5} | {'卖次数':>5} | "
        f"{'买入(万)':>10} | {'卖出(万)':>10} | {'净买(万)':>10} | "
        f"{'占比':>7} | {'换手':>6} | {'流通亿':>7} | "
        f"{'D1':>7} | {'D5':>7}",
        "-" * 130,
    ]

    for _, row in df.iterrows():
        code = str(row.get("代码", ""))
        name = str(row.get("名称", ""))[:6]
        date = str(row.get("上榜日期", ""))[:10]
        buy_t = row.get("买方机构数")
        sell_t = row.get("卖方机构数")
        buy = row.get("机构买入总额")
        sell = row.get("机构卖出总额")
        net = row.get("机构买入净额")
        ratio = row.get("机构净买额占总成交额比")
        turnover = row.get("换手率")
        free_cap = row.get("流通市值")
        d1 = row.get("D1_CLOSE_ADJCHRATE")
        d5 = row.get("D5_CLOSE_ADJCHRATE")

        buy_t_s = f"{int(buy_t)}" if pd.notna(buy_t) else "-"
        sell_t_s = f"{int(sell_t)}" if pd.notna(sell_t) else "-"
        buy_s = f"{buy/10000:.0f}" if pd.notna(buy) else "-"
        sell_s = f"{sell/10000:.0f}" if pd.notna(sell) else "-"
        net_s = f"{net/10000:.0f}" if pd.notna(net) else "-"
        ratio_s = f"{ratio:.2f}%" if pd.notna(ratio) else "-"
        turnover_s = f"{turnover:.2f}%" if pd.notna(turnover) else "-"
        cap_s = f"{free_cap:.1f}" if pd.notna(free_cap) else "-"
        d1_s = f"{d1:.2f}%" if pd.notna(d1) else "-"
        d5_s = f"{d5:.2f}%" if pd.notna(d5) else "-"

        lines.append(
            f"{code:<8} | {name:<8} | {date} | "
            f"{buy_t_s:>5} | {sell_t_s:>5} | "
            f"{buy_s:>10} | {sell_s:>10} | {net_s:>10} | "
            f"{ratio_s:>7} | {turnover_s:>6} | {cap_s:>7} | "
            f"{d1_s:>7} | {d5_s:>7}"
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="机构买卖每日统计 Demo")
    parser.add_argument("--code", default=None, help="股票代码，不传则全市场")
    parser.add_argument("--start", default=None, help="起始日期 YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="截止日期 YYYY-MM-DD")
    parser.add_argument("--csv", default=None, help="导出 CSV 路径")
    args = parser.parse_args()

    start = args.start or "2000-01-01"
    end = args.end or datetime.now().strftime("%Y-%m-%d")

    print(f"机构交易: {args.code or '全市场'} {start} ~ {end}")
    print()

    df = fetch_inst_trade_batch(
        start_date=start,
        end_date=end,
        code=args.code,
    )

    print()
    print(format_output(df))

    if args.csv and not df.empty:
        df.to_csv(args.csv, index=False, encoding="utf-8-sig")
        print(f"\n已导出到: {args.csv}")


if __name__ == "__main__":
    main()
