"""龙虎榜批量获取 Demo

从东财数据中心批量拉取龙虎榜数据。
公共模块: em_utils.py (em_get / em_datacenter)

支持按月分批拉取（by_month=True），避免单次请求数据量过大。

Usage:
    # 单日
    uv run python dragon_tiger_em.py --start 2026-06-18 --end 2026-06-18

    # 跨月批量（自动按月分批）
    uv run python dragon_tiger_em.py --start 2026-04-01 --end 2026-06-18

    # 指定股票
    uv run python dragon_tiger_em.py --code 000858 --start 2026-06-01 --end 2026-06-18

    # 导出CSV
    uv run python dragon_tiger_em.py --csv lhb_data.csv
"""

from __future__ import annotations

import argparse
import calendar
from datetime import datetime

import pandas as pd

from em_utils import em_get, em_datacenter


# ---------------------------------------------------------------------------
# 龙虎榜批量获取
# ---------------------------------------------------------------------------

_LHB_COLUMNS = (
    "SECURITY_CODE,SECUCODE,SECURITY_NAME_ABBR,TRADE_DATE,EXPLAIN,"
    "CLOSE_PRICE,CHANGE_RATE,BILLBOARD_NET_AMT,BILLBOARD_BUY_AMT,"
    "BILLBOARD_SELL_AMT,BILLBOARD_DEAL_AMT,ACCUM_AMOUNT,"
    "DEAL_NET_RATIO,DEAL_AMOUNT_RATIO,TURNOVERRATE,FREE_MARKET_CAP,"
    "EXPLANATION,D1_CLOSE_ADJCHRATE,D2_CLOSE_ADJCHRATE,"
    "D5_CLOSE_ADJCHRATE,D10_CLOSE_ADJCHRATE,SECURITY_TYPE_CODE"
)


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
    page_size: int = 5000,
) -> tuple[list[dict], int]:
    """拉取一页龙虎榜数据，返回 (rows, total_count)。"""
    from em_utils import DATACENTER_URL

    params = {
        "reportName": "RPT_DAILYBILLBOARD_DETAILSNEW",
        "columns": _LHB_COLUMNS,
        "filter": filter_str,
        "pageNumber": str(page),
        "pageSize": str(page_size),
        "sortColumns": "SECURITY_CODE,TRADE_DATE",
        "sortTypes": "1,-1",
        "source": "WEB",
        "client": "WEB",
    }
    r = em_get(DATACENTER_URL, params=params, timeout=15)
    d = r.json()
    result = d.get("result") or {}
    return result.get("data") or [], result.get("count", 0)


def fetch_dragon_tiger_batch(
    start_date: str = "2000-01-01",
    end_date: str | None = None,
    code: str | None = None,
    by_month: bool = True,
    page_size: int = 5000,
) -> pd.DataFrame:
    """批量拉取龙虎榜上榜记录。

    Args:
        start_date: 起始日期 (YYYY-MM-DD)
        end_date:   截止日期 (YYYY-MM-DD)，默认当天
        code:       指定股票代码（可选），不传则拉全市场
        by_month:   True=按月分批拉取（默认），False=一次性拉全部
        page_size:  每页条数，最大 5000

    Returns:
        pd.DataFrame
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    code_filter = ""
    if code:
        code_filter = f'(SECURITY_CODE="{_clean_code(code)}")'

    if code:
        # 个股：数据量小，直接一次性拉
        all_rows = _fetch_all_at_once(start_date, end_date, code_filter, page_size)
    elif by_month:
        # 全市场按月切分
        all_rows = _fetch_by_month(start_date, end_date, code_filter, page_size)
    else:
        all_rows = _fetch_all_at_once(start_date, end_date, code_filter, page_size)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    if "TRADE_DATE" in df.columns:
        df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"]).dt.strftime("%Y-%m-%d")

    amt_cols = [
        "BILLBOARD_NET_AMT", "BILLBOARD_BUY_AMT", "BILLBOARD_SELL_AMT",
        "BILLBOARD_DEAL_AMT", "ACCUM_AMOUNT", "FREE_MARKET_CAP",
    ]
    for col in amt_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _fetch_by_month(
    start_date: str,
    end_date: str,
    code_filter: str,
    page_size: int,
) -> list[dict]:
    """按月分批拉取，避免单次请求数据量过大。"""
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    all_rows: list[dict] = []
    cur = start_dt.replace(day=1)

    while cur <= end_dt:
        month_start = cur.strftime("%Y-%m-%d")
        last_day = calendar.monthrange(cur.year, cur.month)[1]
        month_end_dt = min(cur.replace(day=last_day), end_dt)
        month_end = month_end_dt.strftime("%Y-%m-%d")

        filter_str = (
            f"(TRADE_DATE>='{month_start}')"
            f"(TRADE_DATE<='{month_end}')"
            f"{code_filter}"
        )

        print(f"  [{month_start} ~ {month_end}] ", end="", flush=True)

        month_rows = _fetch_paginated(filter_str, page_size)
        all_rows.extend(month_rows)
        print(f"got {len(month_rows)} rows (cumulative: {len(all_rows)})")

        # 下个月
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)

    return all_rows


def _fetch_all_at_once(
    start_date: str,
    end_date: str,
    code_filter: str,
    page_size: int,
) -> list[dict]:
    """一次性拉取全部数据（自动分页）。"""
    filter_str = (
        f"(TRADE_DATE>='{start_date}')"
        f"(TRADE_DATE<='{end_date}')"
        f"{code_filter}"
    )
    return _fetch_paginated(filter_str, page_size)


def _fetch_paginated(filter_str: str, page_size: int) -> list[dict]:
    """自动分页拉取，直到拿完所有数据。"""
    all_rows: list[dict] = []
    page = 1

    while True:
        data, total = _fetch_one_page(filter_str, page, page_size)
        if not data:
            break

        all_rows.extend(data)
        if len(all_rows) >= total:
            break
        page += 1

    return all_rows


# ---------------------------------------------------------------------------
# 输出格式化
# ---------------------------------------------------------------------------

def format_output(df: pd.DataFrame) -> str:
    """将 DataFrame 格式化为可读文本输出。"""
    if df.empty:
        return "未查询到龙虎榜数据。"

    lines = [
        f"# 龙虎榜批量数据",
        f"# 数据量: {len(df)} 条",
        f"# 时间范围: {df['TRADE_DATE'].min()} ~ {df['TRADE_DATE'].max()}",
        f"# 查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        f"{'代码':<8} | {'名称':<10} | {'日期':<10} | {'原因':<20} | "
        f"{'收盘价':>7} | {'涨跌幅':>7} | {'净买入(万)':>10} | "
        f"{'买入(万)':>9} | {'卖出(万)':>9} | {'D1涨跌':>6} | {'D5涨跌':>6}",
        "-" * 140,
    ]

    for _, row in df.iterrows():
        code = str(row.get("SECURITY_CODE", ""))
        name = str(row.get("SECURITY_NAME_ABBR", ""))[:6]
        date = str(row.get("TRADE_DATE", ""))[:10]
        reason = str(row.get("EXPLAIN", ""))[:20]
        close = row.get("CLOSE_PRICE")
        chg = row.get("CHANGE_RATE")
        net = row.get("BILLBOARD_NET_AMT")
        buy = row.get("BILLBOARD_BUY_AMT")
        sell = row.get("BILLBOARD_SELL_AMT")
        d1 = row.get("D1_CLOSE_ADJCHRATE")
        d5 = row.get("D5_CLOSE_ADJCHRATE")

        close_s = f"{close:.2f}" if pd.notna(close) else "N/A"
        chg_s = f"{chg:.2f}%" if pd.notna(chg) else "N/A"
        net_s = f"{net/10000:.0f}" if pd.notna(net) else "N/A"
        buy_s = f"{buy/10000:.0f}" if pd.notna(buy) else "N/A"
        sell_s = f"{sell/10000:.0f}" if pd.notna(sell) else "N/A"
        d1_s = f"{d1:.2f}%" if pd.notna(d1) else "N/A"
        d5_s = f"{d5:.2f}%" if pd.notna(d5) else "N/A"

        lines.append(
            f"{code:<8} | {name:<10} | {date} | {reason:<20} | "
            f"{close_s:>7} | {chg_s:>7} | {net_s:>10} | {buy_s:>9} | {sell_s:>9} | "
            f"{d1_s:>6} | {d5_s:>6}"
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="龙虎榜批量获取 Demo")
    parser.add_argument("--code", default=None, help="股票代码，不传则全市场")
    parser.add_argument("--start", default=None, help="起始日期 YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="截止日期 YYYY-MM-DD")
    parser.add_argument("--csv", default=None, help="导出 CSV 路径")
    args = parser.parse_args()

    start = args.start or "2000-01-01"
    end = args.end or datetime.now().strftime("%Y-%m-%d")

    print(f"龙虎榜: {args.code or '全市场'} {start} ~ {end}")
    print()

    df = fetch_dragon_tiger_batch(
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
