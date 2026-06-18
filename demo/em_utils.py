"""东财数据中心公共模块 — 节流请求 + datacenter 统一查询。

所有 eastmoney.com 请求通过 em_get() 走统一入口，自动限流防封。

Ref: https://github.com/simonlin1212/TradingAgents-astoc (a_stock.py)

Usage:
    from em_utils import em_get, em_datacenter

    # 直接请求
    r = em_get("https://push2.eastmoney.com/api/qt/stock/get", params={...})

    # datacenter 统一查询
    rows = em_datacenter(
        "RPT_DAILYBILLBOARD_DETAILSNEW",
        filter_str="(TRADE_DATE>='2026-06-01')",
    )
"""

from __future__ import annotations

import os
import random
import time

import requests

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
DATACENTER_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"

# ---------------------------------------------------------------------------
# 东财防封：全局节流 + 会话复用
# ---------------------------------------------------------------------------

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": UA})
_MIN_INTERVAL = float(os.environ.get("EM_MIN_INTERVAL", "1.0"))
_last_call = [0.0]


def em_get(
    url: str,
    params: dict | None = None,
    headers: dict | None = None,
    timeout: int = 15,
    **kwargs,
) -> requests.Response:
    """东财统一请求入口：自动节流 + 复用 session + 默认 UA。

    Ref: a_stock.py _em_get()
    风控规则: >5次/秒 或 并发≥10 或 1分钟≥200次 → 封 IP。
    串行限流：与上次请求间隔 < EM_MIN_INTERVAL 时 sleep 补足 + 随机抖动。
    """
    wait = _MIN_INTERVAL - (time.time() - _last_call[0])
    if wait > 0:
        time.sleep(wait + random.uniform(0.1, 0.5))
    try:
        return _SESSION.get(
            url, params=params, headers=headers, timeout=timeout, **kwargs
        )
    finally:
        _last_call[0] = time.time()


# ---------------------------------------------------------------------------
# datacenter 统一查询
# ---------------------------------------------------------------------------


def em_datacenter(
    report_name: str,
    columns: str = "ALL",
    filter_str: str = "",
    page_size: int = 50,
    sort_columns: str = "",
    sort_types: str = "-1",
) -> list[dict]:
    """东财数据中心统一查询 — 龙虎榜/解禁/融资融券等共用。"""
    params = {
        "reportName": report_name,
        "columns": columns,
        "filter": filter_str,
        "pageNumber": "1",
        "pageSize": str(page_size),
        "sortColumns": sort_columns,
        "sortTypes": sort_types,
        "source": "WEB",
        "client": "WEB",
    }
    r = em_get(DATACENTER_URL, params=params, timeout=15)
    d = r.json()
    if d.get("result") and d["result"].get("data"):
        return d["result"]["data"]
    return []
