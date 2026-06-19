"""东财数据中心公共模块 — 节流请求 + push2/datacenter 统一查询。

所有 eastmoney.com 请求通过 em_get() / em_push2() 走统一入口，自动限流防封。
push2 接口支持多 URL 回退，应对东财封禁。

核心防封策略:
  1. curl_cffi TLS 指纹模拟浏览器 (JA3 hash + sec-ch-ua)
  2. 全局节流 + 请求间隔控制
  3. tenacity 自动重试 (指数退避)

Ref: https://github.com/simonlin1212/TradingAgents-astoc (a_stock.py)

Usage:
    from em_utils import em_get, em_push2, em_datacenter

    # push2 请求（自动回退多个 URL）
    r = em_push2(params={...})

    # 直接请求
    r = em_get("https://some-url.com/api", params={...})

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

from curl_cffi.requests import Session
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
DATACENTER_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"

# push2 行情接口 — 按优先级排列，遇封禁自动回退
# 参考: https://quote.eastmoney.com/center/gridlist.html
# 浏览器测试: https://push2.eastmoney.com/webguest/api/qt/clist/get?np=1&fltt=1&invt=2&fs=m:90+t:3+f:!50&fields=f12,f13,f14&fid=f3&pn=1&pz=5&po=1&dect=1&ut=fa5fd1943c7b386f172d6893dbfba10b
# 2026-06: 原 /api/ 路径被封，webguest/weblogin 仍可用
PUSH2_URLS = [
    "https://push2.eastmoney.com/webguest/api/qt/clist/get",
    "https://push2.eastmoney.com/weblogin/api/qt/clist/get",
    "https://push2.eastmoney.com/api/qt/clist/get",
]

# ---------------------------------------------------------------------------
# 东财防封：TLS 指纹 + 节流 + 自动重试
# ---------------------------------------------------------------------------

_MIN_INTERVAL = float(os.environ.get("EM_MIN_INTERVAL", "1.0"))
_MAX_RETRIES = int(os.environ.get("EM_MAX_RETRIES", "3"))
_last_call = [0.0]

# curl_cffi session — impersonate 自动处理 TLS 指纹 + sec-ch-ua 头
_SESSION = Session(impersonate="chrome")
_SESSION.headers.update({
    "Accept-Language": "zh-CN,zh;q=0.9",
})
# 代理：优先读环境变量 EM_PROXY，否则走系统代理
_EM_PROXY = os.environ.get("EM_PROXY", "")
if _EM_PROXY:
    _SESSION.proxies = {"http": _EM_PROXY, "https": _EM_PROXY}


def _throttle() -> None:
    """全局节流：确保请求间隔不低于 EM_MIN_INTERVAL。"""
    wait = _MIN_INTERVAL - (time.time() - _last_call[0])
    if wait > 0:
        time.sleep(wait + random.uniform(0.1, 0.5))
    _last_call[0] = time.time()


@retry(
    stop=stop_after_attempt(_MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((ConnectionError, TimeoutError)),
    reraise=True,
)
def em_get(
    url: str,
    params: dict | None = None,
    headers: dict | None = None,
    timeout: int = 15,
    **kwargs,
) -> "Session.Response":
    """东财统一请求入口：自动节流 + 重试 + TLS 指纹模拟。

    Ref: a_stock.py _em_get()
    风控规则: >5次/秒 或 并发≥10 或 1分钟≥200次 → 封 IP。
    重试: tenacity 指数退避，仅对连接/超时错误重试。
    """
    _throttle()
    return _SESSION.get(
        url, params=params, headers=headers, timeout=timeout, **kwargs
    )


def em_push2(
    params: dict,
    timeout: int = 15,
    referer: str = "https://quote.eastmoney.com/center/gridlist.html",
    **kwargs,
) -> "Session.Response":
    """push2 行情接口：自动回退多个 URL，应对东财封禁。

    按 PUSH2_URLS 顺序逐个尝试，首个成功即返回。
    自动添加 Referer 头（push2 接口需要）。
    """
    headers = {"Referer": referer}
    last_exc: Exception | None = None
    for url in PUSH2_URLS:
        try:
            r = em_get(url, params=params, headers=headers, timeout=timeout, **kwargs)
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
            continue
    raise RuntimeError(
        f"所有 push2 URL 均失败: {[u.split('/')[-2] for u in PUSH2_URLS]}"
    ) from last_exc


# datacenter 统一查询
# 参考: https://datacenter-web.eastmoney.com/api/data/v1/get
# 报表列表: https://data.eastmoney.com/stock/lhb.html


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
