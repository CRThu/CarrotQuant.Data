"""东财数据源驱动实现。

支持以下 table_id:
  - ashare.concept.eastmoney       (EV) 概念板块成分股
  - ashare.industry.eastmoney      (EV) 行业板块成分股
  - ashare.dragon_tiger.eastmoney  (EV) 龙虎榜
  - ashare.inst_trade.eastmoney    (EV) 机构买卖每日统计
"""

from __future__ import annotations

import math
import re
from datetime import datetime
from typing import Any

import polars as pl
from loguru import logger

from app.provider.base import BaseProvider
from app.provider.data_cleaner import DataCleaner
from app.provider.em_utils import em_push2, em_datacenter
from app.utils.time_utils import ts_to_str


class EastMoneyProvider(BaseProvider):
    """东财数据源驱动，对接 push2 / datacenter-web 接口。"""

    _SUPPORTED_TABLE_MAP: dict[str, str] = {
        "ashare.concept.eastmoney": "event",
        "ashare.industry.eastmoney": "event",
        "ashare.dragon_tiger.eastmoney": "event",
        "ashare.inst_trade.eastmoney": "event",
    }

    # push2 板块列表字段
    _BOARD_FIELDS = "f12,f14"
    _BOARD_FIELD_MAP = {"f12": "board_code", "f14": "board_name"}

    # 板块列表缓存: {board_type: {"board_code": "board_name"}}
    _board_name_cache: dict[str, dict[str, str]] = {}

    # push2 成分股字段
    _CONS_FIELDS = "f12,f14"
    _CONS_FIELD_MAP = {"f12": "symbol", "f14": "stock_name"}

    # datacenter 龙虎榜字段
    _LHB_COLUMNS = (
        "ACCUM_AMOUNT,BILLBOARD_BUY_AMT,BILLBOARD_DEAL_AMT,BILLBOARD_NET_AMT,"
        "BILLBOARD_SELL_AMT,CHANGE_RATE,CLOSE_PRICE,D1_CLOSE_ADJCHRATE,"
        "D2_CLOSE_ADJCHRATE,D5_CLOSE_ADJCHRATE,D10_CLOSE_ADJCHRATE,"
        "DEAL_AMOUNT_RATIO,DEAL_NET_RATIO,EXPLAIN,EXPLANATION,FREE_MARKET_CAP,"
        "SECURITY_CODE,SECURITY_NAME_ABBR,TRADE_DATE,TURNOVERRATE"
    )

    # datacenter 机构交易字段
    _INST_COLUMNS = (
        "ACCUM_AMOUNT,BUY_AMT,BUY_TIMES,CHANGE_RATE,CLOSE_PRICE,EXPLANATION,"
        "FREECAP,NET_BUY_AMT,RATIO,SECURITY_CODE,SECURITY_NAME_ABBR,SELL_AMT,"
        "SELL_TIMES,TRADE_DATE,TURNOVERRATE,D1_CLOSE_ADJCHRATE,"
        "D2_CLOSE_ADJCHRATE,D3_CLOSE_ADJCHRATE,D5_CLOSE_ADJCHRATE,"
        "D10_CLOSE_ADJCHRATE"
    )

    def get_supported_tables(self) -> list[str]:
        return list(self._SUPPORTED_TABLE_MAP.keys())

    def get_table_category(self, table_id: str) -> str:
        if table_id not in self._SUPPORTED_TABLE_MAP:
            raise ValueError(f"Table '{table_id}' is not supported by EastMoneyProvider.")
        return self._SUPPORTED_TABLE_MAP[table_id]

    def get_all_symbols(self, table_id: str) -> list[str]:
        """发现全量代码。市场宽表返回 ['_ALL_']，板块成分股表返回板块代码列表。"""
        if table_id not in self.get_supported_tables():
            raise ValueError(f"Table '{table_id}' is not supported by EastMoneyProvider.")

        if table_id in ("ashare.dragon_tiger.eastmoney", "ashare.inst_trade.eastmoney"):
            return ["_ALL_"]

        # 板块成分股表：逐板块拉取，返回板块代码列表
        board_type = self._detect_board_type(table_id)
        name_map = self._fetch_board_list(board_type)
        symbols = list(name_map.keys())
        logger.info(f"Discovered {len(symbols)} boards for {table_id}")
        return symbols

    def fetch(
        self, table_id: str, symbol: str, start_date: Any, end_date: Any, **kwargs
    ) -> pl.DataFrame:
        """根据 table_id 路由至具体的下载逻辑。"""
        if table_id not in self.get_supported_tables():
            raise ValueError(f"Table '{table_id}' is not supported by EastMoneyProvider.")

        # 参数标准化：转换毫秒戳为 YYYY-MM-DD
        if isinstance(start_date, int):
            start_date = ts_to_str(start_date)
        if isinstance(end_date, int):
            end_date = ts_to_str(end_date)

        if table_id == "ashare.concept.eastmoney":
            return self._fetch_board_cons_df("concept", symbol)
        elif table_id == "ashare.industry.eastmoney":
            return self._fetch_board_cons_df("industry", symbol)
        elif table_id == "ashare.dragon_tiger.eastmoney":
            return self._fetch_dragon_tiger(start_date, end_date, **kwargs)
        elif table_id == "ashare.inst_trade.eastmoney":
            return self._fetch_inst_trade(start_date, end_date, **kwargs)
        else:
            raise NotImplementedError(f"Table '{table_id}' not implemented.")

    # -----------------------------------------------------------------------
    # 板块相关
    # -----------------------------------------------------------------------

    def _detect_board_type(self, table_id: str) -> str:
        """从 table_id 推断板块类型 (concept / industry)。"""
        if "concept" in table_id:
            return "concept"
        elif "industry" in table_id:
            return "industry"
        raise ValueError(f"Cannot detect board type from {table_id}")

    def _board_fs(self, board_type: str) -> str:
        """板块列表的 fs 参数。"""
        if board_type == "concept":
            return "m:90 t:3 f:!50"
        elif board_type == "industry":
            return "m:90 t:2 f:!50"
        raise ValueError(f"Unknown board type: {board_type}")

    def _fetch_board_list(self, board_type: str) -> dict[str, str]:
        """从 push2 接口拉取板块列表，返回 {"board_code": "board_name"}。"""
        if board_type in self._board_name_cache:
            return self._board_name_cache[board_type]

        fs = self._board_fs(board_type)
        params = {
            "pn": "1", "pz": "100", "po": "1", "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2", "invt": "2", "fid": "f12",
            "fs": fs,
            "fields": self._BOARD_FIELDS,
        }

        r = em_push2(params=params, timeout=15)
        data_json = r.json()
        diff = data_json["data"]["diff"] or []
        total = data_json["data"]["total"]
        if not diff:
            return []

        per_page = len(diff)
        total_page = math.ceil(total / per_page)
        all_rows = list(diff)

        for page in range(2, total_page + 1):
            params["pn"] = str(page)
            r = em_push2(params=params, timeout=15)
            data_json = r.json()
            all_rows.extend(data_json["data"]["diff"] or [])

        result = {row[self._BOARD_FIELD_MAP["f12"]]: row[self._BOARD_FIELD_MAP["f14"]] for row in all_rows}
        self._board_name_cache[board_type] = result
        return result

    def _fetch_board_cons(self, board_type: str, board_code: str) -> list[dict]:
        """从 push2 接口拉取指定板块的成分股。"""
        fs = f"b:{board_code} f:!50"
        params = {
            "pn": "1", "pz": "100", "po": "1", "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2", "invt": "2", "fid": "f12",
            "fs": fs,
            "fields": self._CONS_FIELDS,
        }

        r = em_push2(params=params, timeout=15)
        data_json = r.json()
        diff = data_json["data"]["diff"] or []
        total = data_json["data"]["total"]
        if not diff:
            return []

        per_page = len(diff)
        total_page = math.ceil(total / per_page)
        all_rows = list(diff)

        for page in range(2, total_page + 1):
            params["pn"] = str(page)
            r = em_push2(params=params, timeout=15)
            data_json = r.json()
            all_rows.extend(data_json["data"]["diff"] or [])

        return [
            {self._CONS_FIELD_MAP[k]: v for k, v in row.items() if k in self._CONS_FIELD_MAP}
            for row in all_rows
        ]

    def _fetch_board_cons_df(self, board_type: str, board_code: str) -> pl.DataFrame:
        """拉取板块成分股，返回标准化 Polars DataFrame。"""
        cons = self._fetch_board_cons(board_type, board_code)
        if not cons:
            return self._empty_cons_df()

        df = pl.DataFrame(cons)
        # 从缓存查 board_name
        name_map = self._fetch_board_list(board_type)
        board_name = name_map.get(board_code, "")
        # 添加 board_code 和 board_name 列
        df = df.with_columns([
            pl.lit(board_code).alias("board_code"),
            pl.lit(board_name).alias("board_name"),
        ])
        # 添加展示时间列
        now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000+08:00")
        df = df.with_columns([
            pl.lit(now_str).alias("datetime"),
            pl.lit(int(datetime.now().timestamp() * 1000)).alias("timestamp"),
        ])
        # 核心列置前
        cols = list(df.columns)
        for col in ["symbol", "datetime", "timestamp"]:
            if col in cols:
                cols.remove(col)
                cols.insert(0, col)
        return df.select(cols)

    def _empty_cons_df(self) -> pl.DataFrame:
        return pl.DataFrame(schema={
            "symbol": pl.String, "stock_name": pl.String, "board_code": pl.String, "board_name": pl.String,
            "datetime": pl.String, "timestamp": pl.Int64,
        })

    # -----------------------------------------------------------------------
    # 龙虎榜
    # -----------------------------------------------------------------------

    def _fetch_dragon_tiger(
        self, start_date: str, end_date: str, **kwargs
    ) -> pl.DataFrame:
        """批量拉取龙虎榜数据，返回标准化 Polars DataFrame。"""
        code = kwargs.get("code")

        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if start_date is None:
            start_date = "2000-01-01"

        code_filter = ""
        if code:
            code_filter = f'(SECURITY_CODE="{self._clean_code(code)}")'

        all_rows = self._fetch_datacenter_paginated(
            "RPT_DAILYBILLBOARD_DETAILSNEW",
            self._LHB_COLUMNS,
            start_date, end_date, code_filter,
            sort_columns="SECURITY_CODE,TRADE_DATE",
        )

        if not all_rows:
            return self._empty_lhb_df()

        df = pl.DataFrame(all_rows)

        # 日期格式化
        if "TRADE_DATE" in df.columns:
            df = df.with_columns(
                pl.col("TRADE_DATE").str.slice(0, 10).alias("TRADE_DATE")
            )

        # 数值类型转换
        num_cols = [
            "BILLBOARD_NET_AMT", "BILLBOARD_BUY_AMT", "BILLBOARD_SELL_AMT",
            "BILLBOARD_DEAL_AMT", "ACCUM_AMOUNT", "FREE_MARKET_CAP",
            "D1_CLOSE_ADJCHRATE", "D2_CLOSE_ADJCHRATE", "D5_CLOSE_ADJCHRATE",
            "D10_CLOSE_ADJCHRATE", "CHANGE_RATE", "TURNOVERRATE",
            "DEAL_NET_RATIO", "DEAL_AMOUNT_RATIO", "CLOSE_PRICE",
        ]
        for col in num_cols:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))

        # 重命名
        rename_map = {
            "SECURITY_CODE": "symbol",
            "SECURITY_NAME_ABBR": "stock_name",
            "TRADE_DATE": "trade_date",
            "EXPLAIN": "explain",
            "CLOSE_PRICE": "close_price",
            "CHANGE_RATE": "change_pct",
            "BILLBOARD_NET_AMT": "lhb_net_amt",
            "BILLBOARD_BUY_AMT": "lhb_buy_amt",
            "BILLBOARD_SELL_AMT": "lhb_sell_amt",
            "BILLBOARD_DEAL_AMT": "lhb_deal_amt",
            "ACCUM_AMOUNT": "market_amount",
            "DEAL_NET_RATIO": "net_ratio",
            "DEAL_AMOUNT_RATIO": "deal_ratio",
            "TURNOVERRATE": "turnover_rate",
            "FREE_MARKET_CAP": "float_mcap",
            "EXPLANATION": "reason",
            "D1_CLOSE_ADJCHRATE": "d1_pct",
            "D2_CLOSE_ADJCHRATE": "d2_pct",
            "D5_CLOSE_ADJCHRATE": "d5_pct",
            "D10_CLOSE_ADJCHRATE": "d10_pct",
        }
        actual_rename = {k: v for k, v in rename_map.items() if k in df.columns}
        df = df.rename(actual_rename)

        # 标准化时间轴
        if "trade_date" in df.columns:
            df = DataCleaner.standardize(
                df, "trade_date", time_fmt="%Y-%m-%d",
                source_tz="Asia/Shanghai", display_tz="Asia/Shanghai",
                time_shift_hours=15,
            )

        return df

    def _empty_lhb_df(self) -> pl.DataFrame:
        return pl.DataFrame(schema={
            "symbol": pl.String, "stock_name": pl.String,
            "datetime": pl.String, "timestamp": pl.Int64,
        })

    # -----------------------------------------------------------------------
    # 机构交易
    # -----------------------------------------------------------------------

    def _fetch_inst_trade(
        self, start_date: str, end_date: str, **kwargs
    ) -> pl.DataFrame:
        """批量拉取机构买卖每日统计，返回标准化 Polars DataFrame。"""
        code = kwargs.get("code")

        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if start_date is None:
            start_date = "2000-01-01"

        code_filter = ""
        if code:
            code_filter = f'(SECURITY_CODE="{self._clean_code(code)}")'

        all_rows = self._fetch_datacenter_paginated(
            "RPT_ORGANIZATION_TRADE_DETAILSNEW",
            self._INST_COLUMNS,
            start_date, end_date, code_filter,
            sort_columns="TRADE_DATE,SECURITY_CODE",
        )

        if not all_rows:
            return self._empty_inst_df()

        df = pl.DataFrame(all_rows)

        # 日期格式化
        if "TRADE_DATE" in df.columns:
            df = df.with_columns(
                pl.col("TRADE_DATE").str.slice(0, 10).alias("TRADE_DATE")
            )

        # 数值类型转换
        num_cols = [
            "CLOSE_PRICE", "CHANGE_RATE", "BUY_TIMES", "SELL_TIMES",
            "BUY_AMT", "SELL_AMT", "NET_BUY_AMT", "ACCUM_AMOUNT",
            "RATIO", "TURNOVERRATE", "FREECAP",
            "D1_CLOSE_ADJCHRATE", "D2_CLOSE_ADJCHRATE", "D3_CLOSE_ADJCHRATE",
            "D5_CLOSE_ADJCHRATE", "D10_CLOSE_ADJCHRATE",
        ]
        for col in num_cols:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))

        # 重命名
        rename_map = {
            "SECURITY_CODE": "symbol",
            "SECURITY_NAME_ABBR": "stock_name",
            "CLOSE_PRICE": "close_price",
            "CHANGE_RATE": "change_pct",
            "BUY_TIMES": "buy_times",
            "SELL_TIMES": "sell_times",
            "BUY_AMT": "inst_buy_amt",
            "SELL_AMT": "inst_sell_amt",
            "NET_BUY_AMT": "inst_net_amt",
            "ACCUM_AMOUNT": "market_amount",
            "RATIO": "inst_net_ratio",
            "TURNOVERRATE": "turnover_rate",
            "FREECAP": "float_mcap",
            "EXPLANATION": "reason",
            "TRADE_DATE": "trade_date",
            "D1_CLOSE_ADJCHRATE": "d1_pct",
            "D2_CLOSE_ADJCHRATE": "d2_pct",
            "D3_CLOSE_ADJCHRATE": "d3_pct",
            "D5_CLOSE_ADJCHRATE": "d5_pct",
            "D10_CLOSE_ADJCHRATE": "d10_pct",
        }
        actual_rename = {k: v for k, v in rename_map.items() if k in df.columns}
        df = df.rename(actual_rename)

        # 标准化时间轴
        if "trade_date" in df.columns:
            df = DataCleaner.standardize(
                df, "trade_date", time_fmt="%Y-%m-%d",
                source_tz="Asia/Shanghai", display_tz="Asia/Shanghai",
                time_shift_hours=15,
            )

        return df

    def _empty_inst_df(self) -> pl.DataFrame:
        return pl.DataFrame(schema={
            "symbol": pl.String, "stock_name": pl.String,
            "datetime": pl.String, "timestamp": pl.Int64,
        })

    # -----------------------------------------------------------------------
    # datacenter 通用分页拉取
    # -----------------------------------------------------------------------

    def _fetch_datacenter_paginated(
        self,
        report_name: str,
        columns: str,
        start_date: str,
        end_date: str,
        code_filter: str,
        sort_columns: str,
        page_size: int = 500,
    ) -> list[dict]:
        """按月分批 + 自动分页拉取 datacenter 数据。"""
        from datetime import datetime as dt
        import calendar

        start_dt = dt.strptime(start_date, "%Y-%m-%d")
        end_dt = dt.strptime(end_date, "%Y-%m-%d")

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

            logger.debug(f"  [{month_start} ~ {month_end}] ", end="")

            month_rows = self._fetch_datacenter_all_pages(
                report_name, columns, filter_str, sort_columns, page_size
            )
            all_rows.extend(month_rows)
            logger.debug(f"got {len(month_rows)} rows (cumulative: {len(all_rows)})")

            if cur.month == 12:
                cur = cur.replace(year=cur.year + 1, month=1)
            else:
                cur = cur.replace(month=cur.month + 1)

        return all_rows

    def _fetch_datacenter_all_pages(
        self,
        report_name: str,
        columns: str,
        filter_str: str,
        sort_columns: str,
        page_size: int,
    ) -> list[dict]:
        """自动分页拉取，直到拿完所有数据。"""
        all_rows: list[dict] = []
        page = 1

        while True:
            resp = em_datacenter(
                report_name=report_name,
                columns=columns,
                filter_str=filter_str,
                page_number=page,
                page_size=page_size,
                sort_columns=sort_columns,
                sort_types="1,-1",
            )
            data = resp["data"]
            total = resp["count"]

            if not data:
                break

            all_rows.extend(data)
            if len(all_rows) >= total:
                break
            page += 1

        return all_rows

    # -----------------------------------------------------------------------
    # 工具方法
    # -----------------------------------------------------------------------

    @staticmethod
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
