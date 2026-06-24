"""东财数据源驱动实现。

对接接口:
  - push2:       https://push2.eastmoney.com (板块列表/成分股)
  - datacenter:  https://datacenter-web.eastmoney.com/api/data/v1/get (龙虎榜/机构交易)

支持以下 table_id:
  - ashare.concept.eastmoney       (EV) 概念板块成分股
  - ashare.industry.eastmoney      (EV) 行业板块成分股
  - ashare.dragon_tiger.eastmoney  (EV) 龙虎榜
  - ashare.inst_trade.eastmoney    (EV) 机构买卖每日统计

akshare 同款接口:
  - 板块列表: stock_board_concept_name_em / stock_board_industry_name_em
  - 板块成分股: stock_board_concept_cons_em / stock_board_industry_cons_em
  - 龙虎榜: stock_lhb_detail_em
  - 机构交易: stock_jgdy_tj_em

目标地址:
  - 板块: https://quote.eastmoney.com/center/boardlist.html#concept_board
  - 龙虎榜: https://data.eastmoney.com/stock/lhb.html
  - 机构交易: https://data.eastmoney.com/stock/jgmy.html

浏览器测试:
  - push2: https://push2.eastmoney.com/webguest/api/qt/clist/get?np=1&fltt=1&invt=2&fs=m:90+t:3+f:!50&fields=f12,f13,f14&fid=f3&pn=1&pz=5&po=1&dect=1&ut=fa5fd1943c7b386f172d6893dbfba10b
  - datacenter: https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SECURITY_CODE,TRADE_DATE&sortTypes=1,-1&pageSize=50&pageNumber=1&reportName=RPT_DAILYBILLBOARD_DETAILSNEW&columns=SECURITY_CODE,SECUCODE,SECURITY_NAME_ABBR,TRADE_DATE,EXPLAIN,CLOSE_PRICE,CHANGE_RATE,BILLBOARD_NET_AMT,BILLBOARD_BUY_AMT,BILLBOARD_SELL_AMT,BILLBOARD_DEAL_AMT,ACCUM_AMOUNT,DEAL_NET_RATIO,DEAL_AMOUNT_RATIO,TURNOVERRATE,FREE_MARKET_CAP,EXPLANATION,D1_CLOSE_ADJCHRATE,D2_CLOSE_ADJCHRATE,D5_CLOSE_ADJCHRATE,D10_CLOSE_ADJCHRATE,SECURITY_TYPE_CODE&source=WEB&client=WEB&filter=(TRADE_DATE<='2026-06-18')(TRADE_DATE>='2026-01-01')
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
    # fs 参数: m:90 t:3 f:!50 = 概念板块; m:90 t:2 f:!50 = 行业板块
    _BOARD_FIELDS = (
        "f12,"     # 板块代码
        "f14"      # 板块名称
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
    )
    _BOARD_FIELD_MAP = {"f12": "board_code", "f14": "board_name"}

    # 板块列表缓存: {board_type: {"board_code": "board_name"}}
    _board_name_cache: dict[str, dict[str, str]] = {}

    # push2 成分股字段
    # fs 参数: b:{board_code} f:!50 = 板块成分股
    _CONS_FIELDS = (
        "f12,"     # 股票代码
        "f14"      # 股票名称
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
    _CONS_FIELD_MAP = {"f12": "symbol", "f14": "stock_name"}

    # 龙虎榜字段映射
    _LHB_FIELD_MAP = {
        "SECURITY_CODE": "symbol",              # 股票代码
        "SECURITY_NAME_ABBR": "stock_name",     # 股票简称
        "TRADE_DATE": "trade_date",             # 上榜日期
        "EXPLAIN": "explain",                   # 解读 (买一主买等)
        "CLOSE_PRICE": "close_price",           # 收盘价
        "CHANGE_RATE": "change_rate",           # 涨跌幅
        "BILLBOARD_NET_AMT": "net_amount",      # 龙虎榜净买额
        "BILLBOARD_BUY_AMT": "buy_amount",      # 龙虎榜买入额
        "BILLBOARD_SELL_AMT": "sell_amount",    # 龙虎榜卖出额
        "BILLBOARD_DEAL_AMT": "deal_amount",    # 龙虎榜成交额
        "ACCUM_AMOUNT": "market_amount",        # 市场总成交额
        "DEAL_NET_RATIO": "deal_net_ratio",     # 净买额占总成交比
        "DEAL_AMOUNT_RATIO": "deal_amount_ratio",  # 成交额占总成交比
        "TURNOVERRATE": "turnover_rate",        # 换手率
        "FREE_MARKET_CAP": "float_market_cap",  # 流通市值
        "EXPLANATION": "explanation",           # 上榜原因
        "D1_CLOSE_ADJCHRATE": "day1_change_rate",   # 上榜后1日涨跌幅
        "D2_CLOSE_ADJCHRATE": "day2_change_rate",   # 上榜后2日涨跌幅
        "D5_CLOSE_ADJCHRATE": "day5_change_rate",   # 上榜后5日涨跌幅
        "D10_CLOSE_ADJCHRATE": "day10_change_rate", # 上榜后10日涨跌幅
    }

    # 机构交易字段映射
    _INST_FIELD_MAP = {
        "SECURITY_CODE": "symbol",              # 股票代码
        "SECURITY_NAME_ABBR": "stock_name",     # 股票简称
        "CLOSE_PRICE": "close_price",           # 收盘价
        "CHANGE_RATE": "change_rate",           # 涨跌幅
        "BUY_TIMES": "buy_times",               # 买方机构数
        "SELL_TIMES": "sell_times",             # 卖方机构数
        "BUY_AMT": "buy_amount",                # 机构买入总额
        "SELL_AMT": "sell_amount",              # 机构卖出总额
        "NET_BUY_AMT": "net_buy_amount",        # 机构买入净额
        "ACCUM_AMOUNT": "market_amount",        # 市场总成交额
        "RATIO": "ratio",                       # 机构净买额占总成交额比
        "TURNOVERRATE": "turnover_rate",        # 换手率
        "FREECAP": "float_market_cap",          # 流通市值
        "EXPLANATION": "explanation",           # 上榜原因
        "TRADE_DATE": "trade_date",             # 上榜日期
        "D1_CLOSE_ADJCHRATE": "day1_change_rate",   # 上榜后1日涨跌幅
        "D2_CLOSE_ADJCHRATE": "day2_change_rate",   # 上榜后2日涨跌幅
        "D3_CLOSE_ADJCHRATE": "day3_change_rate",   # 上榜后3日涨跌幅
        "D5_CLOSE_ADJCHRATE": "day5_change_rate",   # 上榜后5日涨跌幅
        "D10_CLOSE_ADJCHRATE": "day10_change_rate", # 上榜后10日涨跌幅
    }

    # datacenter 龙虎榜字段
    # 报表: RPT_DAILYBILLBOARD_DETAILSNEW
    # 目标地址: https://data.eastmoney.com/stock/lhb.html
    # 参考接口: https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=SECURITY_CODE,TRADE_DATE&sortTypes=1,-1&pageSize=50&pageNumber=1&reportName=RPT_DAILYBILLBOARD_DETAILSNEW&columns=SECURITY_CODE,SECUCODE,SECURITY_NAME_ABBR,TRADE_DATE,EXPLAIN,CLOSE_PRICE,CHANGE_RATE,BILLBOARD_NET_AMT,BILLBOARD_BUY_AMT,BILLBOARD_SELL_AMT,BILLBOARD_DEAL_AMT,ACCUM_AMOUNT,DEAL_NET_RATIO,DEAL_AMOUNT_RATIO,TURNOVERRATE,FREE_MARKET_CAP,EXPLANATION,D1_CLOSE_ADJCHRATE,D2_CLOSE_ADJCHRATE,D5_CLOSE_ADJCHRATE,D10_CLOSE_ADJCHRATE,SECURITY_TYPE_CODE&source=WEB&client=WEB&filter=(TRADE_DATE<='2026-06-18')(TRADE_DATE>='2026-01-01')
    _LHB_COLUMNS = (
        "ACCUM_AMOUNT,"        # akshare | 市场总成交额
        "BILLBOARD_BUY_AMT,"   # akshare | 龙虎榜买入额
        "BILLBOARD_DEAL_AMT,"  # akshare | 龙虎榜成交额
        "BILLBOARD_NET_AMT,"   # akshare | 龙虎榜净买额
        "BILLBOARD_SELL_AMT,"  # akshare | 龙虎榜卖出额
        "CHANGE_RATE,"         # akshare | 涨跌幅
        "CLOSE_PRICE,"         # akshare | 收盘价
        "D1_CLOSE_ADJCHRATE,"  # akshare | 上榜后1日涨跌幅
        "D2_CLOSE_ADJCHRATE,"  # akshare | 上榜后2日涨跌幅
        "D5_CLOSE_ADJCHRATE,"  # akshare | 上榜后5日涨跌幅
        "D10_CLOSE_ADJCHRATE," # akshare | 上榜后10日涨跌幅
        "DEAL_AMOUNT_RATIO,"   # akshare | 成交额占总成交比
        "DEAL_NET_RATIO,"      # akshare | 净买额占总成交比
        "EXPLAIN,"             # akshare | 解读(买一主买等)
        "EXPLANATION,"         # akshare | 上榜原因
        "FREE_MARKET_CAP,"     # akshare | 流通市值
        "SECURITY_CODE,"       # akshare | 股票代码
        "SECURITY_NAME_ABBR,"  # akshare | 股票简称
        "TRADE_DATE,"          # akshare | 上榜日期
        "TURNOVERRATE"         # akshare | 换手率
        # 以下无用字段已剔除:
        # SECUCODE,             # 无用 | akshare请求了但rename时丢弃
        # SECURITY_INNER_CODE,  # 无用 | 内部代码无意义
        # SECURITY_TYPE_CODE,   # 无用 | 全为同一值 058001001
        # MARKET,               # 无用 | SH/SZ/BJ，SECURITY_CODE已含此信息
        # TRADE_MARKET_CODE,    # 无用 | 与TRADE_MARKET重复
        # TRADE_MARKET,         # 无用 | 深交所主板等，冗余分类
        # BUY_RATIO,            # 无用 | 无法推算但席位无法翻译，比例无独立分析价值
        # SELL_RATIO,           # 无用 | 同BUY_RATIO
        # BUY_SEAT,             # 无用 | 席位编码无法翻译为券商名
        # BUY_SEAT_NEW,         # 无用 | 与BUY_SEAT相同仅int→str
        # SELL_SEAT,            # 无用 | 同BUY_SEAT
        # SELL_SEAT_NEW,        # 无用 | 与SELL_SEAT相同仅int→str
        # CHANGE_TYPE,          # 无用 | 上榜类型编码无含义
        # NET_BS_AMT,           # 无用 | = 龙虎榜净买额，重复
        # SUM_BUY_AMT,          # 无用 | = 龙虎榜买入额，重复
        # SUM_SELL_AMT,         # 无用 | = 龙虎榜卖出额，重复
        # TRADE_ID,             # 无用 | 纯标识符
        # D20_CLOSE_ADJCHRATE,  # 无用 | 上榜后20日，长期参考但数据滞后
        # D30_CLOSE_ADJCHRATE,  # 无用 | 上榜后30日，同上
    )

    # datacenter 机构交易字段
    # 报表: RPT_ORGANIZATION_TRADE_DETAILSNEW
    # 目标地址: https://data.eastmoney.com/stock/jgmy.html
    # 参考接口: https://datacenter-web.eastmoney.com/api/data/v1/get?reportName=RPT_ORGANIZATION_TRADE_DETAILSNEW
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
        # 以下无用字段已剔除:
        # SECUCODE,             # 无用 | 代码全称，akshare丢弃
        # SECURITY_TYPE_CODE,   # 无用 | 全为同一值
        # MARKET,               # 无用 | 深市/沪市/北交所
        # TRADE_MARKET_CODE,    # 无用 | 交易市场代码
        # BUY_COUNT,            # 无用 | 非机构席位数，网页版无此列
        # SELL_COUNT,           # 无用 | 非机构席位数，网页版无此列
    )

    def get_supported_tables(self) -> list[str]:
        return list(self._SUPPORTED_TABLE_MAP.keys())

    def get_table_category(self, table_id: str) -> str:
        if table_id not in self._SUPPORTED_TABLE_MAP:
            raise ValueError(f"Table '{table_id}' is not supported by EastMoneyProvider.")
        return self._SUPPORTED_TABLE_MAP[table_id]

    def get_sort_keys(self, table_id: str) -> list[str]:
        """
        返回指定 table_id 的排序列列表。
        板块成分股无 timestamp，按 board_code, board_name, symbol 排序。
        龙虎榜/机构交易有 timestamp，按 timestamp, symbol 排序。
        """
        if table_id in ("ashare.concept.eastmoney", "ashare.industry.eastmoney"):
            return ["board_code", "board_name", "symbol"]
        return ["timestamp", "symbol"]

    def get_all_symbols(self, table_id: str) -> list[str]:
        """发现全量代码。市场宽表返回 ['_ALL_']，板块成分股表返回板块代码列表。"""
        if table_id not in self.get_supported_tables():
            raise ValueError(f"Table '{table_id}' is not supported by EastMoneyProvider.")

        if table_id in ("ashare.dragon_tiger.eastmoney", "ashare.inst_trade.eastmoney"):
            return ["_ALL_"]

        # 板块成分股表：逐板块拉取，返回板块代码列表
        board_type = self._detect_board_type(table_id)
        name_map = self._fetch_board_list(board_type)
        symbols = sorted(name_map.keys())
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
        """从 push2 接口拉取板块列表，返回 {"board_code": "board_name"}。

        接口: https://push2.eastmoney.com/webguest/api/qt/clist/get
        akshare 同款: stock_board_concept_name_em / stock_board_industry_name_em
        """
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
            return {}

        per_page = len(diff)
        total_page = math.ceil(total / per_page)
        all_rows = list(diff)

        for page in range(2, total_page + 1):
            params["pn"] = str(page)
            r = em_push2(params=params, timeout=15)
            data_json = r.json()
            all_rows.extend(data_json["data"]["diff"] or [])

        result = {row["f12"]: row["f14"] for row in all_rows}
        self._board_name_cache[board_type] = result
        return result

    def _fetch_board_cons(self, board_type: str, board_code: str) -> list[dict]:
        """从 push2 接口拉取指定板块的成分股。

        接口: https://push2.eastmoney.com/webguest/api/qt/clist/get
        akshare 同款: stock_board_concept_cons_em / stock_board_industry_cons_em
        fs 参数: b:{board_code} f:!50 = 板块成分股
        """
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
        """拉取板块成分股，返回标准化 Polars DataFrame。

        板块成分股为静态列表数据，无事件时间属性，不生成 timestamp/datetime 列。
        主键: [board_code, symbol]，列顺序: [board_code, board_name, symbol, stock_name]
        """
        cons = self._fetch_board_cons(board_type, board_code)
        # 创建 DataFrame，指定 schema（空数据时也保持列结构）
        cons_cols = list(self._CONS_FIELD_MAP.values())
        df = pl.DataFrame(cons, schema={c: pl.String for c in cons_cols})
        # 从缓存查 board_name
        name_map = self._fetch_board_list(board_type)
        board_name = name_map.get(board_code, "")
        # 添加 board_code 和 board_name 列
        df = df.with_columns([
            pl.lit(board_code).alias("board_code"),
            pl.lit(board_name).alias("board_name"),
        ])
        # symbol 标准化
        df = df.with_columns(
            pl.col("symbol").map_elements(self._to_standard_symbol, return_dtype=pl.String).alias("symbol")
        )
        # 主键列置前: [board_code, board_name, symbol, stock_name]
        return df.select(["board_code", "board_name", "symbol", "stock_name"])

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
            start_date = "2020-01-01"

        code_filter = ""
        if code:
            code_filter = f'(SECURITY_CODE="{self._clean_code(code)}")'

        all_rows = self._fetch_datacenter_paginated(
            "RPT_DAILYBILLBOARD_DETAILSNEW",
            self._LHB_COLUMNS,
            start_date, end_date, code_filter,
            sort_columns="SECURITY_CODE,TRADE_DATE",
            sort_types="1,-1",
        )

        # 创建 DataFrame，指定 schema（空数据时也保持列结构）
        lhb_cols = [c.strip() for c in self._LHB_COLUMNS.split(",")]
        df = pl.DataFrame(all_rows, schema={c: pl.String for c in lhb_cols})

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
        actual_rename = {k: v for k, v in self._LHB_FIELD_MAP.items() if k in df.columns}
        df = df.rename(actual_rename)

        # symbol 标准化
        if "symbol" in df.columns:
            df = df.with_columns(
                pl.col("symbol").map_elements(self._to_standard_symbol, return_dtype=pl.String)
            )

        # 标准化时间轴
        if "trade_date" in df.columns:
            df = DataCleaner.standardize(
                df, "trade_date", time_fmt="%Y-%m-%d",
                source_tz="Asia/Shanghai", display_tz="Asia/Shanghai",
                time_shift_hours=15,
            )

        return df

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
            start_date = "2020-01-01"

        code_filter = ""
        if code:
            code_filter = f'(SECURITY_CODE="{self._clean_code(code)}")'

        all_rows = self._fetch_datacenter_paginated(
            "RPT_ORGANIZATION_TRADE_DETAILSNEW",
            self._INST_COLUMNS,
            start_date, end_date, code_filter,
            sort_columns="TRADE_DATE,SECURITY_CODE",
            sort_types="1,1",
        )

        # 创建 DataFrame，指定 schema（空数据时也保持列结构）
        inst_cols = [c.strip() for c in self._INST_COLUMNS.split(",")]
        df = pl.DataFrame(all_rows, schema={c: pl.String for c in inst_cols})

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
        actual_rename = {k: v for k, v in self._INST_FIELD_MAP.items() if k in df.columns}
        df = df.rename(actual_rename)

        # symbol 标准化
        if "symbol" in df.columns:
            df = df.with_columns(
                pl.col("symbol").map_elements(self._to_standard_symbol, return_dtype=pl.String)
            )

        # 标准化时间轴
        if "trade_date" in df.columns:
            df = DataCleaner.standardize(
                df, "trade_date", time_fmt="%Y-%m-%d",
                source_tz="Asia/Shanghai", display_tz="Asia/Shanghai",
                time_shift_hours=15,
            )

        return df

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
        sort_types: str = "1,-1",
        page_size: int = 500,
    ) -> list[dict]:
        """按月分批 + 自动分页拉取 datacenter 数据。

        接口: https://datacenter-web.eastmoney.com/api/data/v1/get
        注意: API 硬限制 500 条/页，按月分批避免单次请求数据量过大。
        """
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
                report_name, columns, filter_str, sort_columns, sort_types, page_size
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
        sort_types: str,
        page_size: int,
    ) -> list[dict]:
        """自动分页拉取，直到拿完所有数据。

        API 硬限制 500 条/页，自动翻页直到数据全部获取。
        """
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
                sort_types=sort_types,
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

    @staticmethod
    def _to_standard_symbol(code: str) -> str:
        """将纯数字股票代码转换为标准格式 (sh./sz./bj. 前缀)。

        规则: 6开头 → sh, 8/4开头 → bj, 其余 → sz
        """
        code = code.strip()
        if code.startswith("6"):
            return f"sh.{code}"
        elif code.startswith("8") or code.startswith("4"):
            return f"bj.{code}"
        return f"sz.{code}"
