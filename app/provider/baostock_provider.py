import baostock as bs
import polars as pl
from typing import Any
from loguru import logger
from app.provider.base import BaseProvider
from app.provider.data_cleaner import DataCleaner
from app.utils.time_utils import ts_to_str

class BaostockProvider(BaseProvider):
    """
    Baostock 数据源驱动实现
    """

    def __init__(self):
        """
        初始化并登录 Baostock
        """
        self.lg = bs.login()
        if self.lg.error_code != '0':
            logger.error(f"Baostock login failed: {self.lg.error_msg}")
        else:
            logger.info("Baostock login success")

    def __del__(self):
        """
        析构时退出 Baostock
        """
        try:
            bs.logout()
            logger.info("Baostock logout success")
        except Exception as e:
            logger.warning(f"Baostock logout error: {e}")

    def get_supported_tables(self) -> list[str]:
        """
        返回 Baostock 支持的所有 table_id 列表。
        """
        return [
            "ashare.kline.1d.adj.baostock",
            "ashare.kline.1d.raw.baostock",
            "ashare.kline.5m.adj.baostock",
            "ashare.kline.5m.raw.baostock",
            "aindex.kline.1d.raw.baostock"
        ]

    def get_all_symbols(self, table_id: str) -> list[str]:
        """
        全量证券列表发现逻辑（基础信息库）。使用 query_stock_basic 获取全市场（含退市）的所有证券代码。
        
        解析 table_id 获取第一个分段 prefix:
        - 若 prefix == "ashare"：过滤 type == "1"（个股），确保包含退市股。
        - 若 prefix == "aindex"：过滤 type == "2"（指数）。
        - 其他：抛出 ValueError。
        """
        # 1. 预校验支持库
        if table_id not in self.get_supported_tables():
            raise ValueError(f"Table '{table_id}' is not supported by BaostockProvider.")
            
        # 解析 table_id 获取第一个分段 prefix
        prefix = table_id.split('.')[0]
        
        rs = bs.query_stock_basic()
        
        if rs.error_code != '0':
            raise ValueError(f"Baostock discovery (basic) failed: {rs.error_msg}")
            
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
            
        if not data_list:
            raise ValueError(f"Baostock discovery (basic) returned empty list for table: {table_id}")
            
        # 根据返回列 ['code', 'code_name', 'ipoDate', 'outDate', 'type', 'status']
        # 提取 code (row[0])，根据 prefix 过滤 type (row[4])
        if prefix == "ashare":
            symbols = [row[0] for row in data_list if row[4] == "1"]
            logger.info(f"Discovered {len(symbols)} symbols for Universe 'ashare' (Stocks) from Baostock")
        elif prefix == "aindex":
            symbols = [row[0] for row in data_list if row[4] == "2"]
            logger.info(f"Discovered {len(symbols)} symbols for Universe 'aindex' (Indices) from Baostock")
        else:
            raise ValueError(f"Unsupported Universe prefix: {prefix}. Only 'ashare' and 'aindex' are supported.")
        
        return symbols

    def fetch(self, table_id: str, symbol: str, start_date: Any, end_date: Any, **kwargs) -> pl.DataFrame:
        """
        根据 table_id 路由至具体的下载逻辑。支持传入毫秒戳或日期字符串。
        """
        # 0. 预校验支持库
        supported = self.get_supported_tables()
        if table_id not in supported:
            # 特殊处理：如果是一个包含 baostock 后缀但不明确支持的表，记录警告
            if table_id.endswith('.baostock'):
                logger.warning(f"Table '{table_id}' is explicitly handled by Baostock but not in supported_tables list.")
            else:
                raise ValueError(f"Table '{table_id}' is not supported by BaostockProvider.")

        # 1. 参数标准化：转换毫秒戳为 YYYY-MM-DD
        if isinstance(start_date, int):
            start_date = ts_to_str(start_date)
        if isinstance(end_date, int):
            end_date = ts_to_str(end_date)

        # 2. 路由逻辑：解析 table_id 中间的部分 (如 kline)
        parts = table_id.split('.')
        if 'kline' in parts:
            return self._fetch_kline(table_id, symbol, start_date, end_date, **kwargs)
        else:
            raise NotImplementedError(f"Table category not supported by Baostock: {table_id}")

    def _fetch_kline(self, table_id: str, symbol: str, start_date: str, end_date: str, **kwargs) -> pl.DataFrame:
        """
        私有方法：下载 K 线数据
        """
        # 解析频率和复权
        # table_id 格式示例: ashare.kline.1d.adj.baostock
        parts = table_id.split('.')
        prefix = parts[0]
        is_index = (prefix == "aindex")
        
        freq_raw = parts[2] if len(parts) > 2 else '1d'
        adj_raw = parts[3] if len(parts) > 3 else 'raw'
        
        # 映射频率
        freq_map = {'1d': 'd', '5m': '5'}
        freq = freq_map.get(freq_raw, 'd')
        is_day = (freq == 'd')
        
        # 映射复权
        adj_map = {'raw': '3', 'adj': '1'}
        adj = adj_map.get(adj_raw, '3')
        
        # 指数数据通常没有复权，强制使用不复权 (raw)
        if is_index:
            # 1. 校验复权：指数仅支持 raw，显式拦截其他请求
            if adj_raw != 'raw':
                logger.warning(f"Baostock indices only support 'raw' (unadjusted) data, but '{adj_raw}' was requested for {symbol}. Returning empty standardized DF.")
                # 指数不支持分钟线，所以 fields 必定是 day_fields (包含 date)
                df_empty = pl.DataFrame(None, schema={f: pl.Utf8 for f in fields.split(',')})
                return DataCleaner.standardize(df_empty, "date", time_fmt="%Y-%m-%d")
            
            # 2. 校验频率：指数仅支持日线，分钟数据极其不完整且不受官方正式支持，统一拦截
            if freq != 'd':
                logger.warning(f"Baostock doesn't support reliable minute kline for indices: {symbol}. Returning empty standardized DF.")
                df_empty = pl.DataFrame(None, schema={f: pl.Utf8 for f in fields.split(',')})
                return DataCleaner.standardize(df_empty, "date", time_fmt="%Y-%m-%d")
            
            adj = "3"

        # Baostock K 线字段定义
        if is_index:
            # 指数 K 线字段 (日线) - 剔除返回 0 的无用字段
            #day_fields = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg"
            day_fields = "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg"
            fields = day_fields
        else:
            # 个股 K 线字段 (日线, 五分钟)
            day_fields = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"
            min_fields = "date,time,code,open,high,low,close,volume,amount,adjustflag"
            fields = day_fields if is_day else min_fields
        
        logger.debug(f"Fetching {symbol} ({prefix}) kline from Baostock: {start_date} to {end_date} (freq={freq}, adj={adj})")
        
        rs = bs.query_history_k_data_plus(
            symbol, fields,
            start_date=start_date, end_date=end_date,
            frequency=freq, adjustflag=adj
        )
        
        if rs.error_code != '0':
            logger.error(f"Baostock fetch error: {rs.error_msg}")
            # 出错时也返回标准化的空表
            df_empty = pl.DataFrame(None, schema={f: pl.Utf8 for f in fields.split(',')})
            if is_day:
                return DataCleaner.standardize(df_empty, "date", time_fmt="%Y-%m-%d")
            else:
                return DataCleaner.standardize(df_empty, "time", time_fmt="%Y%m%d%H%M%S%3f")

        # 转换为 Polars DataFrame
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
            
        df = pl.DataFrame(data_list, schema={f: pl.Utf8 for f in fields.split(',')}, orient="row")
        
        # 字段清洗与重命名
        rename_map = {
            "code": "symbol",
            "pctChg": "change_pct",
            "turn": "turnover_rate",
            "tradestatus": "trade_status",
            "isST": "is_st",
            "peTTM": "pe_ttm",
            "pbMRQ": "pb_mrq",
            "psTTM": "ps_ttm",
            "pcfNcfTTM": "pcf_ncf_ttm"
        }
        # 只重命名存在的列
        actual_rename = {k: v for k, v in rename_map.items() if k in df.columns}
        df = df.rename(actual_rename)
        
        # 处理 adjustflag 映射 (1:adj, 2:qfq, 3:raw)
        if "adjustflag" in df.columns:
            # 强制拦截前复权
            if (df["adjustflag"] == "2").any():
                raise ValueError("Detect Forward Adjustment (qfq) data, which is FORBIDDEN in this system.")
            
            df = df.with_columns(
                pl.col("adjustflag").replace_strict(
                    {"1": "adj", "3": "raw"}, default="unknown"
                ).alias("adjust_flag")
            ).drop("adjustflag")

        # 转换为数值类型（Baostock 返回的全是字符串）
        numeric_cols = [
            "open", "high", "low", "close", "preclose", "volume", "amount", 
            "change_pct", "turnover_rate", "pe_ttm", "pb_mrq", "ps_ttm", "pcf_ncf_ttm"
        ]
        for col in numeric_cols:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))

        # 数据清洗与标准化
        if is_day:
            return DataCleaner.standardize(df, "date", time_fmt="%Y-%m-%d")
        else:
            # Baostock 分钟线 time 格式: YYYYMMDDHHMMSSsss (无小数点)
            # 此时 date 列是多余的，一并删除
            if "date" in df.columns:
                df = df.drop("date")
            return DataCleaner.standardize(df, "time", time_fmt="%Y%m%d%H%M%S%3f")

