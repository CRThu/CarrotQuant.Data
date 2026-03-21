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

    def get_all_symbols(self, table_id: str) -> list[str]:
        """
        全量证券列表发现逻辑（基础信息库）。使用 query_stock_basic 获取全市场（含退市）的所有证券代码。
        """
        rs = bs.query_stock_basic()
        
        if rs.error_code != '0':
            raise ValueError(f"Baostock discovery (basic) failed: {rs.error_msg}")
            
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
            
        if not data_list:
            raise ValueError(f"Baostock discovery (basic) returned empty list for table: {table_id}")
            
        # 根据返回列 ['code', 'code_name', 'ipoDate', 'outDate', 'type', 'status']
        # 提取第一列 code，不进行任何过滤，尽可能全面
        symbols = [row[0] for row in data_list]
        
        logger.info(f"Discovered {len(symbols)} symbols from Baostock (Basic Info Library)")
        return symbols

    def fetch(self, table_id: str, symbol: str, start_date: Any, end_date: Any, **kwargs) -> pl.DataFrame:
        """
        根据 table_id 路由至具体的下载逻辑。支持传入毫秒戳或日期字符串。
        """
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
        freq_raw = parts[2] if len(parts) > 2 else '1d'
        adj_raw = parts[3] if len(parts) > 3 else 'raw'
        
        # 映射频率
        freq_map = {'1d': 'd', '5m': '5'}
        freq = freq_map.get(freq_raw, 'd')
        
        # 映射复权
        adj_map = {'raw': '3', 'adj': '1'}
        adj = adj_map.get(adj_raw, '3')

        # Baostock K 线字段定义
        day_fields = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"
        min_fields = "date,time,code,open,high,low,close,volume,amount,adjustflag"
        
        is_day = freq in ['d']
        fields = day_fields if is_day else min_fields
        
        logger.debug(f"Fetching {symbol} kline from Baostock: {start_date} to {end_date} (freq={freq}, adj={adj})")
        
        rs = bs.query_history_k_data_plus(
            symbol, fields,
            start_date=start_date, end_date=end_date,
            frequency=freq, adjustflag=adj
        )
        
        if rs.error_code != '0':
            logger.error(f"Baostock fetch error: {rs.error_msg}")
            return pl.DataFrame()

        # 转换为 Polars DataFrame
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        if not data_list:
            return pl.DataFrame()
            
        df = pl.DataFrame(data_list, schema=fields.split(','), orient="row")
        
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
                pl.col("adjustflag").map_elements(
                    lambda x: "adj" if x == "1" else ("raw" if x == "3" else "unknown"),
                    return_dtype=pl.Utf8
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
