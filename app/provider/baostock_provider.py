import baostock as bs
import polars as pl
from loguru import logger
from app.provider.base import BaseProvider
from app.storage.utils import TimeStandardizer

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

    def fetch(self, table_id: str, symbol: str, start_date: str, end_date: str, **kwargs) -> pl.DataFrame:
        """
        根据 table_id 路由至具体的下载逻辑
        """
        # 路由逻辑：解析 table_id 中间的部分 (如 kline)
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
            "pctChg": "change_pct",
            "amount": "turnover",
            "turn": "turnover_rate",
            "tradestatus": "trade_status",
            "isST": "is_st"
        }
        # 只重命名存在的列
        actual_rename = {k: v for k, v in rename_map.items() if k in df.columns}
        df = df.rename(actual_rename)
        
        # 删除源特有列 'code'
        if 'code' in df.columns:
            df = df.drop("code")

        # 转换为数值类型（Baostock 返回的全是字符串）
        numeric_cols = ["open", "high", "low", "close", "preclose", "volume", "turnover", "change_pct", "turnover_rate"]
        for col in numeric_cols:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))

        # 时间标准化
        return TimeStandardizer.standardize(df, "date")
