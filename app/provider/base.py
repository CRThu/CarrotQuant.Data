import abc
import polars as pl

class BaseProvider(abc.ABC):
    """
    数据源驱动基类，定义统一的下载接口
    """

    @abc.abstractmethod
    def fetch(self, table_id: str, symbol: str, start_date: str, end_date: str, **kwargs) -> pl.DataFrame:
        """
        原子化下载接口，一次仅处理单支证券
        
        Args:
            table_id: 表标识符 (例如 ashare.kline.1d.adj.baostock)
            symbol: 证券代码 (例如 sh.600000)
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            **kwargs: 其他源特有的参数
            
        Returns:
            pl.DataFrame: 标准化后的数据，核心包含 timestamp, datetime 列
        """
        pass
