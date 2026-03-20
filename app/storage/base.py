import abc
import polars as pl

class StorageManager(abc.ABC):
    """
    存储管理器抽象基类，定义统一的存储层接口。
    """

    def __init__(self, category: str = "TS"):
        self.category = category

    @abc.abstractmethod
    def read(self, table_id: str, symbol: str, year: int) -> pl.DataFrame:
        """
        读取特定表、代码及年份的数据。
        
        Args:
            table_id: 表 ID，格式为 {market}.{freq}.{adj}.{source}
            symbol: 证券代码
            year: 年份
            
        Returns:
            pl.DataFrame: 读取到的数据
        """
        pass

    @abc.abstractmethod
    def write(self, table_id: str, df: pl.DataFrame, mode: str = "append"):
        """
        写入数据。根据 mode 参数执行覆盖或增量写入。
        
        Args:
            table_id: 表 ID
            df: 包含 symbol 和 timestamp 等字段的 DataFrame
            mode: 写入模式，支持 "overwrite" (覆盖) 或 "append" (增量)
        """
        pass

    @abc.abstractmethod
    def get_all_symbols(self, table_id: str) -> list[str]:
        """
        返回该数据集下所有的证券代码。
        """
        pass

    @abc.abstractmethod
    def get_total_bars(self, table_id: str) -> int:
        """
        返回该数据集的物理总行数。
        """
        pass

    @abc.abstractmethod
    def get_global_time_range(self, table_id: str) -> tuple[int, int]:
        """
        返回该数据集全局最小/最大毫秒戳。
        """
        pass

    @abc.abstractmethod
    def get_unique_timestamps(self, table_id: str) -> list[int]:
        """
        返回该数据集去重后的所有时间点（可选，性能敏感）。
        """
        pass
