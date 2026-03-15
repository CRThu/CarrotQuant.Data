import abc
import polars as pl

class StorageManager(abc.ABC):
    """
    存储管理器抽象基类，定义统一的存储层接口。
    """

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
    def write(self, table_id: str, df: pl.DataFrame):
        """
        全量/覆盖写入数据。
        
        Args:
            table_id: 表 ID
            df: 包含 symbol 和 date 等字段的 DataFrame
        """
        pass

    @abc.abstractmethod
    def append(self, table_id: str, df: pl.DataFrame):
        """
        增量写入数据。合并新旧数据并按主键（通常是 date）去重。
        
        Args:
            table_id: 表 ID
            df: 包含 symbol 和 date 等字段的 DataFrame
        """
        pass
