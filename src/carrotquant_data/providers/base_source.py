from abc import ABC, abstractmethod
import polars as pl
from typing import Optional, Dict, Any
from datetime import datetime

class BaseSource(ABC):
    """
    数据源驱动基类
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    @abstractmethod
    def fetch(self, symbol: str, start_date: str, end_date: str, **kwargs) -> pl.DataFrame:
        """
        从远端获取原始数据
        :param symbol: 标的代码 (如 "sh600000")
        :param start_date: 开始日期 "YYYY-MM-DD"
        :param end_date: 结束日期 "YYYY-MM-DD"
        :return: Polars DataFrame
        """
        pass
