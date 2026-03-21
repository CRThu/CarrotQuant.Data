from typing import Dict
from app.provider.base import BaseProvider
from app.provider.baostock_provider import BaostockProvider

class ProviderManager:
    """
    驱动管理器，负责驱动的实例化与缓存
    """
    
    _instance = None
    _providers: Dict[str, BaseProvider] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ProviderManager, cls).__new__(cls)
        return cls._instance

    def get_provider(self, table_id: str) -> BaseProvider:
        """
        根据 table_id 获取对应的 Provider
        
        table_id 格式: {market}.{category}.{freq}.{adj}.{source}
        例如: ashare.kline.1d.adj.baostock
        """
        source = table_id.split('.')[-1]
        
        if source not in self._providers:
            if source == 'baostock':
                self._providers[source] = BaostockProvider()
            else:
                raise ValueError(f"Unsupported data source: {source}")
                
        return self._providers[source]
