from typing import Dict
from app.provider.base import BaseProvider
from app.provider.baostock_provider import BaostockProvider
from app.provider.eastmoney_provider import EastMoneyProvider
from app.provider.tdx_provider import TDXProvider
from app.config.settings import settings


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

    def get_provider(self, table_id: str, **kwargs) -> BaseProvider:
        """
        根据 table_id 获取对应的 Provider

        table_id 格式: {market}.{category}.{freq}.{adj}.{source}
        例如: ashare.kline.1d.adj.baostock

        kwargs: 传递给 Provider 构造函数的额外参数 (如 TDXProvider 的 mode, vipdoc_dir)
        """
        source = table_id.split('.')[-1]
        
        if source not in self._providers:
            if source == 'baostock':
                self._providers[source] = BaostockProvider()
            elif source == 'eastmoney':
                self._providers[source] = EastMoneyProvider()
            elif source == 'tdx':
                self._providers[source] = TDXProvider(**kwargs)
            else:
                raise ValueError(f"Unsupported data source: {source}")
                
        return self._providers[source]
