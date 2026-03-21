from .base import StorageManager
from .csv_storage import CSVStorage

class StorageFactory:
    """存储工厂类，负责根据格式返回对应的存储引擎实例 (Simplifying dependencies)"""

    @staticmethod
    def get_storage(storage_format: str, storage_root: str) -> StorageManager:
        """
        获取存储引擎。
        
        Args:
            storage_format: 存储格式 (如 "csv")
            storage_root: 存储根目录
            
        Returns:
            StorageManager: 存储管理实例
        """
        if storage_format == "csv":
            # 根据 spec，此处动态实例化 CSVStorage
            # 这里的 storage_root 传入后内部会拼接 format 路径
            return CSVStorage(storage_root=f"{storage_root}/csv")
        else:
            raise ValueError(f"Unsupported storage format: {storage_format}")
