from .base import StorageManager
from .csv_storage import CSVStorage
from .parquet_storage import ParquetStorage

class StorageFactory:
    """存储工厂类，负责根据格式返回对应的存储引擎实例 (Simplifying dependencies)"""

    @staticmethod
    def get_storage(storage_format: str, data_dir: str, category: str = "timeseries") -> StorageManager:
        """
        获取存储引擎。
        
        Args:
            storage_format: 存储格式 (如 "csv")
            data_dir: 数据存储根目录
            category: 数据类别 (TS 或 EV)
            
        Returns:
            StorageManager: 存储管理实例
        """
        if storage_format == "csv":
            # 内部拼接 format 路径
            return CSVStorage(data_dir=f"{data_dir}/csv", category=category)
        elif storage_format == "parquet":
            return ParquetStorage(data_dir=f"{data_dir}/parquet", category=category)
        else:
            raise ValueError(f"Unsupported storage format: {storage_format}")
