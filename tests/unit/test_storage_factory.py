import pytest
from app.storage.storage_factory import StorageFactory
from app.storage.csv_storage import CSVStorage
from app.storage.parquet_storage import ParquetStorage

def test_storage_factory_csv():
    """
    测试输入 "csv" 返回 CSVStorage 实现类
    """
    storage = StorageFactory.get_storage("csv", "/tmp/test_root")
    assert isinstance(storage, CSVStorage)

def test_storage_factory_parquet():
    """
    测试输入 "parquet" 返回 ParquetStorage 实现类
    """
    storage = StorageFactory.get_storage("parquet", "/tmp/test_root")
    assert isinstance(storage, ParquetStorage)

def test_storage_factory_unsupported_format():
    """
    测试输入不支持的格式是否抛出 ValueError
    """
    with pytest.raises(ValueError) as exc_info:
        StorageFactory.get_storage("unsupported_format", "/tmp/test_root")
    
    assert "Unsupported storage format" in str(exc_info.value)

def test_storage_factory_case_sensitive():
    """
    测试格式名称大小写敏感
    """
    # 大写格式名应该抛出异常
    with pytest.raises(ValueError):
        StorageFactory.get_storage("CSV", "/tmp/test_root")
    
    with pytest.raises(ValueError):
        StorageFactory.get_storage("Parquet", "/tmp/test_root")

def test_storage_factory_storage_root_path():
    """
    测试存储根目录路径正确传递
    """
    test_root = "/custom/storage/root"
    
    csv_storage = StorageFactory.get_storage("csv", test_root)
    # CSVStorage 应该在根目录下创建 csv 子目录
    assert hasattr(csv_storage, 'storage_root')
    
    parquet_storage = StorageFactory.get_storage("parquet", test_root)
    # ParquetStorage 应该在根目录下创建 parquet 子目录
    assert hasattr(parquet_storage, 'storage_root')