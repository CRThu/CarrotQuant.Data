import polars as pl
from pathlib import Path
import shutil
from .base import IStorage

class ParquetStorage(IStorage):
    def read(self, path: Path) -> pl.DataFrame:
        if not path.exists():
            return pl.DataFrame()
        # 如果是目录，读取目录下所有parquet文件
        if path.is_dir():
            return pl.read_parquet(path / "*.parquet")
        return pl.read_parquet(path)

    def write(self, path: Path, df: pl.DataFrame):
        # 确保目录存在
        path.parent.mkdir(parents=True, exist_ok=True)
        
        # 原子写入逻辑: 先写临时文件，再移动
        tmp_path = path.with_suffix(".tmp")
        df.write_parquet(tmp_path)
        
        if path.exists():
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
        
        tmp_path.rename(path)

    def append(self, path: Path, df: pl.DataFrame, unique_subset: list[str] = None):
        if not path.exists():
            self.write(path, df)
            return

        existing_df = self.read(path)
        combined_df = pl.concat([existing_df, df])
        
        if unique_subset:
            combined_df = combined_df.unique(subset=unique_subset, keep="last")
        
        self.write(path, combined_df)
