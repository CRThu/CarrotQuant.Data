from pathlib import Path
import os
import polars as pl
from .base import StorageManager
from .data_merger import DataMerger

class ParquetStorage(StorageManager):
    """
    Parquet 存储实现类，按月度大表存储。
    路径规则：storage_root/parquet/{table_id}/year=YYYY/YYYY-MM.parquet
    """

    def __init__(self, storage_root: str = "storage_root/parquet", category: str = "TS"):
        super().__init__(category=category)
        self.storage_root = Path(storage_root)

    def _get_path(self, table_id: str, year: int, month: int) -> Path:
        """获取月度 Parquet 文件的完整路径。"""
        return self.storage_root / table_id / f"year={year}" / f"{year}-{month:02d}.parquet"

    def read(self, table_id: str, symbol: str, year: int) -> pl.DataFrame:
        """
        读取特定年份中包含指定证券的代码的数据。
        注意：由于是月度大表，需要从该年份所有月份文件中过滤出 symbol。
        """
        year_dir = self.storage_root / table_id / f"year={year}"
        if not year_dir.exists():
            return pl.DataFrame()
        
        dfs = []
        for file in year_dir.glob("*.parquet"):
            df = pl.read_parquet(file).filter(pl.col("symbol") == symbol)
            if not df.is_empty():
                dfs.append(df)
        
        if not dfs:
            return pl.DataFrame()
        
        return pl.concat(dfs).sort("timestamp")

    def write(self, table_id: str, df: pl.DataFrame, mode: str = "append"):
        """
        写入 Parquet 数据，自动按月分区落盘。
        """
        if df.is_empty():
            return

        # 提取年份和月份用于分区 (不再强加时区，使用系统默认/原始值)
        df = df.with_columns([
            pl.from_epoch(pl.col("timestamp"), time_unit="ms").dt.year().alias("_year"),
            pl.from_epoch(pl.col("timestamp"), time_unit="ms").dt.month().alias("_month")
        ])

        # 按 _year 和 _month 分组处理
        for (year, month), group_df in df.partition_by(["_year", "_month"], as_dict=True).items():
            path = self._get_path(table_id, year, month)
            patch_df = group_df.drop(["_year", "_month"])

            if mode == "append" and path.exists():
                # 读取月度大表并合并
                old_df = pl.read_parquet(path)
                final_df = DataMerger.merge_by_symbol_timestamp(old_df, patch_df)
            else:
                # 首次写入或 Overwrite，仍需按复合主键去重排序
                final_df = DataMerger.merge_by_symbol_timestamp(pl.DataFrame(), patch_df)

            # 原子写入
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = path.with_suffix(".tmp")
            final_df.write_parquet(tmp_path, compression="zstd")
            os.replace(tmp_path, path)

    def get_all_symbols(self, table_id: str) -> list[str]:
        """扫描所有 parquet 文件，提取唯一证券代码 (基于计算，可能较慢)"""
        table_dir = self.storage_root / table_id
        if not table_dir.exists():
            return []
        
        # 使用 scan_parquet 快速通过元数据提取唯一值
        pattern = table_dir / "year=*" / "*.parquet"
        if not any(table_dir.glob("year=*/*.parquet")):
            return []
            
        return (
            pl.scan_parquet(str(pattern))
            .select("symbol")
            .unique()
            .sort("symbol")
            .collect()["symbol"]
            .to_list()
        )

    def get_total_bars(self, table_id: str) -> int:
        """极速统计总行数 (基于 Parquet Metadata)"""
        table_dir = self.storage_root / table_id
        pattern = table_dir / "year=*" / "*.parquet"
        if not any(table_dir.glob("year=*/*.parquet")):
            return 0
        
        return pl.scan_parquet(str(pattern)).select(pl.len()).collect().item()

    def get_global_time_range(self, table_id: str) -> tuple[int, int]:
        """计算数据集全局最小/最大时间戳"""
        table_dir = self.storage_root / table_id
        pattern = table_dir / "year=*" / "*.parquet"
        if not any(table_dir.glob("year=*/*.parquet")):
            return (0, 0)
            
        res = pl.scan_parquet(str(pattern)).select([
            pl.col("timestamp").min().alias("min_ts"),
            pl.col("timestamp").max().alias("max_ts")
        ]).collect()
        
        min_ts = res["min_ts"][0] if not res.is_empty() and res["min_ts"][0] is not None else 0
        max_ts = res["max_ts"][0] if not res.is_empty() and res["max_ts"][0] is not None else 0
        return (min_ts, max_ts)

    def get_unique_timestamps(self, table_id: str) -> list[int]:
        """获取全局去重后的时间点"""
        table_dir = self.storage_root / table_id
        pattern = table_dir / "year=*" / "*.parquet"
        if not any(table_dir.glob("year=*/*.parquet")):
            return []
            
        df = pl.scan_parquet(str(pattern)).select("timestamp").unique().sort("timestamp").collect()
        return df["timestamp"].to_list() if not df.is_empty() else []
