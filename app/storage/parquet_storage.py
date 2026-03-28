from pathlib import Path
import os
import polars as pl
from .base import StorageManager
from .data_merger import DataMerger

class ParquetStorage(StorageManager):
    """
    Parquet 存储实现类，按月度大表存储。
    TS 路径规则：storage_root/parquet/{table_id}/year=YYYY/YYYY-MM.parquet
    EV 路径规则：storage_root/parquet/{table_id}/year=YYYY/data.parquet
    """

    def __init__(self, storage_root: str = "storage_root/parquet", category: str = "TS"):
        super().__init__(category=category)
        self.storage_root = Path(storage_root)

    def _get_series_path(self, table_id: str, year: int, month: int) -> Path:
        """获取 TS 月度 Parquet 文件的完整路径。"""
        return self.storage_root / table_id / f"year={year}" / f"{year}-{month:02d}.parquet"

    def _get_event_path(self, table_id: str, year: int) -> Path:
        """获取 EV 数据文件的完整路径。文件名固定为 data。"""
        return self.storage_root / table_id / f"year={year}" / "data.parquet"

    def read_series(self, table_id: str, symbol: str, year: int) -> pl.DataFrame:
        """
        读取 TS 数据：从该年份所有月度大表中过滤出单支证券。
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

    def read_event(self, table_id: str, year: int) -> pl.DataFrame:
        """
        读取 EV 数据：读取该年份对应的全量 data.parquet 文件。
        """
        path = self._get_event_path(table_id, year)
        if not path.exists():
            return pl.DataFrame()
        return pl.read_parquet(path)

    def write_series(self, table_id: str, df: pl.DataFrame, mode: str = "append"):
        """
        写入时间序列数据 (TS)。按月分区落盘。
        """
        if df.is_empty():
            return

        # 提取年份和月份用于分区
        df = df.with_columns([
            pl.from_epoch(pl.col("timestamp"), time_unit="ms").dt.year().alias("_year"),
            pl.from_epoch(pl.col("timestamp"), time_unit="ms").dt.month().alias("_month")
        ])

        # 按 _year 和 _month 分组处理
        for (year, month), group_df in df.partition_by(["_year", "_month"], as_dict=True).items():
            path = self._get_series_path(table_id, year, month)
            patch_df = group_df.drop(["_year", "_month"])

            if mode == "append" and path.exists():
                # 读取月度大表并合并
                old_df = pl.read_parquet(path)
                # TS 模式：基于 [symbol, timestamp] 去重
                merged_df = DataMerger.merge(old_df, patch_df, subset=["symbol", "timestamp"])
            else:
                # 首次写入或 Overwrite
                merged_df = patch_df
            
            # 显式使用 Symbol-First 排序，支持 MMF 索引
            final_df = DataMerger.sort(merged_df, keys=["symbol", "timestamp"])

            # 原子写入
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = path.with_suffix(".tmp")
            final_df.write_parquet(tmp_path, compression="zstd")
            os.replace(tmp_path, path)

    def write_event(self, table_id: str, df: pl.DataFrame, mode: str = "append"):
        """
        写入事件数据 (EV)。按 year 单文件布局存储，执行全行去重。
        EV 模式采用年份单文件布局与全行去重。
        """
        if df.is_empty():
            return

        # 提取年份用于分区
        df = df.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms").dt.year().alias("_year")
        )

        # 按 _year 分组处理
        for (year,), group_df in df.partition_by(["_year"], as_dict=True).items():
            path = self._get_event_path(table_id, year)
            patch_df = group_df.drop("_year")

            if mode == "append" and path.exists():
                # 读取并合并
                old_df = pl.read_parquet(path)
                # EV 模式：全行去重
                merged_df = DataMerger.merge(old_df, patch_df, subset=None)
            else:
                # 首次写入或 Overwrite
                merged_df = patch_df
            
            # 显式使用 Time-First 排序，维护事件流
            final_df = DataMerger.sort(merged_df, keys=["timestamp", "symbol"])

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
        
        try:
            # 尝试提取 symbol 列
            symbols = (
                pl.scan_parquet(str(pattern))
                .select("symbol")
                .unique()
                .sort("symbol")
                .collect()["symbol"]
                .to_list()
            )
            return symbols
        except Exception:
            # 如果提取 symbol 列失败（如 EV 数据没有 symbol 列），返回空列表
            return []

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
