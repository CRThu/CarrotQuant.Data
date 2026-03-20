from pathlib import Path
import polars as pl
from .base import StorageManager
from .data_merger import DataMerger

class CSVStorage(StorageManager):
    """
    CSV 存储实现类，支持 Hive 分区样式的存储格式。
    路径规则：storage_root/csv/{table_id}/year={yyyy}/{symbol}.csv
    """

    def __init__(self, storage_root: str = "storage_root/csv", category: str = "TS"):
        super().__init__(category=category)
        self.storage_root = Path(storage_root)

    def _get_path(self, table_id: str, symbol: str, year: int) -> Path:
        """获取文件的完整路径。table_id 直接作为文件夹名称。"""
        return self.storage_root / table_id / f"year={year}" / f"{symbol}.csv"

    def read(self, table_id: str, symbol: str, year: int) -> pl.DataFrame:
        """读取 CSV 数据"""
        path = self._get_path(table_id, symbol, year)
        if not path.exists():
            return pl.DataFrame()
        # CSV 中的 timestamp 建议始终以 Int64 读取
        return pl.read_csv(path).with_columns(pl.col("timestamp").cast(pl.Int64))

    def write(self, table_id: str, df: pl.DataFrame):
        """
        全量写入。直接基于 timestamp 提取 year。
        """
        if df.is_empty():
            return

        # 基于核心的 timestamp (ms) 提取年份用于 Hive 分区
        df = df.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms").dt.year().alias("year")
        )

        # 按 symbol 和 year 分组并写入
        for (symbol, year), group_df in df.partition_by(["symbol", "year"], as_dict=True).items():
            path = self._get_path(table_id, symbol, year)
            path.parent.mkdir(parents=True, exist_ok=True)
            group_df.drop("year").write_csv(path)

    def append(self, table_id: str, df: pl.DataFrame):
        """
        增量写入。
        """
        if df.is_empty():
            return

        # 提取年份用于路径定位
        df = df.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms").dt.year().alias("year")
        )

        # 按 symbol 和 year 分组
        for (symbol, year), patch_df in df.partition_by(["symbol", "year"], as_dict=True).items():
            path = self._get_path(table_id, symbol, year)
            
            if path.exists():
                old_df = pl.read_csv(path).with_columns(pl.col("timestamp").cast(pl.Int64))
                # 使用 DataMerger 执行基于数字 timestamp 的高性能合并
                combined_df = DataMerger.merge_by_timestamp(old_df, patch_df.drop("year"))
                combined_df.write_csv(path)
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                patch_df.drop("year").write_csv(path)

    def get_all_symbols(self, table_id: str) -> list[str]:
        """扫描所有 year 目录，提取唯一 Symbol (文件名)"""
        table_dir = self.storage_root / table_id
        if not table_dir.exists():
            return []
        
        symbols = set()
        # 遍历 year={year} 目录下的 csv 文件
        for csv_file in table_dir.glob("year=*/*.csv"):
            symbols.add(csv_file.stem)
        return sorted(list(symbols))

    def get_total_bars(self, table_id: str) -> int:
        """利用 scan_csv 的通配符扫描极速汇总总行数"""
        pattern = self.storage_root / table_id / "year=*" / "*.csv"
        # 显式路径检查，避免 scan_csv 报错
        if not any(self.storage_root.glob(f"{table_id}/year=*/*.csv")):
            return 0
            
        # 强制指定 timestamp 的 schema_overrides 以确保推断类型正确
        return pl.scan_csv(
            str(pattern), 
            schema_overrides={"timestamp": pl.Int64}
        ).select(pl.len()).collect().item()

    def get_global_time_range(self, table_id: str) -> tuple[int, int]:
        """计算数据集全局最小/最大时间戳"""
        pattern = self.storage_root / table_id / "year=*" / "*.csv"
        if not any(self.storage_root.glob(f"{table_id}/year=*/*.csv")):
            return (0, 0)
            
        res = pl.scan_csv(
            str(pattern), 
            schema_overrides={"timestamp": pl.Int64}
        ).select([
            pl.col("timestamp").min().alias("min_ts"),
            pl.col("timestamp").max().alias("max_ts")
        ]).collect()
        
        # 安全读取结果并提供默认值
        min_ts = res["min_ts"][0] if not res.is_empty() and res["min_ts"][0] is not None else 0
        max_ts = res["max_ts"][0] if not res.is_empty() and res["max_ts"][0] is not None else 0
        return (min_ts, max_ts)

    def get_unique_timestamps(self, table_id: str) -> list[int]:
        """获取全局去重后的时间点"""
        pattern = self.storage_root / table_id / "year=*" / "*.csv"
        if not any(self.storage_root.glob(f"{table_id}/year=*/*.csv")):
            return []
            
        df = pl.scan_csv(
            str(pattern), 
            schema_overrides={"timestamp": pl.Int64}
        ).select("timestamp").unique().sort("timestamp").collect()
        return df["timestamp"].to_list() if not df.is_empty() else []
