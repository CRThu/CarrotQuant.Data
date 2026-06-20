from pathlib import Path
import os
import polars as pl
from .base import StorageManager
from .data_merger import DataMerger
from ..service.metadata_manager import MetadataManager

class ParquetStorage(StorageManager):
    """
    Parquet 存储实现类，按年度大表存储。
    TS 路径规则：storage_root/parquet/{table_id}/year=YYYY/data.parquet
    EV 路径规则：storage_root/parquet/{table_id}/year=YYYY/data.parquet
    """

    def __init__(self, storage_root: str = "storage_root/parquet", category: str = "timeseries", partition: str = None, layout: str = "hive"):
        # 逻辑纠偏：如果是 EV，partition 默认为 "none"；如果是 TS，默认为 "none" (因为 Parquet 按年聚合，物理文件级别分区为 none)
        if partition is None:
            partition = "none"
            
        super().__init__(category=category)
        self.storage_root = Path(storage_root)
        self._partition = partition
        self._layout = layout

    @property
    def partition(self) -> str:
        return self._partition

    @property
    def layout(self) -> str:
        return self._layout

    def _get_series_path(self, table_id: str, year: int) -> Path:
        """获取 TS 年度 Parquet 文件的完整路径。"""
        return self.storage_root / table_id / f"year={year}" / "data.parquet"

    def _get_event_path(self, table_id: str, year: int) -> Path:
        """获取 EV 数据文件的完整路径。文件名固定为 data。"""
        return self.storage_root / table_id / f"year={year}" / "data.parquet"

    def _read_with_schema(self, table_id: str, path: Path, schema_override: dict = None) -> pl.DataFrame:
        """辅助方法：使用元数据中的 Schema 强锁类型读取 Parquet"""
        # 1. 物理读取
        df = pl.read_parquet(path)
        
        # 2. 优先使用传入的 override schema (用于同步过程中的增量合并)
        if schema_override:
            # 过滤掉不存在于 df 中的列，防止 cast 报错
            valid_schema = {k: v for k, v in schema_override.items() if k in df.columns}
            return df.cast(valid_schema)

        # 3. 加载元数据
        meta_mgr = MetadataManager(self.storage_root.parent)
        metadata = meta_mgr.load(table_id, "parquet")
        
        schema_dict = metadata.get("schema")
        if schema_dict:
            # 映射表：String -> Polars DataType
            type_map = {
                "Int64": pl.Int64,
                "Float64": pl.Float64,
                "String": pl.String,
                "Boolean": pl.Boolean,
                "Date": pl.Date,
                "Datetime": pl.Datetime
            }
            
            polars_schema = {}
            for col, dtype_str in schema_dict.items():
                if dtype_str.startswith("Datetime"):
                    polars_schema[col] = pl.Datetime
                else:
                    polars_schema[col] = type_map.get(dtype_str, pl.String)
            
            # 显式执行 cast 强制对齐类型
            return df.cast(polars_schema)
            
        # 强制要求元数据：保持架构统一性
        raise RuntimeError(f"Metadata not found for table '{table_id}' (format: parquet). "
                          f"Parquet reading requires explicit schema from metadata.json to ensure type safety.")

    def read_series(self, table_id: str, symbol: str, year: int) -> pl.DataFrame:
        """
        读取 TS 数据：从该年份全量 Parquet 文件中过滤出单支证券。
        """
        path = self._get_series_path(table_id, year)
        if not path.exists():
            return pl.DataFrame()
        
        df = self._read_with_schema(table_id, path).filter(pl.col("symbol") == symbol)
        
        if df.is_empty():
            return pl.DataFrame()
        
        return df.sort("timestamp")

    def _get_flat_event_path(self, table_id: str) -> Path:
        """获取平铺 EV 数据文件路径（无 Hive 分区）。"""
        return self.storage_root / table_id / "data.parquet"

    def _is_flat_event(self, table_id: str) -> bool:
        """判断该表是否使用平铺布局（无 timestamp 列的板块数据等）。"""
        return self._get_flat_event_path(table_id).exists()

    def read_event(self, table_id: str, year: int = None) -> pl.DataFrame:
        """读取 EV 数据。优先检查平铺文件，其次按年份读取。"""
        # 平铺模式
        if self._is_flat_event(table_id):
            path = self._get_flat_event_path(table_id)
            return self._read_with_schema(table_id, path)
        # Hive 分区模式
        if year is None:
            return pl.DataFrame()
        path = self._get_event_path(table_id, year)
        if not path.exists():
            return pl.DataFrame()
        return self._read_with_schema(table_id, path)

    def write_series(self, table_id: str, df: pl.DataFrame, mode: str = "append"):
        """
        写入时间序列数据 (TS)。按年分区落盘。
        """
        if df.is_empty():
            return

        # 提取年份用于分区
        df = df.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms").dt.year().alias("_year")
        )

        # 按 _year 分组处理
        for (year,), group_df in df.partition_by(["_year"], as_dict=True).items():
            path = self._get_series_path(table_id, year)
            patch_df = group_df.drop("_year")

            if mode == "append" and path.exists():
                # 读取并合并（使用强锁对齐 patch_df 的类型）
                old_df = self._read_with_schema(table_id, path, schema_override=patch_df.schema)
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

    def write_event(self, table_id: str, df: pl.DataFrame, mode: str, sort_keys: list[str]):
        """
        写入事件数据 (EV)。
        - 有 timestamp 列: 按 year Hive 分区布局 + 全行去重
        - 无 timestamp 列: 平铺布局 + 全行去重 (板块成分股等静态列表)
        """
        if df.is_empty():
            return

        if "timestamp" not in df.columns:
            # 平铺模式：无 Hive 分区，文件路径为 {root}/{table_id}/data.parquet
            path = self._get_flat_event_path(table_id)
            
            if mode == "append" and path.exists():
                old_df = self._read_with_schema(table_id, path, schema_override=df.schema)
                merged_df = DataMerger.merge(old_df, df, subset=None)
            else:
                merged_df = df
            
            final_df = DataMerger.sort(merged_df, keys=sort_keys)
            
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = path.with_suffix(".tmp")
            final_df.write_parquet(tmp_path, compression="zstd")
            os.replace(tmp_path, path)
            return

        # Hive 分区模式：提取年份用于分区
        df = df.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms").dt.year().alias("_year")
        )

        # 按 _year 分组处理
        for (year,), group_df in df.partition_by(["_year"], as_dict=True).items():
            path = self._get_event_path(table_id, year)
            patch_df = group_df.drop("_year")

            if mode == "append" and path.exists():
                # 读取并合并（使用强锁对齐 patch_df 的类型）
                old_df = self._read_with_schema(table_id, path, schema_override=patch_df.schema)
                # EV 模式：全行去重
                merged_df = DataMerger.merge(old_df, patch_df, subset=None)
            else:
                # 首次写入或 Overwrite
                merged_df = patch_df
            
            final_df = DataMerger.sort(merged_df, keys=sort_keys)

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
        # 平铺模式
        if self._is_flat_event(table_id):
            path = self._get_flat_event_path(table_id)
            return pl.scan_parquet(str(path)).select(pl.len()).collect().item()
        # Hive 分区模式
        table_dir = self.storage_root / table_id
        pattern = table_dir / "year=*" / "*.parquet"
        if not any(table_dir.glob("year=*/*.parquet")):
            return 0
        
        return pl.scan_parquet(str(pattern)).select(pl.len()).collect().item()

    def get_global_time_range(self, table_id: str) -> tuple[int, int]:
        """计算数据集全局最小/最大时间戳"""
        # 平铺模式（无 timestamp）返回 (0, 0)
        if self._is_flat_event(table_id):
            return (0, 0)
        # Hive 分区模式
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
        # 平铺模式（无 timestamp）返回空列表
        if self._is_flat_event(table_id):
            return []
        # Hive 分区模式
        table_dir = self.storage_root / table_id
        pattern = table_dir / "year=*" / "*.parquet"
        if not any(table_dir.glob("year=*/*.parquet")):
            return []
            
        df = pl.scan_parquet(str(pattern)).select("timestamp").unique().sort("timestamp").collect()
        return df["timestamp"].to_list() if not df.is_empty() else []
