from pathlib import Path
import polars as pl
import os
from .base import StorageManager
from .data_merger import DataMerger
from ..service.metadata_manager import MetadataManager

class CSVStorage(StorageManager):
    """
    CSV 存储实现类，支持 Hive 分区样式的存储格式。
    TS 路径规则：storage_root/csv/{table_id}/year={yyyy}/{symbol}.csv
    EV 路径规则：storage_root/csv/{table_id}/year={yyyy}/data.csv
    """

    def __init__(self, storage_root: str = "storage_root/csv", category: str = "TS"):
        super().__init__(category=category)
        self.storage_root = Path(storage_root)

    def _get_series_path(self, table_id: str, symbol: str, year: int) -> Path:
        """获取 TS 数据文件的完整路径。"""
        return self.storage_root / table_id / f"year={year}" / f"{symbol}.csv"

    def _get_event_path(self, table_id: str, year: int) -> Path:
        """获取 EV 数据文件的完整路径。文件名固定为 data。"""
        return self.storage_root / table_id / f"year={year}" / "data.csv"

    def _read_with_schema(self, table_id: str, path: Path, schema_override: dict = None) -> pl.DataFrame:
        """辅助方法：使用元数据中的 Schema 强锁类型读取 CSV"""
        # 1. 优先使用传入的 override schema (用于同步过程中的增量合并)
        if schema_override:
            return pl.read_csv(path, schema=schema_override)

        # 2. 加载元数据
        # self.storage_root 传入时已是 storage_root/csv，MetadataManager 需要 base_root
        meta_mgr = MetadataManager(self.storage_root.parent)
        metadata = meta_mgr.load(table_id, "csv")
        
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
                # 处理可能带参数的 Datetime 字符串
                if dtype_str.startswith("Datetime"):
                    polars_schema[col] = pl.Datetime
                else:
                    polars_schema[col] = type_map.get(dtype_str, pl.String)
            
            return pl.read_csv(path, schema=polars_schema)
            
        # 强制要求元数据：禁止自动推断以消除类型风险
        raise RuntimeError(f"Metadata not found for table '{table_id}' (format: csv). "
                          f"CSV reading requires explicit schema from metadata.json to ensure type safety.")

    def read_series(self, table_id: str, symbol: str, year: int) -> pl.DataFrame:
        """读取 TS 数据"""
        path = self._get_series_path(table_id, symbol, year)
        if not path.exists():
            return pl.DataFrame()
        return self._read_with_schema(table_id, path)

    def read_event(self, table_id: str, year: int) -> pl.DataFrame:
        """读取 EV 数据。返回整个 DataFrame。"""
        path = self._get_event_path(table_id, year)
        if not path.exists():
            return pl.DataFrame()
        return self._read_with_schema(table_id, path)

    def write_series(self, table_id: str, df: pl.DataFrame, mode: str = "append"):
        """
        写入时间序列数据 (TS)。按 symbol 和 year 分区存储。
        """
        if df.is_empty():
            return

        # 基于核心的 timestamp (ms) 提取年份用于 Hive 分区
        df = df.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms").dt.year().alias("_year")
        )

        # 按 symbol 和 _year 分组并处理每一块
        for (symbol, year), group_df in df.partition_by(["symbol", "_year"], as_dict=True).items():
            path = self._get_series_path(table_id, symbol, year)
            
            # 去除生成的 _year 辅助列，以便写入
            patch_df = group_df.drop("_year")
            
            if mode == "append" and path.exists():
                # 读取并合并
                old_df = self._read_with_schema(table_id, path, schema_override=patch_df.schema)
                # TS 模式：基于 [symbol, timestamp] 去重
                merged_df = DataMerger.merge(old_df, patch_df, subset=["symbol", "timestamp"])
            else:
                # 首次写入或 Overwrite
                merged_df = patch_df
            
            # 显式使用单 symbol 时间轴排序
            final_df = DataMerger.sort(merged_df, keys=["timestamp"])
            
            # 统一路径创建与原子落盘
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = path.with_suffix(".tmp")
            final_df.write_csv(tmp_path)
            os.replace(tmp_path, path)

    def write_event(self, table_id: str, df: pl.DataFrame, mode: str = "append"):
        """
        写入事件数据 (EV)。按 year 单文件布局存储，执行全行去重。
        EV 模式采用年份单文件布局与全行去重。
        """
        if df.is_empty():
            return

        # 基于核心的 timestamp (ms) 提取年份用于 Hive 分区
        df = df.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms").dt.year().alias("_year")
        )

        # 按 _year 分组并处理每一块
        for (year,), group_df in df.partition_by(["_year"], as_dict=True).items():
            path = self._get_event_path(table_id, year)
            
            # 去除生成的 _year 辅助列，以便写入
            patch_df = group_df.drop("_year")
            
            if mode == "append" and path.exists():
                # 读取并合并
                old_df = self._read_with_schema(table_id, path, schema_override=patch_df.schema)
                # EV 模式：全行去重
                merged_df = DataMerger.merge(old_df, patch_df, subset=None)
            else:
                # 首次写入或 Overwrite
                merged_df = patch_df
            
            # 显式使用 Time-First 排序，维护事件流
            final_df = DataMerger.sort(merged_df, keys=["timestamp", "symbol"])
            
            # 统一路径创建与原子落盘
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = path.with_suffix(".tmp")
            final_df.write_csv(tmp_path)
            os.replace(tmp_path, path)

    def get_all_symbols(self, table_id: str) -> list[str]:
        """扫描所有 year 目录，提取唯一 Symbol (文件名)"""
        table_dir = self.storage_root / table_id
        if not table_dir.exists():
            return []
        
        symbols = set()
        # 遍历 year={year} 目录下的 csv 文件
        for csv_file in table_dir.glob("year=*/*.csv"):
            # 排除 EV 的 data.csv 文件
            if csv_file.stem != "data":
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
