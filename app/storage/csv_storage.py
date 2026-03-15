import os
from pathlib import Path
import polars as pl
from .base import StorageManager

class CSVStorage(StorageManager):
    """
    CSV 存储实现类，支持 Hive 分区样式的存储格式。
    路径规则：storage_root/csv/{table_id}/year={yyyy}/{symbol}.csv
    """

    def __init__(self, storage_root: str = "storage_root/csv"):
        self.storage_root = Path(storage_root)

    def _get_path(self, table_id: str, symbol: str, year: int) -> Path:
        """获取文件的完整路径"""
        return self.storage_root / table_id / f"year={year}" / f"{symbol}.csv"

    def read(self, table_id: str, symbol: str, year: int) -> pl.DataFrame:
        """读取 CSV 数据"""
        path = self._get_path(table_id, symbol, year)
        if not path.exists():
            return pl.DataFrame()
        return pl.read_csv(path)

    def write(self, table_id: str, df: pl.DataFrame):
        """
        全量写入。按 symbol 和 year 分片。
        注意：此操作是对分片后的每个文件执行覆盖写入。
        """
        if df.is_empty():
            return

        # 确保 date 字段是日期类型以提取年份
        df = self._ensure_date_and_year(df)

        # 按 symbol 和 year 分组并写入
        for (symbol, year), group_df in df.partition_by(["symbol", "year"], as_dict=True).items():
            path = self._get_path(table_id, symbol, year)
            path.parent.mkdir(parents=True, exist_ok=True)
            # 移除中间生成的 year 列再保存
            group_df.drop("year").write_csv(path)

    def append(self, table_id: str, df: pl.DataFrame):
        """
        增量写入。读取旧数据 -> 合并新数据 -> 去重 -> 按 year 分片。
        """
        if df.is_empty():
            return

        df = self._ensure_date_and_year(df)

        # 按 symbol 和 year 分组
        for (symbol, year), patch_df in df.partition_by(["symbol", "year"], as_dict=True).items():
            path = self._get_path(table_id, symbol, year)
            
            if path.exists():
                old_df = pl.read_csv(path)
                # 确保旧数据的日期列格式与新数据一致
                if old_df.schema["date"] == pl.String:
                    old_df = old_df.with_columns(pl.col("date").str.to_date())
                
                # patch_df 在 _ensure_date_and_year 后也是 date 类型
                patch_clean = patch_df.drop("year")
                
                # 合并并按 date 去重
                combined_df = pl.concat([old_df, patch_clean])
                combined_df = combined_df.unique(subset=["date"], keep="last")
                # 写入前转回字符串保证 CSV 可读性一致性（可选，但 polars 默认写日期也很通用）
                combined_df.write_csv(path)
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                patch_df.drop("year").write_csv(path)

    def _ensure_date_and_year(self, df: pl.DataFrame) -> pl.DataFrame:
        """确保包含 year 列，并且 date 列统一为 Date 类型"""
        # 统一将 date 转换为 Date 类型
        if df.schema["date"] == pl.String:
            df = df.with_columns(pl.col("date").str.to_date())
        
        # 提取年份
        df = df.with_columns(pl.col("date").dt.year().alias("year"))
        return df
