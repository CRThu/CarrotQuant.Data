import polars as pl

class DataMerger:
    """
    数据合并助手类（存储层核心逻辑）
    基于毫秒级时间戳进行对齐与去重
    """

    @staticmethod
    def merge_by_timestamp(old_df: pl.DataFrame, new_df: pl.DataFrame) -> pl.DataFrame:
        """
        基于 timestamp 列进行合并。
        1. 执行 pl.concat。
        2. 基于 timestamp 去重，保留最后一条。
        3. 按 timestamp 升序排序。
        """
        if old_df.is_empty():
            return new_df.sort("timestamp")
        if new_df.is_empty():
            return old_df.sort("timestamp")

        # 确保 timestamp 类型一致 (Int64)
        old_df = old_df.with_columns(pl.col("timestamp").cast(pl.Int64))
        new_df = new_df.with_columns(pl.col("timestamp").cast(pl.Int64))

        combined_df = pl.concat([old_df, new_df])
        
        # 按 timestamp 去重并排序
        combined_df = combined_df.unique(subset=["timestamp"], keep="last")
        combined_df = combined_df.sort("timestamp")

        return combined_df
