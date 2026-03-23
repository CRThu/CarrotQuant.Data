import polars as pl

class DataMerger:
    """
    数据合并助手类（存储层核心逻辑）
    基于 [symbol, timestamp] 复合主键进行对齐与去重
    """

    @staticmethod
    def merge_by_symbol_timestamp(old_df: pl.DataFrame, new_df: pl.DataFrame) -> pl.DataFrame:
        """
        基于 [symbol, timestamp] 复合主键进行合并。
        1. 执行 pl.concat。
        2. 基于 symbol, timestamp 去重，保留最后一条。
        3. 按 symbol, timestamp 升序排序。
        """
        if old_df.is_empty():
            return new_df.sort(["symbol", "timestamp"])
        if new_df.is_empty():
            return old_df.sort(["symbol", "timestamp"])

        # 确保 timestamp 类型一致 (Int64)
        old_df = old_df.with_columns(pl.col("timestamp").cast(pl.Int64))
        new_df = new_df.with_columns(pl.col("timestamp").cast(pl.Int64))

        combined_df = pl.concat([old_df, new_df])
        
        # 按 symbol, timestamp 去重并排序 (MMF 对齐需求)
        combined_df = combined_df.unique(subset=["symbol", "timestamp"], keep="last")
        combined_df = combined_df.sort(["symbol", "timestamp"])

        return combined_df
