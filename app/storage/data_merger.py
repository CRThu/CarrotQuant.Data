import polars as pl

class DataMerger:
    """
    数据合并助手类（存储层核心逻辑）
    解耦合并与排序算子，支持 TS 和 EV 不同的去重策略
    """

    @staticmethod
    def merge(old_df: pl.DataFrame, new_df: pl.DataFrame, subset: list[str] | None = None) -> pl.DataFrame:
        """
        执行数据合并与去重。
        
        Args:
            old_df: 原有数据
            new_df: 新数据
            subset: 去重列，None 表示全行去重
            
        Returns:
            pl.DataFrame: 合并去重后的数据
        """
        if old_df.is_empty():
            return new_df
        if new_df.is_empty():
            return old_df

        # 确保 timestamp 类型一致 (Int64)
        old_df = old_df.with_columns(pl.col("timestamp").cast(pl.Int64))
        new_df = new_df.with_columns(pl.col("timestamp").cast(pl.Int64))

        combined_df = pl.concat([old_df, new_df])
        
        # 根据 subset 参数执行去重
        if subset:
            combined_df = combined_df.unique(subset=subset, keep="last")
        else:
            # 全行去重
            combined_df = combined_df.unique(keep="last")

        return combined_df

    @staticmethod
    def sort(df: pl.DataFrame, keys: list[str] | None = None) -> pl.DataFrame:
        """
        执行物理排序。
        
        Args:
            df: 待排序数据
            keys: 排序键，默认 ["timestamp", "symbol"]
            
        Returns:
            pl.DataFrame: 排序后的数据
        """
        if df.is_empty():
            return df
            
        if keys is None:
            keys = ["timestamp", "symbol"]
        
        # 动态排序：过滤掉不存在的列
        existing_keys = [key for key in keys if key in df.columns]
        
        # 如果没有任何列存在，返回原始数据
        if not existing_keys:
            return df
            
        return df.sort(existing_keys)
