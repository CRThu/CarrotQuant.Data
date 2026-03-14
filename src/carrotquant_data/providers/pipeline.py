import polars as pl
from loguru import logger

class DataPipeline:
    @staticmethod
    def clean_kline(df: pl.DataFrame, column_mapping: dict = None) -> pl.DataFrame:
        """
        标准 K 线清洗：重命名、日期转换、类型转换
        """
        if df.is_empty():
            return df

        # 1. 列名映射
        if column_mapping:
            df = df.rename(column_mapping)
        
        # 2. 日期标准化 (确保 date 列存在且为 Date 类型)
        if "date" in df.columns:
            # 尝试多种日期格式解析
            if df["date"].dtype == pl.String:
                df = df.with_columns(
                    pl.col("date").str.to_date(strict=False)
                )
        
        # 3. 类型强制转换
        # OHLCV 标准类型
        type_map = {
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Float64,
            "amount": pl.Float64,
        }
        
        for col, dtype in type_map.items():
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(dtype))
                
        return df

    @staticmethod
    def check_quality(df: pl.DataFrame) -> bool:
        """
        数据质量审计
        """
        if df.is_empty():
            return True
            
        # 检查是否有空值 (在关键列)
        critical_cols = ["date", "open", "close"]
        for col in critical_cols:
            if col in df.columns and df[col].null_count() > 0:
                logger.warning(f"Column {col} contains null values")
                return False
                
        # 检查逻辑异常 (如 high < low)
        if "high" in df.columns and "low" in df.columns:
            anomalies = df.filter(pl.col("high") < pl.col("low"))
            if not anomalies.is_empty():
                logger.error(f"Logic anomaly found: high < low in {len(anomalies)} rows")
                return False
                
        return True
