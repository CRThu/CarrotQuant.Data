import polars as pl

class TimeStandardizer:
    """
    时间标准化工具类，确保输出数据符合“双时间轴协议”
    """

    @staticmethod
    def standardize(df: pl.DataFrame, time_col: str) -> pl.DataFrame:
        """
        标准化时间列，生成 timestamp (Int64) 和 datetime (String) 字段
        
        Args:
            df: 输入的 Polars DataFrame
            time_col: 原始时间列名 (可以是字符串或日期类型)
            
        Returns:
            标准化后的 DataFrame，包含 timestamp 和 datetime 列，并删除了原始 time_col
        """
        # 1. 检测并转换 time_col 为 pl.Datetime
        # 如果是字符串，先解析；如果是日期，转换为日期时间
        if df.schema[time_col] == pl.Utf8:
            df = df.with_columns(pl.col(time_col).str.to_datetime())
        elif df.schema[time_col] == pl.Date:
            df = df.with_columns(pl.col(time_col).cast(pl.Datetime))
        
        # 2. 生成核心列 timestamp: Int64 毫秒级 Unix 时间戳
        # 3. 生成可读列 datetime: ISO8601 字符串格式
        df = df.with_columns([
            pl.col(time_col).dt.timestamp("ms").alias("timestamp"),
            pl.col(time_col).dt.strftime("%Y-%m-%dT%H:%M:%S%.f").alias("datetime")
        ])
        
        # 4. 删除冗余的原始时间列
        df = df.drop(time_col)
        
        # 调整列顺序，通常将 timestamp 和 datetime 放在最前面
        cols = ["timestamp", "datetime"] + [c for c in df.columns if c not in ["timestamp", "datetime"]]
        return df.select(cols)
