import polars as pl

class TimeStandardizer:
    """
    时间标准化工具类，确保输出数据符合“双时间轴协议”
    """

    @staticmethod
    def standardize(df: pl.DataFrame, time_col: str, time_fmt: str = None) -> pl.DataFrame:
        """
        标准化时间列，生成 timestamp (Int64) 和 datetime (String) 字段
        
        Args:
            df: 输入的 Polars DataFrame
            time_col: 原始时间列名 (可以是字符串或日期类型)
            time_fmt: 字符串时间解析格式 (例如 "%Y-%m-%d" 或 "%Y%m%d%H%M%S%.3f")
            
        Returns:
            标准化后的 DataFrame，包含 timestamp 和 datetime 列
        """
        # 1. 检测并转换 time_col 为 pl.Datetime
        if df.schema[time_col] == pl.Utf8:
            if time_fmt:
                df = df.with_columns(pl.col(time_col).str.to_datetime(format=time_fmt))
            else:
                df = df.with_columns(pl.col(time_col).str.to_datetime())
        elif df.schema[time_col] == pl.Date:
            df = df.with_columns(pl.col(time_col).cast(pl.Datetime))
        
        # 2. 生成核心列 timestamp: Int64 毫秒级 Unix 时间戳
        # 3. 生成可读列 datetime: ISO8601 字符串格式
        df = df.with_columns([
            pl.col(time_col).dt.timestamp("ms").alias("timestamp"),
            pl.col(time_col).dt.strftime("%Y-%m-%dT%H:%M:%S%.3f").alias("datetime")
        ])
        
        # 3. 按照规范，将核心列 [timestamp, datetime, symbol] 移动到最前面
        cols = list(df.columns)
        # 从后往前处理插入，确保最终顺序为 [symbol, datetime, timestamp, ...]
        for col in ["timestamp", "datetime", "symbol"]:
            if col in cols:
                cols.remove(col)
                cols.insert(0, col)
        
        # 4. 剔除原始的时间处理列 (如果它不是 symbol 或其他核心列)
        if time_col in cols and time_col not in ["timestamp", "datetime", "symbol"]:
            cols.remove(time_col)
                
        return df.select(cols)
