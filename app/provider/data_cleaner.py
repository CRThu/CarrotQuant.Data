import polars as pl
from datetime import datetime
from zoneinfo import ZoneInfo

class DataCleaner:
    """
    数据清洗工具类，负责标准化时间、列名等数据清洗逻辑
    """

    @staticmethod
    def standardize(df: pl.DataFrame, time_col: str, time_fmt: str = None, 
                   source_tz: str = "Asia/Shanghai", display_tz: str = "Asia/Shanghai") -> pl.DataFrame:
        """
        标准化时间列，生成 timestamp (Int64) 和 datetime (String) 字段
        
        Args:
            df: 输入的 Polars DataFrame
            time_col: 原始时间列名 (可以是字符串或日期类型)
            time_fmt: 字符串时间解析格式 (例如 "%Y-%m-%d" 或 "%Y%m%d%H%M%S%.3f")
            source_tz: 原始数据的时区名称 (IANA 格式，默认 Asia/Shanghai)
            display_tz: 展示时区名称 (IANA 格式，默认 Asia/Shanghai)
            
        Returns:
            标准化后的 DataFrame，包含 timestamp 和 datetime 列
        """
        if df.is_empty():
            # 即使是空表，也要添加标准化的列
            # 检查是否存在 time_col
            if time_col in df.columns:
                # 添加 timestamp 和 datetime 列（即使为空）
                df = df.with_columns([
                    pl.lit(None).cast(pl.Int64).alias("timestamp"),
                    pl.lit(None).cast(pl.Utf8).alias("datetime")
                ])
                
                # 按照规范，将核心列 [timestamp, datetime, symbol] 移动到最前面
                cols = list(df.columns)
                # 从后往前处理插入，确保最终顺序为 [symbol, datetime, timestamp, ...]
                for col in ["timestamp", "datetime", "symbol"]:
                    if col in cols:
                        cols.remove(col)
                        cols.insert(0, col)
                
                # 剔除原始的时间处理列 (如果它不是 symbol 或其他核心列)
                if time_col in cols and time_col not in ["timestamp", "datetime", "symbol"]:
                    cols.remove(time_col)
                        
                return df.select(cols)
            else:
                # 如果没有 time_col，直接返回
                return df
            
        # 1. 检测并转换 time_col 为 pl.Datetime (不带时区)
        if df.schema[time_col] == pl.Utf8:
            if time_fmt:
                df = df.with_columns(pl.col(time_col).str.to_datetime(format=time_fmt))
            else:
                df = df.with_columns(pl.col(time_col).str.to_datetime())
        elif df.schema[time_col] == pl.Date:
            df = df.with_columns(pl.col(time_col).cast(pl.Datetime))
        
        # 2. 标签化：将 naive datetime 视为 source_tz 的当地时间
        # 使用 replace_time_zone 设置时区信息
        df = df.with_columns(
            pl.col(time_col).dt.replace_time_zone(source_tz).alias("_temp_tz")
        )
        
        # 3. 存物理戳：转换为 UTC 并获取毫秒戳
        df = df.with_columns(
            pl.col("_temp_tz")
            .dt.convert_time_zone("UTC")
            .dt.timestamp("ms")
            .alias("timestamp")
        )
        
        # 4. 转展示列：转换为 display_tz 并格式化为带时区偏移的字符串
        df = df.with_columns(
            pl.col("_temp_tz")
            .dt.convert_time_zone(display_tz)
            .dt.strftime("%Y-%m-%dT%H:%M:%S%.3f%:z")
            .alias("datetime")
        )
        
        # 5. 清理临时列
        df = df.drop("_temp_tz")
        
        # 6. 按照规范，将核心列 [timestamp, datetime, symbol] 移动到最前面
        cols = list(df.columns)
        # 从后往前处理插入，确保最终顺序为 [symbol, datetime, timestamp, ...]
        for col in ["timestamp", "datetime", "symbol"]:
            if col in cols:
                cols.remove(col)
                cols.insert(0, col)
        
        # 7. 剔除原始的时间处理列 (如果它不是 symbol 或其他核心列)
        if time_col in cols and time_col not in ["timestamp", "datetime", "symbol"]:
            cols.remove(time_col)
                
        return df.select(cols)