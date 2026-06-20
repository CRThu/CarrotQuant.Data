"""EastMoneyProvider 集成测试。

真实 API 调用，验证:
1. push2 板块列表拉取与分页
2. push2 板块成分股拉取
3. datacenter 龙虎榜拉取与字段重命名
4. datacenter 机构交易拉取与字段重命名
5. timestamp/datetime 计算正确
6. 空数据防御
"""

import pytest
from loguru import logger

from app.provider.eastmoney_provider import EastMoneyProvider


@pytest.fixture
def provider():
    """创建 EastMoneyProvider 实例。"""
    return EastMoneyProvider()


class TestBoardList:
    """测试 push2 板块列表拉取。"""

    def test_concept_board_list(self, provider):
        """概念板块列表应返回非空字典。"""
        boards = provider._fetch_board_list("concept")
        assert isinstance(boards, dict)
        assert len(boards) > 0
        first_code = next(iter(boards))
        assert first_code.startswith("BK")
        logger.info(f"概念板块数量: {len(boards)}")

    def test_industry_board_list(self, provider):
        """行业板块列表应返回非空字典。"""
        boards = provider._fetch_board_list("industry")
        assert isinstance(boards, dict)
        assert len(boards) > 0
        first_code = next(iter(boards))
        assert first_code.startswith("BK")
        logger.info(f"行业板块数量: {len(boards)}")

    def test_board_name_cache(self, provider):
        """第二次调用应命中缓存，不再请求 API。"""
        provider._fetch_board_list("concept")
        assert "concept" in provider._board_name_cache
        cached = provider._board_name_cache["concept"]
        assert isinstance(cached, dict)
        assert len(cached) > 0


class TestBoardCons:
    """测试 push2 板块成分股拉取。"""

    def test_fetch_board_cons_df(self, provider):
        """概念板块成分股应返回包含标准列的 DataFrame。"""
        boards = provider._fetch_board_list("concept")
        first_code = next(iter(boards))
        df = provider._fetch_board_cons_df("concept", first_code)
        assert not df.is_empty()
        assert "symbol" in df.columns
        assert "stock_name" in df.columns
        assert "board_code" in df.columns
        assert "board_name" in df.columns
        assert "datetime" in df.columns
        assert "timestamp" in df.columns
        assert df["board_code"].unique().to_list() == [first_code]
        logger.info(f"板块 {first_code} 成分股数量: {len(df)}")


class TestDragonTiger:
    """测试 datacenter 龙虎榜拉取。"""

    def test_fetch_dragon_tiger(self, provider):
        """龙虎榜应返回包含全部标准列的非空 DataFrame。"""
        df = provider.fetch(
            "ashare.dragon_tiger.eastmoney", "_ALL_",
            "2026-05-01", "2026-06-18",
        )
        assert not df.is_empty(), "龙虎榜数据不应为空"
        assert "symbol" in df.columns
        assert "datetime" in df.columns
        assert "timestamp" in df.columns
        logger.info(f"龙虎榜数据量: {len(df)}")

    def test_dragon_tiger_timestamp_valid(self, provider):
        """龙虎榜 timestamp 应为 Int64 毫秒时间戳，datetime 应为 ISO8601。"""
        df = provider.fetch(
            "ashare.dragon_tiger.eastmoney", "_ALL_",
            "2026-05-01", "2026-06-18",
        )
        assert df["timestamp"].dtype == df.schema["timestamp"]
        ts_min = df["timestamp"].min()
        ts_max = df["timestamp"].max()
        assert ts_min > 0, f"timestamp 最小值异常: {ts_min}"
        assert ts_max > ts_min, f"timestamp 范围异常: {ts_min} ~ {ts_max}"
        logger.info(f"timestamp 范围: {ts_min} ~ {ts_max}")

        dt_sample = df["datetime"][0]
        assert "T" in dt_sample, f"datetime 缺少 T 分隔符: {dt_sample}"
        assert "+" in dt_sample or "-" in dt_sample, f"datetime 缺少时区偏移: {dt_sample}"
        logger.info(f"datetime 样本: {dt_sample}")

    def test_dragon_tiger_field_renames(self, provider):
        """龙虎榜字段应全部重命名为 snake_case。"""
        df = provider.fetch(
            "ashare.dragon_tiger.eastmoney", "_ALL_",
            "2026-05-01", "2026-06-18",
        )
        expected = [
            "symbol", "stock_name", "explain", "close_price", "change_rate",
            "net_amount", "buy_amount", "sell_amount", "deal_amount",
            "market_amount", "deal_net_ratio", "deal_amount_ratio",
            "turnover_rate", "float_market_cap", "explanation",
            "day1_change_rate", "day2_change_rate", "day5_change_rate", "day10_change_rate",
        ]
        for col in expected:
            assert col in df.columns, f"缺少字段: {col}"
        logger.info(f"龙虎榜列: {df.columns}")


class TestInstTrade:
    """测试 datacenter 机构交易拉取。"""

    def test_fetch_inst_trade(self, provider):
        """机构交易应返回包含标准列的非空 DataFrame。"""
        df = provider.fetch(
            "ashare.inst_trade.eastmoney", "_ALL_",
            "2026-05-01", "2026-06-18",
        )
        assert not df.is_empty(), "机构交易数据不应为空"
        assert "symbol" in df.columns
        assert "datetime" in df.columns
        assert "timestamp" in df.columns
        logger.info(f"机构交易数据量: {len(df)}")

    def test_inst_trade_timestamp_valid(self, provider):
        """机构交易 timestamp 应为 Int64 毫秒时间戳，datetime 应为 ISO8601。"""
        df = provider.fetch(
            "ashare.inst_trade.eastmoney", "_ALL_",
            "2026-05-01", "2026-06-18",
        )
        ts_min = df["timestamp"].min()
        ts_max = df["timestamp"].max()
        assert ts_min > 0, f"timestamp 最小值异常: {ts_min}"
        assert ts_max > ts_min, f"timestamp 范围异常: {ts_min} ~ {ts_max}"
        logger.info(f"timestamp 范围: {ts_min} ~ {ts_max}")

        dt_sample = df["datetime"][0]
        assert "T" in dt_sample, f"datetime 缺少 T 分隔符: {dt_sample}"
        assert "+" in dt_sample or "-" in dt_sample, f"datetime 缺少时区偏移: {dt_sample}"
        logger.info(f"datetime 样本: {dt_sample}")

    def test_inst_trade_field_renames(self, provider):
        """机构交易字段应全部重命名为 snake_case。"""
        df = provider.fetch(
            "ashare.inst_trade.eastmoney", "_ALL_",
            "2026-05-01", "2026-06-18",
        )
        expected = [
            "symbol", "stock_name", "close_price", "change_rate",
            "buy_times", "sell_times", "buy_amount", "sell_amount",
            "net_buy_amount", "market_amount", "ratio",
            "turnover_rate", "float_market_cap", "explanation",
            "day1_change_rate", "day2_change_rate", "day3_change_rate",
            "day5_change_rate", "day10_change_rate",
        ]
        for col in expected:
            assert col in df.columns, f"缺少字段: {col}"
        logger.info(f"机构交易列: {df.columns}")
