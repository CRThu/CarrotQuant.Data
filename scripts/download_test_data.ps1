Write-Host "--- 正在启动一键下载测试数据任务（包含 K 线复权/不复权、复权因子、通达信、东财板块/龙虎榜/机构交易） ---" -ForegroundColor Cyan

# 1. 独立下载复权因子，起始日期设置为 2015-01-01 (确保因子数据绝对完整)
Write-Host ">>> 正在下载复权因子 (2015-01-01 至今)..." -ForegroundColor Yellow
uv run -m app.gateway.cli sync `
    --tables "ashare.adj_factor.baostock" `
    --formats "csv,parquet" `
    --start "2015-01-01" `
    --end "2025-12-31" `
    --limit 10 `
    --output "./test_data_root"

# 2. 下载 K 线数据，起始日期设置为 2021-01-01
Write-Host ">>> 正在下载 K 线数据 (2021-01-01 至今)..." -ForegroundColor Yellow
uv run -m app.gateway.cli sync `
    --tables "ashare.kline.1d.adj.baostock,ashare.kline.1d.raw.baostock" `
    --formats "csv,parquet" `
    --start "2021-01-01" `
    --end "2025-12-31" `
    --limit 10 `
    --output "./test_data_root"

# 3. 下载通达信日线数据 (与 baostock 区间一致)
Write-Host ">>> 正在下载通达信日线数据 (2021-01-01 至今, online)..." -ForegroundColor Yellow
uv run -m app.gateway.cli sync `
    --tables "ashare.kline.1d.raw.tdx" `
    --formats "csv,parquet" `
    --start "2021-01-01" `
    --end "2025-12-31" `
    --limit 10 `
    --output "./test_data_root"

# 4. 下载通达信分钟线数据 (区间缩小，防止数据量爆炸)
Write-Host ">>> 正在下载通达信分钟线数据 (5m/1m, online)..." -ForegroundColor Yellow
uv run -m app.gateway.cli sync `
    --tables "ashare.kline.5m.raw.tdx,ashare.kline.1m.raw.tdx" `
    --formats "csv,parquet" `
    --start "2025-06-01" `
    --end "2025-06-30" `
    --limit 10 `
    --output "./test_data_root"

# 5. 下载东方财富板块成分股数据
Write-Host ">>> 正在下载东方财富板块成分股数据 (概念/行业)..." -ForegroundColor Yellow
uv run -m app.gateway.cli sync `
    --tables "ashare.concept.eastmoney,ashare.industry.eastmoney" `
    --formats "csv,parquet" `
    --start "2021-01-01" `
    --end "2025-12-31" `
    --limit 5 `
    --output "./test_data_root"

# 6. 下载东方财富龙虎榜和机构交易数据
Write-Host ">>> 正在下载东方财富龙虎榜和机构交易数据..." -ForegroundColor Yellow
uv run -m app.gateway.cli sync `
    --tables "ashare.dragon_tiger.eastmoney,ashare.inst_trade.eastmoney" `
    --formats "csv,parquet" `
    --start "2026-05-01" `
    --end "2026-06-18" `
    --output "./test_data_root"

if ($LASTEXITCODE -eq 0) {
    Write-Host "--- 所有数据下载完成 ---" -ForegroundColor Green
} else {
    Write-Host "--- 部分数据下载失败，请检查 ---" -ForegroundColor Red
}
