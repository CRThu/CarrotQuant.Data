Write-Host "--- 正在启动一键下载测试数据任务（包含 K 线复权/不复权及复权因子） ---" -ForegroundColor Cyan

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

if ($LASTEXITCODE -eq 0) {
    Write-Host "--- 所有数据下载完成 ---" -ForegroundColor Green
} else {
    Write-Host "--- 部分数据下载失败，请检查 ---" -ForegroundColor Red
}
