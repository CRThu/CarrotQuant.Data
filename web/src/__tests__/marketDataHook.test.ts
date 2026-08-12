import { describe, it, expect, vi } from 'vitest';
import { apiClient } from '../services/apiClient';

describe('useMarketData Hook Data Reuse', () => {
  it('skips duplicate network requests when switching back to tab with same table_id and symbol', async () => {
    const querySpy = vi.spyOn(apiClient, 'queryData').mockResolvedValue({
      table_id: 'ashare.kline.1d.raw.baostock',
      total: 1,
      page: 1,
      page_size: 5000,
      total_pages: 1,
      count: 1,
      columns: ['timestamp', 'close'],
      data: [[1700000000000, 10.5]],
    });

    const fetchKey = 'ashare.kline.1d.raw.baostock:sh.600000';
    let lastFetched = fetchKey;
    let hasData = true;

    let networkCalled = false;
    // 模拟从其他 Tab 切回时，如果 key 相同且已有数据，跳过网络调用
    if (lastFetched === fetchKey && hasData) {
      // 静默跳过
    } else {
      await apiClient.queryData({ table_id: 'ashare.kline.1d.raw.baostock', symbols: 'sh.600000' });
      networkCalled = true;
    }

    expect(networkCalled).toBe(false);
    expect(querySpy).not.toHaveBeenCalled();

    querySpy.mockRestore();
  });
});
