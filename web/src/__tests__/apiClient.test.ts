import { describe, it, expect, vi, beforeEach } from 'vitest';
import { apiClient } from '../services/apiClient';
import axios from 'axios';

describe('apiClient full execution coverage test suite', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it('should execute getHealth correctly', async () => {
    const spy = vi.spyOn(axios.Axios.prototype, 'request').mockResolvedValue({
      data: { status: 'healthy', version: '1.1.0' },
    });

    const res = await apiClient.getHealth();
    expect(spy).toHaveBeenCalled();
    expect(res.status).toBe('healthy');
  });

  it('should execute listTables with format param', async () => {
    const spy = vi.spyOn(axios.Axios.prototype, 'request').mockResolvedValue({
      data: { tables: ['ashare.kline.1d.raw.baostock'], total: 1 },
    });

    const res = await apiClient.listTables('csv');
    expect(spy).toHaveBeenCalled();
    expect(res.total).toBe(1);
  });

  it('should execute listSymbols correctly', async () => {
    const spy = vi.spyOn(axios.Axios.prototype, 'request').mockResolvedValue({
      data: { table_id: 'ashare.kline.1d.raw.baostock', symbols: ['sh.600000'] },
    });

    const res = await apiClient.listSymbols('ashare.kline.1d.raw.baostock');
    expect(spy).toHaveBeenCalled();
    expect(res.symbols).toContain('sh.600000');
  });

  it('should execute getTimeRange correctly', async () => {
    const spy = vi.spyOn(axios.Axios.prototype, 'request').mockResolvedValue({
      data: { min_timestamp: 1704067200000, max_timestamp: 1704240000000 },
    });

    const res = await apiClient.getTimeRange('ashare.kline.1d.raw.baostock');
    expect(spy).toHaveBeenCalled();
    expect(res.min_timestamp).toBe(1704067200000);
  });

  it('should execute queryData correctly', async () => {
    const spy = vi.spyOn(axios.Axios.prototype, 'request').mockResolvedValue({
      data: { table_id: 'ashare.kline.1d.raw.baostock', count: 5, columns: [], data: [] },
    });

    const res = await apiClient.queryData({ table_id: 'ashare.kline.1d.raw.baostock', symbols: 'sh.600000' });
    expect(spy).toHaveBeenCalled();
    expect(res.count).toBe(5);
  });

  it('should execute triggerSync with provider_kwargs correctly', async () => {
    const spy = vi.spyOn(axios.Axios.prototype, 'request').mockResolvedValue({
      data: { status: 'accepted', started_tasks: ['ashare.kline.1d.raw.tdx'] },
    });

    const res = await apiClient.triggerSync({
      table_ids: ['ashare.kline.1d.raw.tdx'],
      provider_kwargs: { mode: 'local', vipdoc_dir: 'C:\\custom_tdx' },
    });
    expect(spy).toHaveBeenCalled();
    expect(res.status).toBe('accepted');
  });

  it('should execute checkTdxPath correctly', async () => {
    const spy = vi.spyOn(axios.Axios.prototype, 'request').mockResolvedValue({
      data: { path: 'C:\\new_tdx\\vipdoc', exists: true, symbol_count: 5000, valid: true },
    });

    const res = await apiClient.checkTdxPath('C:\\new_tdx\\vipdoc');
    expect(spy).toHaveBeenCalled();
    expect(res.symbol_count).toBe(5000);
  });

  it('should execute downloadTdxZip correctly', async () => {
    const spy = vi.spyOn(axios.Axios.prototype, 'request').mockResolvedValue({
      data: { status: 'accepted', task_id: 'tdx.download.hsjday' },
    });

    const res = await apiClient.downloadTdxZip('C:\\new_tdx\\vipdoc');
    expect(spy).toHaveBeenCalled();
    expect(res.task_id).toBe('tdx.download.hsjday');
  });

  it('should execute getActiveTasks correctly', async () => {
    const spy = vi.spyOn(axios.Axios.prototype, 'request').mockResolvedValue({
      data: { active_tasks: [] },
    });

    const res = await apiClient.getActiveTasks();
    expect(spy).toHaveBeenCalled();
    expect(res.active_tasks).toEqual([]);
  });
});
