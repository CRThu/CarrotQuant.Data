import { describe, it, expect } from 'vitest';
import { matrixToOHLC, ohlcToVolume } from '../services/transformers';
import type { QueryMatrixResponse } from '../types/api';

describe('transformers test suite', () => {
  const mockMatrix: QueryMatrixResponse = {
    table_id: 'ashare.kline.1d.raw.baostock',
    total: 3,
    page: 1,
    page_size: 500,
    total_pages: 1,
    count: 3,
    columns: ['timestamp', 'datetime', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'amount'],
    data: [
      [1704067200000, '2024-01-01T15:00:00.000+08:00', 'sh.600000', 10.0, 10.5, 9.8, 10.2, 10000, 102000],
      [1704153600000, '2024-01-02T15:00:00.000+08:00', 'sh.600000', 10.2, 10.8, 10.1, 10.6, 12000, 127200],
      [1704240000000, '2024-01-03T15:00:00.000+08:00', 'sh.600000', 10.6, 10.7, 10.0, 10.1, 8000, 80800],
    ],
  };

  it('should parse 2D matrix to TradingView OHLC Bars correctly', () => {
    const bars = matrixToOHLC(mockMatrix);
    expect(bars.length).toBe(3);
    expect(bars[0].time).toBe('2024-01-01');
    expect(bars[0].open).toBe(10.0);
    expect(bars[0].close).toBe(10.2);
    expect(bars[0].volume).toBe(10000);
  });

  it('should respect maxBars limit parameter', () => {
    const bars = matrixToOHLC(mockMatrix, 2);
    expect(bars.length).toBe(2);
    expect(bars[0].time).toBe('2024-01-02');
    expect(bars[1].time).toBe('2024-01-03');
  });

  it('should generate red/green volume histogram bars correctly', () => {
    const bars = matrixToOHLC(mockMatrix);
    const volsRed = ohlcToVolume(bars, 'redUpGreenDown');
    const volsGreen = ohlcToVolume(bars, 'greenUpRedDown');

    expect(volsRed.length).toBe(3);
    // 2024-01-01: close 10.2 >= open 10.0 -> A股模式: 涨 (红 #ef4444) / 美股模式: 涨 (绿 #22c55e)
    expect(volsRed[0].color).toBe('#ef4444');
    expect(volsGreen[0].color).toBe('#22c55e');

    // 2024-01-03: close 10.1 < open 10.6 -> A股模式: 跌 (绿 #22c55e) / 美股模式: 跌 (红 #ef4444)
    expect(volsRed[2].color).toBe('#22c55e');
    expect(volsGreen[2].color).toBe('#ef4444');
  });

  it('should parse 5m intraday K-lines without truncating to same date', () => {
    const mock5mMatrix: QueryMatrixResponse = {
      table_id: 'ashare.kline.5m.raw.baostock',
      total: 2,
      page: 1,
      page_size: 500,
      total_pages: 1,
      count: 2,
      columns: ['timestamp', 'datetime', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'amount'],
      data: [
        [1704072900000, '2024-01-01T09:35:00.000+08:00', 'sh.600000', 10.0, 10.5, 9.8, 10.2, 10000, 102000],
        [1704073200000, '2024-01-01T09:40:00.000+08:00', 'sh.600000', 10.2, 10.6, 10.1, 10.4, 12000, 124800],
      ],
    };

    const bars = matrixToOHLC(mock5mMatrix);
    expect(bars.length).toBe(2);
    expect(bars[0].time).toBe('2024-01-01 09:35');
    expect(bars[1].time).toBe('2024-01-01 09:40');
  });
});
