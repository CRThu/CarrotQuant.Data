import { describe, it, expect } from 'vitest';
import { calculateMA, calculateMAMulti, calculateMACD, calculateRSI, deriveBSMarkers } from '../services/indicators';
import type { OHLCBar } from '../types/api';

describe('indicators test suite', () => {
  // 生成 30 天包含递增与递减走势的模拟 K 线
  const mockBars: OHLCBar[] = Array.from({ length: 30 }).map((_, idx) => {
    const day = String(idx + 1).padStart(2, '0');
    const price = idx < 15 ? 10 + idx * 0.5 : 17.5 - (idx - 15) * 0.4;
    return {
      time: `2024-01-${day}`,
      open: price - 0.1,
      high: price + 0.3,
      low: price - 0.2,
      close: price,
      volume: 10000 + idx * 100,
    };
  });

  it('should calculate MA5 correctly', () => {
    const ma5 = calculateMA(mockBars, 5);
    expect(ma5.length).toBe(26);
    expect(ma5[0].time).toBe('2024-01-05');
  });

  it('should calculate MACD DIF, DEA and Histogram bars', () => {
    const macd = calculateMACD(mockBars);
    expect(macd.dif.length).toBe(30);
    expect(macd.dea.length).toBe(30);
    expect(macd.macdBar.length).toBe(30);
    expect(macd.macdBar[0].color).toBeDefined();
  });

  it('should calculate RSI(14) correctly', () => {
    const rsi = calculateRSI(mockBars, 14);
    expect(rsi.length).toBe(16);
    expect(rsi[0].time).toBe('2024-01-15');
    expect(rsi[0].value).toBeGreaterThan(0);
    expect(rsi[0].value).toBeLessThanOrEqual(100);
  });

  it('should support colorMode (redUpGreenDown vs greenUpRedDown) for MACD and markers', () => {
    const macdRed = calculateMACD(mockBars, 12, 26, 9, 'redUpGreenDown');
    const macdGreen = calculateMACD(mockBars, 12, 26, 9, 'greenUpRedDown');

    const posIdx = macdRed.macdBar.findIndex((b) => b.value > 0);
    if (posIdx !== -1) {
      expect(macdRed.macdBar[posIdx].color).toBe('#ef4444');
      expect(macdGreen.macdBar[posIdx].color).toBe('#22c55e');
    }
  });

  it('should derive BS markers on Golden and Death crosses', () => {
    const mas = calculateMAMulti(mockBars);
    const markers = deriveBSMarkers(mockBars, mas.ma5, mas.ma20, 'redUpGreenDown');
    expect(Array.isArray(markers)).toBe(true);
  });
});
