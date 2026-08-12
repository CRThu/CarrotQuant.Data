import { describe, it, expect } from 'vitest';
import { deriveBSMarkers } from '../services/indicators';
import type { OHLCBar, LineDataPoint } from '../types/api';

describe('Performance and Data Optimization Unit Tests', () => {
  it('ensures deriveBSMarkers handles empty array gracefully without allocating excess objects', () => {
    const bars: OHLCBar[] = [];
    const ma5: LineDataPoint[] = [];
    const ma20: LineDataPoint[] = [];
    const markers = deriveBSMarkers(bars, ma5, ma20, 'redUpGreenDown');
    expect(markers).toEqual([]);
  });

  it('correctly generates golden/death cross markers with expected timestamps and ascending order', () => {
    const mockBars: OHLCBar[] = [
      { time: '2024-01-01', open: 10, high: 11, low: 9, close: 10, volume: 100 },
      { time: '2024-01-02', open: 10, high: 12, low: 9, close: 11, volume: 110 },
      { time: '2024-01-03', open: 11, high: 13, low: 10, close: 12, volume: 120 },
      { time: '2024-01-04', open: 12, high: 14, low: 11, close: 13, volume: 130 },
    ];
    // Cross over: MA5 crosses MA20 upwards on 2024-01-03
    const ma5 = [
      { time: '2024-01-01', value: 9 },
      { time: '2024-01-02', value: 10 },
      { time: '2024-01-03', value: 12 },
      { time: '2024-01-04', value: 13 },
    ];
    const ma20 = [
      { time: '2024-01-01', value: 10 },
      { time: '2024-01-02', value: 10.5 },
      { time: '2024-01-03', value: 11 },
      { time: '2024-01-04', value: 11.5 },
    ];

    const markers = deriveBSMarkers(mockBars, ma5, ma20, 'redUpGreenDown');
    expect(markers).toHaveLength(1);
    expect(markers[0].time).toBe('2024-01-03');
    expect(markers[0].text).toBe('买 B');
    expect(markers[0].shape).toBe('arrowUp');
  });

  it('handles custom colorMode switching for markers correctly', () => {
    const mockBars: OHLCBar[] = [
      { time: '2024-01-01', open: 10, high: 11, low: 9, close: 10, volume: 100 },
      { time: '2024-01-02', open: 10, high: 12, low: 9, close: 11, volume: 110 },
      { time: '2024-01-03', open: 11, high: 13, low: 10, close: 12, volume: 120 },
    ];
    const ma5 = [
      { time: '2024-01-01', value: 9 },
      { time: '2024-01-02', value: 10 },
      { time: '2024-01-03', value: 12 },
    ];
    const ma20 = [
      { time: '2024-01-01', value: 10 },
      { time: '2024-01-02', value: 10.5 },
      { time: '2024-01-03', value: 11 },
    ];

    const markersRedUp = deriveBSMarkers(mockBars, ma5, ma20, 'redUpGreenDown');
    const markersGreenUp = deriveBSMarkers(mockBars, ma5, ma20, 'greenUpRedDown');

    expect(markersRedUp[0].color).toBe('#ef4444'); // Red for Up
    expect(markersGreenUp[0].color).toBe('#22c55e'); // Green for Up
  });
});
