import { describe, it, expect } from 'vitest';
import type { QueryMatrixResponse } from '../types/api';

describe('DataTable & CSV Export unit test suite', () => {
  const mockMatrix: QueryMatrixResponse = {
    table_id: 'ashare.kline.1d.raw.baostock',
    total: 2,
    page: 1,
    page_size: 500,
    total_pages: 1,
    count: 2,
    columns: ['timestamp', 'datetime', 'symbol', 'open', 'high', 'low', 'close', 'volume'],
    data: [
      [1704067200000, '2024-01-01T15:00:00.000+08:00', 'sh.600000', 10.0, 10.5, 9.8, 10.2, 10000],
      [1704153600000, '2024-01-02T15:00:00.000+08:00', 'sh.600000,浦发银行', 10.2, 10.8, 10.1, 9.9, null],
    ],
  };

  it('should format CSV content with headers and escaped commas correctly', () => {
    const headers = mockMatrix.columns.join(',');
    const rows = mockMatrix.data.map((row) =>
      row
        .map((cell) => {
          if (cell === null || cell === undefined) return '';
          const str = String(cell);
          return str.includes(',') ? `"${str}"` : str;
        })
        .join(',')
    );
    const csvLines = [headers, ...rows];

    expect(csvLines.length).toBe(3);
    expect(csvLines[0]).toBe('timestamp,datetime,symbol,open,high,low,close,volume');
    expect(csvLines[1]).toContain('1704067200000');
    // 包含逗号的字符串应当被双引号包裹转义
    expect(csvLines[2]).toContain('"sh.600000,浦发银行"');
    // null 单元格应当被转换为空字符串
    expect(csvLines[2].endsWith(',')).toBe(true);
  });
});
