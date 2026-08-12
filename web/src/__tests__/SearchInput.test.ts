import { describe, it, expect } from 'vitest';
import { matchItem, type SearchableItem } from '../services/pinyin';

describe('SearchInput matching logic unit test', () => {
  const sampleItems: SearchableItem[] = [
    { code: 'sh.600000', name: '浦发银行', subText: '银行' },
    { code: 'sz.000001', name: '平安银行', subText: '银行' },
    { code: 'sh.600519', name: '贵州茅台', subText: '白酒' },
    { code: 'BK0612', name: '低空经济', subText: '概念板块' },
  ];

  it('filters items correctly by pinyin initials', () => {
    // pfyh -> 浦发银行
    const pfyhResults = sampleItems.filter((item) => matchItem(item, 'pfyh'));
    expect(pfyhResults.length).toBe(1);
    expect(pfyhResults[0].code).toBe('sh.600000');

    // payh -> 平安银行
    const payhResults = sampleItems.filter((item) => matchItem(item, 'payh'));
    expect(payhResults.length).toBe(1);
    expect(payhResults[0].name).toBe('平安银行');

    // dkjj -> 低空经济
    const dkResults = sampleItems.filter((item) => matchItem(item, 'dkjj'));
    expect(dkResults.length).toBe(1);
    expect(dkResults[0].name).toBe('低空经济');
  });

  it('filters items correctly by stock or board code', () => {
    const codeResults = sampleItems.filter((item) => matchItem(item, '600519'));
    expect(codeResults.length).toBe(1);
    expect(codeResults[0].name).toBe('贵州茅台');

    const boardResults = sampleItems.filter((item) => matchItem(item, 'BK0612'));
    expect(boardResults.length).toBe(1);
    expect(boardResults[0].name).toBe('低空经济');
  });
});
