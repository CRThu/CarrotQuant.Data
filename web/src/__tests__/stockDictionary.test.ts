import { describe, it, expect, beforeEach } from 'vitest';
import { stockDictionary } from '../services/stockDictionary';

describe('stockDictionary service (0 hardcoding, dynamic load)', () => {
  beforeEach(() => {
    // 动态注入样本数据
    stockDictionary.loadSymbols(['sh.600000', 'sz.000001', 'sh.600519', 'sz.300750'], {
      'sh.600000': '浦发银行',
      'sz.000001': '平安银行',
      'sh.600519': '贵州茅台',
      'sz.300750': '宁德时代',
    });
  });

  it('instant prefix search for pure number 00 / 600', () => {
    // "00" 匹配 sz.000001
    const res00 = stockDictionary.search('00');
    expect(res00.length).toBeGreaterThan(0);
    expect(res00.some((i) => i.symbol === 'sz.000001')).toBe(true);

    // "600" 匹配 sh.600000, sh.600519
    const res600 = stockDictionary.search('600');
    expect(res600.length).toBe(2);
  });

  it('searches by pinyin initials (pfyh, payh, gzmt)', () => {
    const resPfyh = stockDictionary.search('pfyh');
    expect(resPfyh.length).toBe(1);
    expect(resPfyh[0].symbol).toBe('sh.600000');

    const resPayh = stockDictionary.search('payh');
    expect(resPayh.length).toBe(1);
    expect(resPayh[0].symbol).toBe('sz.000001');
  });

  it('supports dynamic addOrUpdate without any hardcoded initial state', () => {
    stockDictionary.addOrUpdate('sz.000002', '万科A');
    const res = stockDictionary.search('wka');
    expect(res.length).toBe(1);
    expect(res[0].name).toBe('万科A');
  });
});
