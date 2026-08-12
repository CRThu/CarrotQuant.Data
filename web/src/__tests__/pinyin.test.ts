import { describe, it, expect } from 'vitest';
import { toPinyinInitials, matchItem, type SearchableItem } from '../services/pinyin';

describe('pinyin matching service', () => {
  it('converts chinese characters to pinyin initials', () => {
    expect(toPinyinInitials('浦发银行')).toBe('PFYH');
    expect(toPinyinInitials('平安银行')).toBe('PAYH');
    expect(toPinyinInitials('低空经济')).toBe('DKJJ');
    expect(toPinyinInitials('sh.600000')).toBe('SH.600000');
  });

  it('matches items by code, Chinese name, or pinyin initials', () => {
    const item: SearchableItem = {
      code: 'sh.600000',
      name: '浦发银行',
    };

    // 1. Code match
    expect(matchItem(item, '600000')).toBe(true);
    expect(matchItem(item, 'sh.600')).toBe(true);

    // 2. Name match
    expect(matchItem(item, '浦发')).toBe(true);

    // 3. Pinyin initial match
    expect(matchItem(item, 'pfyh')).toBe(true);
    expect(matchItem(item, 'pf')).toBe(true);

    // 4. Mismatch
    expect(matchItem(item, '茅台')).toBe(false);
  });
});
