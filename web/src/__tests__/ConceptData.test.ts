import { describe, it, expect } from 'vitest';
import { groupRowsToConceptBoards } from '../services/transformers';

describe('groupRowsToConceptBoards', () => {
  it('correctly aggregates 2D matrix rows into grouped ConceptBoardItems', () => {
    const columns = ['board_code', 'board_name', 'symbol', 'stock_name'];
    const data: (string | number)[][] = [
      ['BK0001', '低空经济', 'sh.600000', '浦发银行'],
      ['BK0001', '低空经济', 'sz.000001', '平安银行'],
      ['BK0002', '人工智能', 'sz.000002', '万科A'],
    ];

    const boards = groupRowsToConceptBoards(columns, data);

    expect(boards).toHaveLength(2);

    const bk1 = boards.find(b => b.board_code === 'BK0001');
    expect(bk1).toBeDefined();
    expect(bk1?.board_name).toEqual('低空经济');
    expect(bk1?.stock_count).toEqual(2);
    expect(bk1?.stocks).toEqual([
      { symbol: 'sh.600000', stock_name: '浦发银行' },
      { symbol: 'sz.000001', stock_name: '平安银行' },
    ]);

    const bk2 = boards.find(b => b.board_code === 'BK0002');
    expect(bk2).toBeDefined();
    expect(bk2?.board_name).toEqual('人工智能');
    expect(bk2?.stock_count).toEqual(1);
  });

  it('handles empty matrix input without error', () => {
    const columns = ['board_code', 'board_name', 'symbol', 'stock_name'];
    const boards = groupRowsToConceptBoards(columns, []);
    expect(boards).toEqual([]);
  });
});
