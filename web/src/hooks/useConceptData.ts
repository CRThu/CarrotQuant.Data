import { useState, useEffect, useCallback, useMemo } from 'react';
import type { ConceptBoardItem } from '../types/api';
import { apiClient } from '../services/apiClient';

export interface UseConceptDataReturn {
  conceptTableId: string;
  setConceptTableId: (id: string) => void;
  boards: ConceptBoardItem[];
  selectedBoardCode: string | null;
  setSelectedBoardCode: (code: string | null) => void;
  currentBoard: ConceptBoardItem | null;
  searchQuery: string;
  setSearchQuery: (query: string) => void;
  loading: boolean;
  error: string | null;
  refreshConceptData: () => void;
}

export const useConceptData = (): UseConceptDataReturn => {
  const [conceptTableId, setConceptTableId] = useState<string>('ashare.concept.eastmoney');
  const [allBoards, setAllBoards] = useState<ConceptBoardItem[]>([]);
  const [selectedBoardCode, setSelectedBoardCode] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  // 方案 A：初始化批量获取全量板块成分股，并在前端快速构建索引树
  const fetchConceptData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await apiClient.queryData({
        table_id: conceptTableId,
        page: 1,
        page_size: 50000, // 批量拉取全量
      });

      if (!res || !res.columns || !res.data) {
        setAllBoards([]);
        return;
      }

      const colMap = new Map<string, number>();
      res.columns.forEach((col, idx) => colMap.set(col.toLowerCase(), idx));

      const idxBoardCode = colMap.get('board_code') ?? -1;
      const idxBoardName = colMap.get('board_name') ?? -1;
      const idxSymbol = colMap.get('symbol') ?? -1;
      const idxStockName = colMap.get('stock_name') ?? -1;

      if (idxBoardCode === -1 || idxSymbol === -1) {
        setAllBoards([]);
        return;
      }

      const boardMap = new Map<string, ConceptBoardItem>();

      for (const row of res.data) {
        const boardCode = String(row[idxBoardCode] || '');
        const boardName = idxBoardName !== -1 ? String(row[idxBoardName] || '') : boardCode;
        const symbol = String(row[idxSymbol] || '');
        const stockName = idxStockName !== -1 ? String(row[idxStockName] || '') : symbol;

        if (!boardCode || !symbol) continue;

        if (!boardMap.has(boardCode)) {
          boardMap.set(boardCode, {
            board_code: boardCode,
            board_name: boardName,
            stock_count: 0,
            stocks: [],
          });
        }

        const item = boardMap.get(boardCode)!;
        item.stocks.push({ symbol, stock_name: stockName });
        item.stock_count += 1;
      }

      const boardList = Array.from(boardMap.values()).sort((a, b) => b.stock_count - a.stock_count);
      setAllBoards(boardList);

      // 智能选中：若当前 selectedBoardCode 为空或在新列表中不存在，则默认重置选中第 1 个板块
      if (boardList.length > 0) {
        setSelectedBoardCode((prev) => {
          const exists = boardList.some((b) => b.board_code === prev);
          return exists ? prev : boardList[0].board_code;
        });
      }
    } catch (err: any) {
      console.error('Failed to fetch concept boards:', err);
      setError(err?.response?.data?.detail || err?.message || '获取板块成分股失败');
      setAllBoards([]);
    } finally {
      setLoading(false);
    }
  }, [conceptTableId]);

  useEffect(() => {
    fetchConceptData();
  }, [fetchConceptData]);

  // 根据搜索关键字过滤板块列表
  const filteredBoards = useMemo(() => {
    if (!searchQuery.trim()) return allBoards;
    const q = searchQuery.toLowerCase().trim();
    return allBoards.filter(
      (b) =>
        b.board_name.toLowerCase().includes(q) ||
        b.board_code.toLowerCase().includes(q) ||
        b.stocks.some((s) => s.stock_name.toLowerCase().includes(q) || s.symbol.toLowerCase().includes(q))
    );
  }, [allBoards, searchQuery]);

  // 当前选中的板块实体
  const currentBoard = useMemo(() => {
    if (!selectedBoardCode) return null;
    return allBoards.find((b) => b.board_code === selectedBoardCode) || null;
  }, [allBoards, selectedBoardCode]);

  return {
    conceptTableId,
    setConceptTableId,
    boards: filteredBoards,
    selectedBoardCode,
    setSelectedBoardCode,
    currentBoard,
    searchQuery,
    setSearchQuery,
    loading,
    error,
    refreshConceptData: fetchConceptData,
  };
};
