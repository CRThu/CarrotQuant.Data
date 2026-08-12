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
  stockLoading: boolean;
  error: string | null;
  refreshConceptData: () => void;
}

export const useConceptData = (): UseConceptDataReturn => {
  const [conceptTableId, setConceptTableId] = useState<string>('ashare.concept.eastmoney');
  const [allBoards, setAllBoards] = useState<ConceptBoardItem[]>([]);
  const [selectedBoardCode, setSelectedBoardCode] = useState<string | null>(null);
  const [currentBoardStocks, setCurrentBoardStocks] = useState<{ symbol: string; stock_name: string }[]>([]);
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [loading, setLoading] = useState<boolean>(false);
  const [stockLoading, setStockLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  // 阶段 1：极速拉取板块列表 (仅 20KB 5ms)
  const fetchBoards = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await apiClient.getConceptBoards({
        table_id: conceptTableId,
        page_size: 1000,
      });

      if (!res || !res.boards) {
        setAllBoards([]);
        return;
      }

      const boardItems: ConceptBoardItem[] = res.boards.map((b) => ({
        board_code: b.board_code,
        board_name: b.board_name,
        stock_count: b.stock_count,
        stocks: [],
      }));

      setAllBoards(boardItems);

      if (boardItems.length > 0) {
        setSelectedBoardCode((prev) => {
          const exists = boardItems.some((b) => b.board_code === prev);
          return exists ? prev : boardItems[0].board_code;
        });
      }
    } catch (err: any) {
      console.error('Failed to fetch concept boards:', err);
      setError(err?.response?.data?.detail || err?.message || '获取板块列表失败');
      setAllBoards([]);
    } finally {
      setLoading(false);
    }
  }, [conceptTableId]);

  useEffect(() => {
    fetchBoards();
  }, [fetchBoards]);

  // 阶段 2：当选中的 selectedBoardCode 变更时，按需切片加载成分股
  useEffect(() => {
    if (!selectedBoardCode) {
      setCurrentBoardStocks([]);
      return;
    }

    let isMounted = true;
    const fetchStocks = async () => {
      setStockLoading(true);
      // 💡 举一反三防误导修补：切换板块时立刻清空上一个板块的成分股，防止列表残留旧数据
      setCurrentBoardStocks([]);
      try {
        const res = await apiClient.queryData({
          table_id: conceptTableId,
          board_code: selectedBoardCode,
          page: 1,
          page_size: 1000,
        });

        if (!isMounted) return;

        if (res && res.columns && res.data) {
          const colMap = new Map<string, number>();
          res.columns.forEach((col, idx) => colMap.set(col.toLowerCase(), idx));
          const idxSymbol = colMap.get('symbol') ?? -1;
          const idxStockName = colMap.get('stock_name') ?? -1;

          if (idxSymbol !== -1) {
            const stocks = res.data.map((row) => ({
              symbol: String(row[idxSymbol] || ''),
              stock_name: idxStockName !== -1 ? String(row[idxStockName] || '') : String(row[idxSymbol] || ''),
            }));
            setCurrentBoardStocks(stocks);
          }
        }
      } catch (err) {
        console.error('Failed to fetch stocks for board:', err);
      } finally {
        if (isMounted) setStockLoading(false);
      }
    };

    fetchStocks();

    return () => {
      isMounted = false;
    };
  }, [conceptTableId, selectedBoardCode]);

  // 搜索关键字过滤
  const filteredBoards = useMemo(() => {
    if (!searchQuery.trim()) return allBoards;
    const q = searchQuery.toLowerCase().trim();
    return allBoards.filter(
      (b) => b.board_name.toLowerCase().includes(q) || b.board_code.toLowerCase().includes(q)
    );
  }, [allBoards, searchQuery]);

  // 构建组装当前选中的板块与动态按需拉取到的成分股
  const currentBoard = useMemo(() => {
    if (!selectedBoardCode) return null;
    const base = allBoards.find((b) => b.board_code === selectedBoardCode);
    if (!base) return null;
    return {
      ...base,
      stocks: currentBoardStocks,
      stock_count: currentBoardStocks.length || base.stock_count,
    };
  }, [allBoards, selectedBoardCode, currentBoardStocks]);

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
    stockLoading,
    error,
    refreshConceptData: fetchBoards,
  };
};
