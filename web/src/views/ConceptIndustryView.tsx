import React, { useMemo } from 'react';
import { useConceptData } from '../hooks/useConceptData';
import { Layers, ArrowRight, Building2 } from 'lucide-react';
import { SearchInput } from '../components/SearchInput';
import { matchItem, type SearchableItem } from '../services/pinyin';

interface ConceptIndustryViewProps {
  onSelectStock: (symbol: string) => void;
  globalSearchQuery?: string;
}

export const ConceptIndustryView: React.FC<ConceptIndustryViewProps> = ({
  onSelectStock,
  globalSearchQuery = '',
}) => {
  const {
    conceptTableId,
    setConceptTableId,
    boards,
    selectedBoardCode,
    setSelectedBoardCode,
    currentBoard,
    searchQuery,
    setSearchQuery,
    loading,
    stockLoading,
    error,
  } = useConceptData();

  // 同步顶栏全局搜索框输入到板块视图
  React.useEffect(() => {
    if (globalSearchQuery !== undefined) {
      setSearchQuery(globalSearchQuery);
    }
  }, [globalSearchQuery, setSearchQuery]);

  // 转换为通用搜索 SearchableItem 清单 (含拼音)
  const searchableBoards: SearchableItem[] = useMemo(() => {
    return boards.map((b) => ({
      code: b.board_code,
      name: b.board_name,
      subText: `${b.stock_count}只成分股`,
    }));
  }, [boards]);

  // 过滤后的板块列表
  const filteredBoards = useMemo(() => {
    if (!searchQuery.trim()) return boards;
    return boards.filter((b) =>
      matchItem({ code: b.board_code, name: b.board_name }, searchQuery)
    );
  }, [boards, searchQuery]);

  return (
    <div className="space-y-3 animate-in fade-in duration-300">
      {/* 统一格式视图页头 */}
      <div className="bg-slate-900/60 p-3.5 rounded-2xl border border-slate-800 flex items-center justify-between gap-4">
        <div className="flex items-center space-x-3">
          <div className="p-2 bg-cyan-950/60 rounded-xl border border-cyan-800/50 text-cyan-400">
            <Layers className="w-5 h-5" />
          </div>
          <div>
            <div className="flex items-center space-x-2">
              <h1 className="text-sm font-bold text-slate-100">板块概念</h1>
              <span className="text-[10px] font-mono text-cyan-400 bg-cyan-950 px-2 py-0.5 rounded border border-cyan-800/60">
                共 {boards.length} 个{conceptTableId.includes('concept') ? '概念' : '行业'}板块
              </span>
            </div>
            <p className="text-[10px] text-slate-400 mt-0.5">
              概念与行业板块成分股列表
            </p>
          </div>
        </div>

        {/* 概念 vs 行业 切换按钮 */}
        <div className="flex items-center bg-slate-950 p-1 rounded-xl border border-slate-800">
          <button
            onClick={() => setConceptTableId('ashare.concept.eastmoney')}
            className={`px-3 py-1 rounded-lg text-xs font-medium transition-all cursor-pointer ${
              conceptTableId === 'ashare.concept.eastmoney'
                ? 'bg-cyan-500 text-slate-950 font-bold'
                : 'text-slate-400 hover:text-slate-200'
            }`}
          >
            概念板块
          </button>
          <button
            onClick={() => setConceptTableId('ashare.industry.eastmoney')}
            className={`px-3 py-1 rounded-lg text-xs font-medium transition-all cursor-pointer ${
              conceptTableId === 'ashare.industry.eastmoney'
                ? 'bg-cyan-500 text-slate-950 font-bold'
                : 'text-slate-400 hover:text-slate-200'
            }`}
          >
            行业板块
          </button>
        </div>
      </div>

      {/* 主布局: 左侧板块列表 + 右侧成分股明细 */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-3">
        {/* 左侧 (4列): 板块搜索与紧凑列表 */}
        <div className="lg:col-span-4 bg-slate-900/80 rounded-2xl border border-slate-800 p-3 flex flex-col space-y-2.5 lg:sticky lg:top-2 lg:self-start h-[calc(100vh-130px)]">
          <div className="flex items-center justify-between">
            <span className="text-xs font-bold text-slate-200">板块列表</span>
            <span className="text-[10px] font-mono text-slate-500">{filteredBoards.length} / {boards.length}</span>
          </div>

          {/* 统一 SearchInput (支持拼音/代码/名称搜索) */}
          <SearchInput
            items={searchableBoards}
            value={searchQuery}
            onChange={setSearchQuery}
            placeholder="搜索板块名称/代码/拼音..."
            onSelect={(item) => setSelectedBoardCode(item.code)}
          />

          <div className="flex-1 overflow-y-auto space-y-1 pr-0.5 min-h-0 divide-y divide-slate-800/40">
            {loading ? (
              <div className="p-4 text-center text-xs text-slate-400 animate-pulse">
                加载板块列表中...
              </div>
            ) : error ? (
              <div className="p-3 text-center text-xs text-red-400">{error}</div>
            ) : filteredBoards.length === 0 ? (
              <div className="p-4 text-center text-xs text-slate-500">未找到匹配的板块</div>
            ) : (
              filteredBoards.map((board) => {
                const isSelected = board.board_code === selectedBoardCode;
                return (
                  <div
                    key={board.board_code}
                    onClick={() => setSelectedBoardCode(board.board_code)}
                    className={`px-2.5 py-2 rounded-xl cursor-pointer transition-all flex items-center justify-between ${
                      isSelected
                        ? 'bg-cyan-950/60 border border-cyan-500/60 text-cyan-300'
                        : 'text-slate-300 hover:bg-slate-800/40'
                    }`}
                  >
                    <div>
                      <div className="text-xs font-bold font-sans">{board.board_name}</div>
                      <div className="text-[10px] font-mono text-slate-500 mt-0.5">{board.board_code}</div>
                    </div>
                    <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-slate-950 text-slate-400 border border-slate-800 shrink-0">
                      {board.stock_count}只
                    </span>
                  </div>
                );
              })
            )}
          </div>
        </div>

        {/* 右侧 (8列): 成分股列表 */}
        <div className="lg:col-span-8 bg-slate-900/80 rounded-2xl border border-slate-800 p-3.5 flex flex-col space-y-3 h-[calc(100vh-130px)] min-h-[450px]">
          {currentBoard ? (
            <>
              {/* 板块 Header */}
              <div className="pb-2.5 border-b border-slate-800 flex items-center justify-between shrink-0">
                <div className="flex items-center space-x-2">
                  <Building2 className="w-4 h-4 text-cyan-400" />
                  <h3 className="text-sm font-bold text-slate-100">{currentBoard.board_name}</h3>
                  <span className="text-[10px] font-mono text-cyan-400 px-1.5 py-0.5 rounded bg-cyan-950 border border-cyan-800 font-bold">
                    {currentBoard.board_code}
                  </span>
                  <span className="text-xs text-slate-400 font-sans">
                    ({currentBoard.stock_count} 只成分股)
                  </span>
                </div>
              </div>

              {/* 成分股列表 */}
              {stockLoading ? (
                <div className="p-8 text-center text-xs text-slate-400 animate-pulse">
                  正在加载成分股...
                </div>
              ) : (
                <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-2 overflow-y-auto flex-1 pr-0.5 min-h-0">
                  {currentBoard.stocks.map((stock) => (
                    <div
                      key={stock.symbol}
                      onClick={() => onSelectStock(stock.symbol)}
                      className="px-3 py-2 bg-slate-950/60 border border-slate-800 hover:border-cyan-500/60 hover:bg-cyan-950/20 rounded-xl cursor-pointer transition-all group flex items-center justify-between"
                    >
                      <div>
                        <div className="text-xs font-bold text-slate-200 group-hover:text-cyan-400 transition-colors font-sans">
                          {stock.stock_name}
                        </div>
                        <div className="text-[10px] font-mono text-slate-500 mt-0.5">{stock.symbol}</div>
                      </div>
                      <ArrowRight className="w-3.5 h-3.5 text-slate-600 group-hover:text-cyan-400 group-hover:translate-x-0.5 transition-all" />
                    </div>
                  ))}
                </div>
              )}
            </>
          ) : (
            <div className="flex-1 flex items-center justify-center text-xs text-slate-500">
              请在左侧选择板块查看成分股
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default ConceptIndustryView;
