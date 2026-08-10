import React from 'react';
import { useConceptData } from '../hooks/useConceptData';
import { Layers, Search, ArrowRight, Building2 } from 'lucide-react';

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

  return (
    <div className="space-y-3">
      {/* 紧凑视图页头 */}
      <div className="bg-slate-900/60 px-3.5 py-2.5 rounded-xl border border-slate-800 flex items-center justify-between">
        <div className="flex items-center space-x-2.5">
          <div className="p-1.5 bg-cyan-950/60 rounded-lg border border-cyan-800/50 text-cyan-400">
            <Layers className="w-4 h-4" />
          </div>
          <div>
            <div className="flex items-center space-x-2">
              <h2 className="text-xs font-bold text-slate-100">板块概念</h2>
              <span className="text-[10px] font-mono text-cyan-400 bg-cyan-950 px-2 py-0.5 rounded border border-cyan-800/60">
                共 {boards.length} 个{conceptTableId.includes('concept') ? '概念' : '行业'}板块
              </span>
            </div>
            <p className="text-[10px] text-slate-400">概念与行业板块成分股穿透</p>
          </div>
        </div>

        {/* 概念 vs 行业 切换按钮 */}
        <div className="flex items-center bg-slate-950 p-0.5 rounded-lg border border-slate-800">
          <button
            onClick={() => setConceptTableId('ashare.concept.eastmoney')}
            className={`px-2.5 py-1 rounded text-xs font-medium transition-all cursor-pointer ${
              conceptTableId === 'ashare.concept.eastmoney'
                ? 'bg-cyan-500 text-slate-950 font-bold'
                : 'text-slate-400 hover:text-slate-200'
            }`}
          >
            概念板块
          </button>
          <button
            onClick={() => setConceptTableId('ashare.industry.eastmoney')}
            className={`px-2.5 py-1 rounded text-xs font-medium transition-all cursor-pointer ${
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
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-3 min-h-[500px]">
        {/* 左侧 (4列): 板块搜索与紧凑列表 */}
        <div className="lg:col-span-4 bg-slate-900/80 rounded-xl border border-slate-800 p-2.5 flex flex-col space-y-2">
          <div className="flex items-center justify-between">
            <span className="text-[11px] font-bold text-slate-300">板块列表</span>
            <span className="text-[10px] font-mono text-slate-500">{boards.length} 个板块</span>
          </div>

          <div className="relative">
            <Search className="w-3.5 h-3.5 text-slate-400 absolute left-2.5 top-1/2 -translate-y-1/2" />
            <input
              id="conceptSearchInput"
              name="conceptSearchInput"
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="搜索板块名称或代码..."
              aria-label="搜索板块名称或代码"
              className="w-full bg-slate-950 border border-slate-800 rounded-lg pl-8 pr-2.5 py-1 text-xs text-slate-200 focus:outline-none focus:border-cyan-500"
            />
          </div>

          <div className="flex-1 overflow-y-auto max-h-[550px] space-y-1 pr-0.5">
            {loading ? (
              <div className="p-4 text-center text-xs text-slate-400 animate-pulse">
                加载板块列表中...
              </div>
            ) : error ? (
              <div className="p-3 text-center text-xs text-red-400">{error}</div>
            ) : boards.length === 0 ? (
              <div className="p-4 text-center text-xs text-slate-500">未找到匹配的板块</div>
            ) : (
              boards.map((board) => {
                const isSelected = board.board_code === selectedBoardCode;
                return (
                  <div
                    key={board.board_code}
                    onClick={() => setSelectedBoardCode(board.board_code)}
                    className={`px-2.5 py-2 rounded-lg border cursor-pointer transition-all flex items-center justify-between ${
                      isSelected
                        ? 'bg-cyan-950/50 border-cyan-500/60 text-cyan-300'
                        : 'bg-slate-950/40 border-slate-800/80 text-slate-300 hover:bg-slate-800/40'
                    }`}
                  >
                    <div>
                      <div className="text-xs font-bold">{board.board_name}</div>
                      <div className="text-[10px] font-mono text-slate-500">{board.board_code}</div>
                    </div>
                    <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-slate-900 text-slate-400 border border-slate-800">
                      {board.stock_count}只
                    </span>
                  </div>
                );
              })
            )}
          </div>
        </div>

        {/* 右侧 (8列): 成分股列表 */}
        <div className="lg:col-span-8 bg-slate-900/80 rounded-xl border border-slate-800 p-3.5 flex flex-col space-y-3">
          {currentBoard ? (
            <>
              {/* 板块 Header */}
              <div className="pb-2 border-b border-slate-800 flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <Building2 className="w-4 h-4 text-cyan-400" />
                  <h3 className="text-sm font-bold text-slate-100">{currentBoard.board_name}</h3>
                  <span className="text-[10px] font-mono text-cyan-400 px-1.5 py-0.5 rounded bg-cyan-950 border border-cyan-800">
                    {currentBoard.board_code}
                  </span>
                  <span className="text-xs text-slate-400">
                    ({currentBoard.stock_count} 只成分股)
                  </span>
                </div>
              </div>

              {/* 成分股网格 */}
              {stockLoading ? (
                <div className="p-8 text-center text-xs text-slate-400 animate-pulse">
                  正在加载成分股...
                </div>
              ) : (
                <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-2 overflow-y-auto max-h-[500px] pr-0.5">
                  {currentBoard.stocks.map((stock) => (
                    <div
                      key={stock.symbol}
                      onClick={() => onSelectStock(stock.symbol)}
                      className="px-2.5 py-2 bg-slate-950/60 border border-slate-800 hover:border-cyan-500/60 hover:bg-cyan-950/20 rounded-lg cursor-pointer transition-all group flex items-center justify-between"
                    >
                      <div>
                        <div className="text-xs font-bold text-slate-200 group-hover:text-cyan-400 transition-colors">
                          {stock.stock_name}
                        </div>
                        <div className="text-[10px] font-mono text-slate-500">{stock.symbol}</div>
                      </div>
                      <ArrowRight className="w-3.5 h-3.5 text-slate-600 group-hover:text-cyan-400 group-hover:translate-x-0.5 transition-all" />
                    </div>
                  ))}
                </div>
              )}
            </>
          ) : (
            <div className="flex-1 flex items-center justify-center text-xs text-slate-500">
              请在左侧选择板块
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
