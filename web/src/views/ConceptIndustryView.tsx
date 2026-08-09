import React from 'react';
import { useConceptData } from '../hooks/useConceptData';
import { Layers, Search, ArrowRight, Sparkles, Building2 } from 'lucide-react';

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
    error,
  } = useConceptData();

  // 同步顶栏全局搜索框输入到板块视图
  React.useEffect(() => {
    if (globalSearchQuery !== undefined) {
      setSearchQuery(globalSearchQuery);
    }
  }, [globalSearchQuery, setSearchQuery]);

  return (
    <div className="space-y-4">
      {/* 视图页头: 支持东财概念板块 / 行业板块切换 */}
      <div className="bg-slate-900/60 p-4 rounded-2xl border border-slate-800 flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div className="flex items-center space-x-3">
          <div className="p-2.5 bg-cyan-950/60 rounded-xl border border-cyan-800/50 text-cyan-400">
            <Layers className="w-5 h-5" />
          </div>
          <div>
            <div className="flex items-center space-x-2">
              <h2 className="text-sm font-bold text-slate-100">东方财富 板块概念与成分股穿透</h2>
              <span className="text-[10px] font-semibold bg-emerald-950 text-emerald-400 border border-emerald-800 px-2 py-0.5 rounded-full flex items-center">
                <Sparkles className="w-3 h-3 mr-1" />
                方案A 极速穿透
              </span>
            </div>
            <p className="text-xs text-slate-400 mt-0.5">
              初始化一次性全量拉取，前端内存按 <span className="font-mono text-cyan-400">board_code</span> 分组聚合，提供 0 延时实时联动体验
            </p>
          </div>
        </div>

        {/* 概念 vs 行业 切换按钮 */}
        <div className="flex items-center bg-slate-950 p-1 rounded-xl border border-slate-800 self-start md:self-auto">
          <button
            onClick={() => setConceptTableId('ashare.concept.eastmoney')}
            className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all cursor-pointer ${
              conceptTableId === 'ashare.concept.eastmoney'
                ? 'bg-cyan-500 text-slate-950 font-bold shadow-md shadow-cyan-950'
                : 'text-slate-400 hover:text-slate-200'
            }`}
          >
            概念板块成分股
          </button>
          <button
            onClick={() => setConceptTableId('ashare.industry.eastmoney')}
            className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all cursor-pointer ${
              conceptTableId === 'ashare.industry.eastmoney'
                ? 'bg-cyan-500 text-slate-950 font-bold shadow-md shadow-cyan-950'
                : 'text-slate-400 hover:text-slate-200'
            }`}
          >
            行业板块成分股
          </button>
        </div>
      </div>

      {/* 主布局: 左侧板块列表 + 右侧成分股穿透明细 */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-4 min-h-[500px]">
        {/* 左侧 (4列): 板块搜索与滚动列表 */}
        <div className="lg:col-span-4 bg-slate-900/80 rounded-2xl border border-slate-800 p-3.5 flex flex-col space-y-3">
          <div className="relative">
            <Search className="w-4 h-4 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2" />
            <input
              id="conceptSearchInput"
              name="conceptSearchInput"
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="搜索板块名称或成分股代码..."
              aria-label="搜索板块名称或成分股代码"
              className="w-full bg-slate-950 border border-slate-800 rounded-xl pl-9 pr-3 py-1.5 text-xs text-slate-200 focus:outline-none focus:border-cyan-500"
            />
          </div>

          <div className="flex-1 overflow-y-auto max-h-[550px] space-y-1.5 pr-1">
            {loading ? (
              <div className="p-6 text-center text-xs text-slate-400 animate-pulse">
                正在进行全量板块内存加载与索引构建...
              </div>
            ) : error ? (
              <div className="p-4 text-center text-xs text-red-400">{error}</div>
            ) : boards.length === 0 ? (
              <div className="p-6 text-center text-xs text-slate-500">未找到匹配的板块</div>
            ) : (
              boards.map((board) => {
                const isSelected = board.board_code === selectedBoardCode;
                return (
                  <div
                    key={board.board_code}
                    onClick={() => setSelectedBoardCode(board.board_code)}
                    className={`p-3 rounded-xl border cursor-pointer transition-all flex items-center justify-between ${
                      isSelected
                        ? 'bg-cyan-950/40 border-cyan-500/60 text-cyan-300 shadow-md'
                        : 'bg-slate-950/40 border-slate-800/80 text-slate-300 hover:bg-slate-800/40'
                    }`}
                  >
                    <div>
                      <div className="text-xs font-bold">{board.board_name}</div>
                      <div className="text-[10px] font-mono text-slate-500">{board.board_code}</div>
                    </div>
                    <span className="text-[10px] font-mono px-2 py-0.5 rounded bg-slate-900 text-slate-400 border border-slate-800">
                      {board.stock_count}只
                    </span>
                  </div>
                );
              })
            )}
          </div>
        </div>

        {/* 右侧 (8列): 当前选中板块的成分股穿透卡片 */}
        <div className="lg:col-span-8 bg-slate-900/80 rounded-2xl border border-slate-800 p-5 flex flex-col space-y-4">
          {currentBoard ? (
            <>
              {/* 板块卡片 Header */}
              <div className="pb-3 border-b border-slate-800 flex items-center justify-between">
                <div>
                  <div className="flex items-center space-x-2">
                    <Building2 className="w-5 h-5 text-cyan-400" />
                    <h3 className="text-base font-bold text-slate-100">{currentBoard.board_name}</h3>
                    <span className="text-xs font-mono text-cyan-400 px-2 py-0.5 rounded bg-cyan-950 border border-cyan-800">
                      {currentBoard.board_code}
                    </span>
                  </div>
                  <p className="text-xs text-slate-400 mt-1">
                    包含 <span className="text-amber-400 font-bold">{currentBoard.stock_count}</span> 只成分股标的，点击成分股卡片可直接穿透跳转至 K 线详情图表
                  </p>
                </div>
              </div>

              {/* 成分股网格列表 */}
              <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-3 overflow-y-auto max-h-[500px] pr-1">
                {currentBoard.stocks.map((stock) => (
                  <div
                    key={stock.symbol}
                    onClick={() => onSelectStock(stock.symbol)}
                    className="p-3 bg-slate-950/60 border border-slate-800 hover:border-cyan-500/60 hover:bg-cyan-950/20 rounded-xl cursor-pointer transition-all group flex items-center justify-between"
                  >
                    <div>
                      <div className="text-xs font-bold text-slate-200 group-hover:text-cyan-400 transition-colors">
                        {stock.stock_name}
                      </div>
                      <div className="text-[10px] font-mono text-slate-500">{stock.symbol}</div>
                    </div>
                    <ArrowRight className="w-4 h-4 text-slate-600 group-hover:text-cyan-400 group-hover:translate-x-0.5 transition-all" />
                  </div>
                ))}
              </div>
            </>
          ) : (
            <div className="flex-1 flex items-center justify-center text-xs text-slate-500">
              请在左侧选择板块以查看成分股穿透明细
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
