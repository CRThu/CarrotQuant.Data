import React, { useState, useEffect, useMemo } from 'react';
import { apiClient } from '../services/apiClient';
import { TrendingUp, ArrowRight, Star, Upload, X } from 'lucide-react';
import { SearchInput } from '../components/SearchInput';
import { stockDictionary } from '../services/stockDictionary';

interface StockListViewProps {
  currentTableId: string;
  onSelectStock: (symbol: string) => void;
  searchQuery?: string;
}

export const StockListView: React.FC<StockListViewProps> = ({
  currentTableId,
  onSelectStock,
  searchQuery = '',
}) => {
  const [symbols, setSymbols] = useState<string[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [localSearch, setLocalSearch] = useState<string>('');
  const [displayLimit, setDisplayLimit] = useState<number>(100);

  // 零硬编码自选股 State (纯从 localStorage 动态载入)
  const [watchlist, setWatchlist] = useState<string[]>(() => {
    try {
      const saved = localStorage.getItem('cqdata_user_watchlist');
      return saved ? JSON.parse(saved) : [];
    } catch {
      return [];
    }
  });

  useEffect(() => {
    localStorage.setItem('cqdata_user_watchlist', JSON.stringify(watchlist));
  }, [watchlist]);

  // 从物理元数据/REST API 动态拉取全量代码清单，并灌入 stockDictionary
  useEffect(() => {
    const fetchSymbols = async () => {
      setLoading(true);
      setError(null);
      try {
        const res = await apiClient.listSymbols(currentTableId);
        const fetchedSymbols = res.symbols || [];
        setSymbols(fetchedSymbols);

        // 动态将拉取到的标的代码注入 stockDictionary (零硬编码)
        stockDictionary.loadSymbols(fetchedSymbols);
      } catch (err: any) {
        console.error('Failed to list symbols:', err);
        setError('无法从本地元数据获取代码清单');
        setSymbols([]);
      } finally {
        setLoading(false);
      }
    };

    fetchSymbols();
  }, [currentTableId]);

  const toggleWatchlist = (sym: string, e: React.MouseEvent) => {
    e.stopPropagation();
    if (watchlist.includes(sym)) {
      setWatchlist(watchlist.filter((s) => s !== sym));
    } else {
      setWatchlist([...watchlist, sym]);
    }
  };

  const removeFromWatchlist = (sym: string, e: React.MouseEvent) => {
    e.stopPropagation();
    setWatchlist(watchlist.filter((s) => s !== sym));
  };

  // 动态检索过滤项目
  const effectiveQuery = localSearch || searchQuery;

  const matchedDictItems = useMemo(() => {
    if (!effectiveQuery.trim()) {
      return stockDictionary.getAll().slice(0, displayLimit);
    }
    return stockDictionary.search(effectiveQuery, 200);
  }, [effectiveQuery, displayLimit, symbols]);

  return (
    <div className="space-y-4 animate-in fade-in duration-300 pb-8">
      {/* 视图 Header: 统一右侧工作区表头 */}
      <div className="bg-slate-900/60 p-3.5 rounded-2xl border border-slate-800 flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div className="flex items-center space-x-3">
          <div className="p-2 bg-cyan-950/60 rounded-xl border border-cyan-800/50 text-cyan-400">
            <TrendingUp className="w-5 h-5" />
          </div>
          <div>
            <div className="flex items-center space-x-2">
              <h1 className="text-sm font-bold text-slate-100">股票搜索</h1>
              <span className="text-[10px] font-mono text-cyan-400 bg-cyan-950 px-2 py-0.5 rounded border border-cyan-800/60">
                {symbols.length} 只标的
              </span>
            </div>
            <p className="text-[10px] text-slate-400 mt-0.5">
              当前数据表 <span className="font-mono text-cyan-400">{currentTableId}</span>
            </p>
          </div>
        </div>

        {/* 极速通用搜索栏 (支持 00/600 纯数字前缀、拼音首字母、中文名称，耗时 < 1ms) */}
        <div className="w-full sm:w-80">
          <SearchInput
            value={localSearch}
            onChange={setLocalSearch}
            placeholder="输入 00 / 600 / 拼音 / 名称 (如 000001)..."
            onSelect={(item) => onSelectStock(item.code)}
          />
        </div>
      </div>

      {/* 自选列表卡片 (带明确 X 删除键) */}
      <div className="bg-slate-900/80 border border-slate-800 rounded-2xl p-3.5 space-y-2.5">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-2">
            <Star className="w-4 h-4 text-amber-400 fill-amber-400" />
            <h2 className="text-xs font-bold text-slate-200">我的自选列表</h2>
            <span className="text-[10px] font-mono text-slate-500">({watchlist.length} 只)</span>
          </div>

          <button
            onClick={() => alert('后续版本将提供自选文本/CSV 批量导入功能')}
            className="px-2.5 py-1 bg-slate-950 hover:bg-slate-800 border border-slate-800 rounded-lg text-[11px] text-slate-400 hover:text-slate-200 transition-colors flex items-center space-x-1 cursor-pointer font-sans"
          >
            <Upload className="w-3 h-3 text-cyan-400" />
            <span>导入自选</span>
          </button>
        </div>

        {watchlist.length === 0 ? (
          <div className="p-4 text-center text-xs text-slate-500 border border-dashed border-slate-800 rounded-xl">
            暂无自选股票，在下方股票列表中点击星标⭐添加
          </div>
        ) : (
          <div className="flex flex-wrap gap-2">
            {watchlist.map((sym) => (
              <div
                key={sym}
                onClick={() => onSelectStock(sym)}
                className="px-2.5 py-1.5 bg-slate-950/90 border border-slate-800 hover:border-cyan-500/60 hover:bg-cyan-950/30 rounded-xl cursor-pointer transition-all flex items-center space-x-2 group"
              >
                <Star className="w-3.5 h-3.5 text-amber-400 fill-amber-400 shrink-0" />
                <span className="text-xs font-mono font-bold text-slate-200 group-hover:text-cyan-300">{sym}</span>
                {/* 明确的 X 删除按键 */}
                <button
                  onClick={(e) => removeFromWatchlist(sym, e)}
                  title="从自选列表中删除"
                  className="p-0.5 text-slate-500 hover:text-red-400 hover:bg-red-950/40 rounded transition-colors cursor-pointer shrink-0 ml-1"
                >
                  <X className="w-3.5 h-3.5" />
                </button>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* 股票紧凑数据列表 */}
      <div className="bg-slate-900/80 border border-slate-800 rounded-2xl overflow-hidden shadow-xl">
        <div className="px-4 py-2.5 bg-slate-950/80 border-b border-slate-800 flex items-center justify-between text-xs text-slate-400 font-semibold">
          <span>全市场代码清单</span>
          <span className="font-mono text-[11px]">显示 {matchedDictItems.length} / {symbols.length}</span>
        </div>

        {loading ? (
          <div className="p-8 text-center text-xs text-slate-400 animate-pulse">正在从物理元数据加载代码...</div>
        ) : error ? (
          <div className="p-6 text-center text-xs text-red-400">{error}</div>
        ) : matchedDictItems.length === 0 ? (
          <div className="p-8 text-center text-xs text-slate-500">未找到匹配的股票代码</div>
        ) : (
          <div className="divide-y divide-slate-800/50">
            {matchedDictItems.map((item) => {
              const isStarred = watchlist.includes(item.symbol);
              return (
                <div
                  key={item.symbol}
                  onClick={() => onSelectStock(item.symbol)}
                  className="px-4 py-2.5 hover:bg-cyan-950/20 transition-colors cursor-pointer flex items-center justify-between group"
                >
                  <div className="flex items-center space-x-3">
                    <Star
                      onClick={(e) => toggleWatchlist(item.symbol, e)}
                      className={`w-3.5 h-3.5 cursor-pointer transition-transform hover:scale-110 ${
                        isStarred ? 'text-amber-400 fill-amber-400' : 'text-slate-600 hover:text-slate-400'
                      }`}
                    />
                    <div className="flex items-center space-x-2">
                      <span className="text-xs font-mono font-bold text-slate-200 group-hover:text-cyan-400 transition-colors">
                        {item.symbol}
                      </span>
                      {item.name !== item.symbol && (
                        <span className="text-xs font-sans text-slate-400">{item.name}</span>
                      )}
                    </div>
                  </div>

                  <div className="flex items-center space-x-2 text-xs text-slate-400">
                    <span className="text-[10px] font-mono text-slate-500 bg-slate-950 px-2 py-0.5 rounded border border-slate-800">
                      {item.market}
                    </span>
                    <ArrowRight className="w-3.5 h-3.5 text-slate-600 group-hover:text-cyan-400 group-hover:translate-x-0.5 transition-all" />
                  </div>
                </div>
              );
            })}
          </div>
        )}

        {!effectiveQuery.trim() && displayLimit < symbols.length && (
          <div className="p-3 text-center bg-slate-950 border-t border-slate-800">
            <button
              onClick={() => setDisplayLimit((prev) => prev + 200)}
              className="px-4 py-1.5 bg-slate-900 hover:bg-slate-800 border border-slate-800 text-xs text-cyan-400 rounded-xl transition-colors cursor-pointer font-mono"
            >
              加载更多代码 ({matchedDictItems.length} / {symbols.length})
            </button>
          </div>
        )}
      </div>
    </div>
  );
};

export default StockListView;
