import React, { useState, useEffect, useMemo } from 'react';
import { apiClient } from '../services/apiClient';
import { TrendingUp, ArrowRight } from 'lucide-react';

interface StockListViewProps {
  currentTableId: string;
  onSelectStock: (symbol: string) => void;
  searchQuery: string;
}

export const StockListView: React.FC<StockListViewProps> = ({
  currentTableId,
  onSelectStock,
  searchQuery,
}) => {
  const [symbols, setSymbols] = useState<string[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  // 初始化拉取当前数据表中的全量 Symbol 清单
  useEffect(() => {
    const fetchSymbols = async () => {
      setLoading(true);
      setError(null);
      try {
        const res = await apiClient.listSymbols(currentTableId);
        setSymbols(res.symbols || []);
      } catch (err: any) {
        console.error('Failed to list symbols:', err);
        setError('无法从本地元数据获取该表的代码列表');
        setSymbols([]);
      } finally {
        setLoading(false);
      }
    };

    fetchSymbols();
  }, [currentTableId]);

  const [displayLimit, setDisplayLimit] = useState<number>(180);

  // 根据搜索关键字过滤股票
  const filteredSymbols = useMemo(() => {
    if (!searchQuery.trim()) return symbols;
    const q = searchQuery.toLowerCase().trim();
    return symbols.filter((s) => s.toLowerCase().includes(q));
  }, [symbols, searchQuery]);

  // 按 displayLimit 进行 DOM 呈现切片，保障极致流畅度
  const visibleSymbols = useMemo(() => {
    if (searchQuery.trim()) return filteredSymbols; // 有搜索词时直接呈现搜索结果
    return filteredSymbols.slice(0, displayLimit);
  }, [filteredSymbols, displayLimit, searchQuery]);

  return (
    <div className="space-y-4">
      {/* 视图页头说明 */}
      <div className="bg-slate-900/60 p-4 rounded-2xl border border-slate-800 flex items-center justify-between">
        <div className="flex items-center space-x-3">
          <div className="p-2.5 bg-cyan-950/60 rounded-xl border border-cyan-800/50 text-cyan-400">
            <TrendingUp className="w-5 h-5" />
          </div>
          <div>
            <h2 className="text-sm font-bold text-slate-100">全市场股票搜索与自选库</h2>
            <p className="text-xs text-slate-400 mt-0.5">
              对应当前数据表 <span className="font-mono text-cyan-400">{currentTableId}</span> 的本地已清洗代码列表
            </p>
          </div>
        </div>
        <div className="text-xs font-mono text-slate-400 bg-slate-950 px-3 py-1.5 rounded-xl border border-slate-800">
          已包含 <span className="text-cyan-400 font-bold">{symbols.length}</span> 只证券代码
        </div>
      </div>

      {/* 股票网格列表 */}
      {loading ? (
        <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3">
          {Array.from({ length: 18 }).map((_, i) => (
            <div key={i} className="h-16 bg-slate-900/40 border border-slate-800/60 rounded-xl animate-pulse" />
          ))}
        </div>
      ) : error ? (
        <div className="p-8 text-center text-xs text-red-400 bg-slate-900/40 rounded-2xl border border-slate-800">
          {error}
        </div>
      ) : filteredSymbols.length === 0 ? (
        <div className="p-12 text-center text-xs text-slate-500 bg-slate-900/40 rounded-2xl border border-slate-800">
          未找到匹配的证券代码
        </div>
      ) : (
        <div className="space-y-4">
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-3">
            {visibleSymbols.map((sym) => (
              <div
                key={sym}
                onClick={() => onSelectStock(sym)}
                className="bg-slate-900/80 border border-slate-800/80 hover:border-cyan-500/50 hover:bg-cyan-950/20 p-3.5 rounded-xl cursor-pointer transition-all duration-200 group flex items-center justify-between shadow-md"
              >
                <div>
                  <div className="text-xs font-bold font-mono text-slate-200 group-hover:text-cyan-400 transition-colors">
                    {sym}
                  </div>
                  <div className="text-[10px] text-slate-500 mt-0.5">A股交易标的</div>
                </div>
                <ArrowRight className="w-4 h-4 text-slate-600 group-hover:text-cyan-400 group-hover:translate-x-0.5 transition-all" />
              </div>
            ))}
          </div>

          {/* 大列表加载更多控制按钮 */}
          {!searchQuery.trim() && displayLimit < filteredSymbols.length && (
            <div className="text-center pt-2">
              <button
                onClick={() => setDisplayLimit((prev) => prev + 300)}
                className="px-4 py-2 bg-slate-900 hover:bg-slate-800 border border-slate-800 text-xs text-cyan-400 rounded-xl transition-colors cursor-pointer font-mono"
              >
                加载更多标的 ({visibleSymbols.length} / {filteredSymbols.length})
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
};
