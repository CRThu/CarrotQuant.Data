import React, { useState } from 'react';
import { useMarketData } from '../hooks/useMarketData';
import { TradingViewKLineChart } from '../components/TradingViewKLineChart';
import { ErrorBoundary } from '../components/ErrorBoundary';
import { BarChart2, Table, RefreshCw, AlertCircle, Loader2 } from 'lucide-react';
import type { ColorMode } from '../types/api';
import { SearchInput } from '../components/SearchInput';

interface StockDetailViewProps {
  currentTableId: string;
  selectedSymbol: string;
  onSymbolChange?: (symbol: string) => void;
  onOpenMatrix?: () => void;
  colorMode?: ColorMode;
}

export const StockDetailView: React.FC<StockDetailViewProps> = ({
  currentTableId,
  selectedSymbol,
  onSymbolChange,
  onOpenMatrix,
  colorMode = 'redUpGreenDown',
}) => {
  const {
    tableId,
    symbol,
    loading,
    error,
    ohlcBars,
    volumeBars,
    maData,
    refreshData,
  } = useMarketData(currentTableId, selectedSymbol, colorMode);

  const [symbolInput, setSymbolInput] = useState<string>(selectedSymbol);

  const handleSymbolSelect = (sym: string) => {
    setSymbolInput(sym);
    if (onSymbolChange) {
      onSymbolChange(sym.trim().toLowerCase());
    }
  };

  return (
    <div className="h-full flex flex-col space-y-2 overflow-hidden animate-in fade-in duration-300">
      {/* 视图页头: 统一风格的工作区表头 */}
      <div className="bg-slate-900/60 p-3 rounded-2xl border border-slate-800 flex flex-col sm:flex-row sm:items-center justify-between gap-3 shrink-0">
        <div className="flex items-center space-x-3">
          <div className="p-2 bg-cyan-950/60 rounded-xl border border-cyan-800/50 text-cyan-400">
            <BarChart2 className="w-5 h-5" />
          </div>
          <div>
            <div className="flex items-center space-x-2">
              <h1 className="text-sm font-bold text-slate-100 tracking-wide">K 线行情</h1>
              <span className="text-[10px] font-mono bg-cyan-950 text-cyan-400 border border-cyan-800/60 px-2 py-0.5 rounded-full">
                {symbol}
              </span>
              <span className="text-[10px] font-mono text-slate-400 hidden lg:inline">
                ({tableId})
              </span>
              {loading && (
                <span className="flex items-center space-x-1 text-[10px] font-mono bg-amber-950/80 text-amber-400 border border-amber-800/60 px-2 py-0.5 rounded-full animate-pulse">
                  <Loader2 className="w-3 h-3 animate-spin" />
                  <span>请求数据中...</span>
                </span>
              )}
            </div>
            <p className="text-[10px] text-slate-400 mt-0.5">
              按时间序列展现 OHLC 与成交量明细 · 共 <span className="text-amber-400 font-mono font-bold">{ohlcBars.length}</span> 条
            </p>
          </div>
        </div>

        {/* 控制选项 */}
        <div className="flex items-center space-x-2">
          <div className="w-44 sm:w-60">
            <SearchInput
              value={symbolInput}
              onChange={setSymbolInput}
              placeholder="搜索代码/拼音/名称 (如 00 / 600000 / 浦发)..."
              onSelect={(item) => handleSymbolSelect(item.code)}
            />
          </div>

          {onOpenMatrix && (
            <button
              onClick={onOpenMatrix}
              className="flex items-center space-x-1.5 px-3 py-1.5 bg-slate-950 hover:bg-slate-800 border border-slate-800 text-slate-300 rounded-xl text-xs font-medium transition-colors cursor-pointer shrink-0"
              title="打开独立数据矩阵切片视图"
            >
              <Table className="w-3.5 h-3.5 text-cyan-400" />
              <span>数据矩阵</span>
            </button>
          )}

          <button
            onClick={refreshData}
            disabled={loading}
            className="p-2 bg-slate-950 hover:bg-slate-800 text-slate-300 rounded-xl border border-slate-800 transition-colors cursor-pointer disabled:opacity-50 shrink-0"
            title="刷新数据"
          >
            <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* 错误警告 Banner */}
      {error && (
        <div className="p-2.5 bg-red-950/40 border border-red-800/60 rounded-xl text-xs text-red-300 flex items-center space-x-2 shrink-0">
          <AlertCircle className="w-4 h-4 text-red-400 shrink-0" />
          <span>{error}</span>
        </div>
      )}

      {/* 极速 K 线 + 成交量 2-Pane 联动图表 */}
      <div className="flex-1 min-h-0 w-full">
        <ErrorBoundary fallbackTitle="K 线图表组件渲染异常拦截">
          <TradingViewKLineChart
            ohlcBars={ohlcBars}
            volumeBars={volumeBars}
            maData={maData}
            colorMode={colorMode}
          />
        </ErrorBoundary>
      </div>
    </div>
  );
};

export default StockDetailView;
