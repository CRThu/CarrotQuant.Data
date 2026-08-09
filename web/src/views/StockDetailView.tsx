import React, { useState } from 'react';
import { useMarketData } from '../hooks/useMarketData';
import { TradingViewKLineChart } from '../components/TradingViewKLineChart';
import { DataTable } from '../components/DataTable';
import { ErrorBoundary } from '../components/ErrorBoundary';
import { BarChart2, Table, RefreshCw, AlertCircle } from 'lucide-react';
import type { ColorMode } from '../types/api';

interface StockDetailViewProps {
  currentTableId: string;
  selectedSymbol: string;
  onSymbolChange?: (symbol: string) => void;
  colorMode?: ColorMode;
}

export const StockDetailView: React.FC<StockDetailViewProps> = ({
  currentTableId,
  selectedSymbol,
  colorMode = 'redUpGreenDown',
}) => {
  const {
    tableId,
    symbol,
    barLimit,
    setBarLimit,
    selectedIndicator,
    setSelectedIndicator,
    loading,
    error,
    ohlcBars,
    volumeBars,
    maData,
    macdData,
    rsiData,
    markers,
    matrixRaw,
    refreshData,
  } = useMarketData(currentTableId, selectedSymbol, colorMode);

  const [showMatrixTable, setShowMatrixTable] = useState<boolean>(false);

  return (
    <div className="space-y-4">
      {/* 视图页头: 股票信息与按钮控制 */}
      <div className="bg-slate-900/60 p-4 rounded-2xl border border-slate-800 flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div className="flex items-center space-x-3">
          <div className="p-2.5 bg-cyan-950/60 rounded-xl border border-cyan-800/50 text-cyan-400">
            <BarChart2 className="w-5 h-5" />
          </div>
          <div>
            <div className="flex items-center space-x-2">
              <h2 className="text-base font-extrabold text-slate-100 font-mono tracking-wide">{symbol}</h2>
              <span className="text-[10px] font-mono bg-cyan-950 text-cyan-400 border border-cyan-800 px-2 py-0.5 rounded-full">
                {tableId}
              </span>
            </div>
            <p className="text-xs text-slate-400 mt-0.5">
              数据行数: <span className="text-amber-400 font-mono font-bold">{ohlcBars.length}</span> 条 | 买卖点标记数: <span className="text-red-400 font-mono font-bold">{markers.length}</span> 个
            </p>
          </div>
        </div>

        {/* 控制选项: 切换股票、刷新、查看 2D 矩阵 */}
        <div className="flex items-center space-x-3">
          <button
            onClick={() => setShowMatrixTable(!showMatrixTable)}
            className={`flex items-center space-x-1.5 px-3 py-1.5 rounded-xl text-xs font-medium border transition-all cursor-pointer ${
              showMatrixTable
                ? 'bg-cyan-950 border-cyan-500/60 text-cyan-300'
                : 'bg-slate-950 border-slate-800 text-slate-400 hover:text-slate-200'
            }`}
          >
            <Table className="w-3.5 h-3.5" />
            <span>{showMatrixTable ? '隐藏 2D 矩阵' : '查看 2D 矩阵'}</span>
          </button>

          <button
            onClick={refreshData}
            disabled={loading}
            className="p-2 bg-slate-950 hover:bg-slate-800 text-slate-300 rounded-xl border border-slate-800 transition-colors cursor-pointer disabled:opacity-50"
            title="刷新数据"
          >
            <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* 错误警告 Banner */}
      {error && (
        <div className="p-4 bg-red-950/40 border border-red-800/60 rounded-2xl text-xs text-red-300 flex items-center space-x-2">
          <AlertCircle className="w-4 h-4 text-red-400 shrink-0" />
          <span>{error}</span>
        </div>
      )}

      {/* 3-Pane TradingView 极速 K 线图表 (带 ErrorBoundary) */}
      <ErrorBoundary fallbackTitle="K 线图表组件渲染异常拦截">
        <TradingViewKLineChart
          ohlcBars={ohlcBars}
          volumeBars={volumeBars}
          maData={maData}
          macdData={macdData}
          rsiData={rsiData}
          markers={markers}
          selectedIndicator={selectedIndicator}
          onIndicatorChange={setSelectedIndicator}
          barLimit={barLimit}
          onBarLimitChange={setBarLimit}
          colorMode={colorMode}
        />
      </ErrorBoundary>

      {/* 2D 矩阵数据表格 (条件渲染) */}
      {showMatrixTable && <DataTable matrix={matrixRaw} loading={loading} colorMode={colorMode} />}
    </div>
  );
};
