import React, { useState } from 'react';
import type { SyncStatusItem } from '../types/api';
import { Loader2, Minimize2, ExternalLink, CheckCircle2, AlertTriangle } from 'lucide-react';

interface FloatingSyncWidgetProps {
  activeStatus?: SyncStatusItem | null;
  onNavigateToManagement: () => void;
}

export const FloatingSyncWidget: React.FC<FloatingSyncWidgetProps> = ({
  activeStatus,
  onNavigateToManagement,
}) => {
  const [minimized, setMinimized] = useState<boolean>(false);

  if (!activeStatus || activeStatus.status === 'idle') {
    return null;
  }

  const isRunning = activeStatus.status === 'running';
  const isSuccess = activeStatus.status === 'success';
  const isFailed = activeStatus.status === 'failed';

  // 缩小图标状态
  if (minimized) {
    return (
      <div className="fixed bottom-6 right-6 z-50 animate-in fade-in duration-300">
        <button
          onClick={() => setMinimized(false)}
          className={`px-3 py-2 rounded-full shadow-2xl flex items-center space-x-2 backdrop-blur-md border cursor-pointer transition-all hover:scale-105 ${
            isRunning
              ? 'bg-slate-900/90 border-cyan-500/60 text-cyan-300 shadow-cyan-950/50'
              : isSuccess
              ? 'bg-emerald-950/90 border-emerald-500/60 text-emerald-300'
              : 'bg-red-950/90 border-red-500/60 text-red-300'
          }`}
          title="点击展开同步进度面板"
        >
          {isRunning && <Loader2 className="w-4 h-4 animate-spin text-cyan-400" />}
          {isSuccess && <CheckCircle2 className="w-4 h-4 text-emerald-400" />}
          {isFailed && <AlertTriangle className="w-4 h-4 text-red-400" />}
          <span className="text-xs font-mono font-bold">
            {isRunning ? `${activeStatus.percentage.toFixed(0)}%` : isSuccess ? '完成' : '失败'}
          </span>
        </button>
      </div>
    );
  }

  // 展开 Mini 卡片状态
  return (
    <div className="fixed bottom-6 right-6 z-50 w-80 bg-slate-900/95 border border-slate-800 rounded-2xl shadow-2xl backdrop-blur-md overflow-hidden animate-in slide-in-from-bottom-5 duration-200">
      {/* 顶部 Handler */}
      <div className="px-3.5 py-2 bg-slate-950/80 border-b border-slate-800 flex items-center justify-between">
        <div className="flex items-center space-x-2 text-xs font-semibold text-slate-200">
          {isRunning && <Loader2 className="w-3.5 h-3.5 animate-spin text-cyan-400" />}
          {isSuccess && <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400" />}
          {isFailed && <AlertTriangle className="w-3.5 h-3.5 text-red-400" />}
          <span className="truncate max-w-[140px]">{activeStatus.table_id}</span>
        </div>

        <div className="flex items-center space-x-1">
          <button
            onClick={() => setMinimized(true)}
            className="p-1 text-slate-400 hover:text-slate-200 hover:bg-slate-800 rounded-md transition-colors cursor-pointer"
            title="最小化"
          >
            <Minimize2 className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>

      {/* Body 描述与进度条 */}
      <div className="p-3.5 space-y-2.5">
        <div className="flex items-center justify-between text-xs font-mono">
          <span className="text-slate-300 truncate max-w-[200px]" title={activeStatus.message || ''}>
            {activeStatus.message || (activeStatus.current_symbol ? `正在下载 ${activeStatus.current_symbol}` : '规划处理中...')}
          </span>
          <span className="font-bold text-cyan-400">
            {activeStatus.current} / {activeStatus.total} ({activeStatus.percentage.toFixed(0)}%)
          </span>
        </div>

        <div className="w-full bg-slate-950 rounded-full h-2 overflow-hidden border border-slate-800">
          <div
            className={`h-full rounded-full transition-all duration-300 ${
              isFailed ? 'bg-red-500' : 'bg-gradient-to-r from-cyan-500 to-blue-500'
            }`}
            style={{ width: `${Math.min(100, Math.max(0, activeStatus.percentage))}%` }}
          />
        </div>

        {/* 失败错误提示 */}
        {isFailed && activeStatus.error_msg && (
          <div className="text-[10px] text-red-400 bg-red-950/40 p-1.5 rounded border border-red-900/40 truncate">
            {activeStatus.error_msg}
          </div>
        )}

        {/* 快捷跳转至数据管理 */}
        <button
          onClick={onNavigateToManagement}
          className="w-full py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-xl text-xs font-medium transition-colors flex items-center justify-center space-x-1 cursor-pointer"
        >
          <span>打开数据管理中心</span>
          <ExternalLink className="w-3 h-3 text-cyan-400" />
        </button>
      </div>
    </div>
  );
};
