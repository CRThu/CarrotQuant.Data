import React, { useState, useEffect } from 'react';
import type { SyncStatusItem } from '../types/api';
import { Loader2, Minimize2, CheckCircle2, AlertTriangle, X } from 'lucide-react';

interface FloatingSyncWidgetProps {
  activeStatus?: SyncStatusItem | null;
  onNavigateToManagement?: () => void;
}

export const FloatingSyncWidget: React.FC<FloatingSyncWidgetProps> = ({
  activeStatus,
}) => {
  const [minimized, setMinimized] = useState<boolean>(false);
  const [dismissed, setDismissed] = useState<boolean>(false);

  // 当同步任务更新或表 ID 改变时，自动重新唤醒浮窗显示
  useEffect(() => {
    if (activeStatus?.table_id && activeStatus.status === 'running') {
      setDismissed(false);
    }
  }, [activeStatus?.table_id, activeStatus?.status]);

  if (!activeStatus || activeStatus.status === 'idle' || dismissed) {
    return null;
  }

  const isRunning = activeStatus.status === 'running';
  const isSuccess = activeStatus.status === 'success';
  const isFailed = activeStatus.status === 'failed';

  // 最小化胶囊状态
  if (minimized) {
    return (
      <div className="fixed bottom-5 right-5 z-50 animate-in fade-in duration-300">
        <button
          onClick={() => setMinimized(false)}
          className={`px-3 py-1.5 rounded-full flex items-center space-x-2 border cursor-pointer transition-colors ${
            isRunning
              ? 'bg-slate-900/98 border-cyan-500/60 text-cyan-300'
              : isSuccess
              ? 'bg-emerald-950/98 border-emerald-500/60 text-emerald-300'
              : 'bg-red-950/98 border-red-500/60 text-red-300'
          }`}
          title="点击展开同步进度"
        >
          {isRunning && <Loader2 className="w-3.5 h-3.5 animate-spin text-cyan-400" />}
          {isSuccess && <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400" />}
          {isFailed && <AlertTriangle className="w-3.5 h-3.5 text-red-400" />}
          <span className="text-xs font-mono font-bold">
            {isRunning ? `${activeStatus.percentage.toFixed(0)}%` : isSuccess ? '完成' : '失败'}
          </span>
        </button>
      </div>
    );
  }

  // 展开紧凑型卡片状态
  return (
    <div className="fixed bottom-5 right-5 z-50 w-72 bg-slate-900/98 border border-slate-800 rounded-xl overflow-hidden animate-in slide-in-from-bottom-3 duration-200">
      {/* 顶部 Header：表名与控制按钮 */}
      <div className="px-3 py-1.5 bg-slate-950/80 border-b border-slate-800 flex items-center justify-between">
        <div className="flex items-center space-x-2 text-xs font-semibold text-slate-200 min-w-0">
          {isRunning && <Loader2 className="w-3.5 h-3.5 animate-spin text-cyan-400 shrink-0" />}
          {isSuccess && <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400 shrink-0" />}
          {isFailed && <AlertTriangle className="w-3.5 h-3.5 text-red-400 shrink-0" />}
          <span className="truncate text-slate-300 font-mono text-[11px]">{activeStatus.table_id}</span>
        </div>

        <div className="flex items-center space-x-1 shrink-0 ml-2">
          <button
            onClick={() => setMinimized(true)}
            className="p-1 text-slate-400 hover:text-slate-200 hover:bg-slate-800 rounded transition-colors cursor-pointer"
            title="最小化"
          >
            <Minimize2 className="w-3 h-3" />
          </button>
          <button
            onClick={() => setDismissed(true)}
            className="p-1 text-slate-400 hover:text-slate-200 hover:bg-slate-800 rounded transition-colors cursor-pointer"
            title="关闭浮窗"
          >
            <X className="w-3 h-3" />
          </button>
        </div>
      </div>

      {/* Body 描述与进度条：拆为独立两行布局，防止长文本挤压折行 */}
      <div className="p-3 space-y-2">
        {/* 第一行：状态/细节描述文本，自动截断防溢出 */}
        <div className="text-[11px] text-slate-300 truncate font-mono" title={activeStatus.message || ''}>
          {activeStatus.message || (activeStatus.current_symbol ? `正在下载 ${activeStatus.current_symbol}` : '规划处理中...')}
        </div>

        {/* 第二行：明确标注进度数量与百分比 */}
        <div className="flex items-center justify-between text-xs font-mono">
          <span className="text-slate-400 text-[11px]">同步进度</span>
          <span className="font-bold text-cyan-400">
            {activeStatus.current} / {activeStatus.total} ({activeStatus.percentage.toFixed(0)}%)
          </span>
        </div>

        {/* 进度条 */}
        <div className="w-full bg-slate-950 rounded-full h-1.5 overflow-hidden border border-slate-800">
          <div
            className={`h-full rounded-full transition-all duration-300 ${
              isFailed ? 'bg-red-500' : 'bg-gradient-to-r from-cyan-500 to-blue-500'
            }`}
            style={{ width: `${Math.min(100, Math.max(0, activeStatus.percentage))}%` }}
          />
        </div>

        {/* 失败错误提示 */}
        {isFailed && activeStatus.error_msg && (
          <div className="text-[10px] text-red-400 bg-red-950/40 p-1.5 rounded border border-red-900/40 truncate" title={activeStatus.error_msg}>
            {activeStatus.error_msg}
          </div>
        )}
      </div>
    </div>
  );
};

