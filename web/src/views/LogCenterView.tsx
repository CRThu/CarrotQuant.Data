import React from 'react';
import { LogTerminal } from '../components/LogTerminal';
import { Terminal } from 'lucide-react';

export const LogCenterView: React.FC = () => {
  return (
    <div className="space-y-4 animate-in fade-in duration-300 pb-8 flex flex-col h-full">
      {/* 视图 Header: 统一格式的工作区表头 */}
      <div className="bg-slate-900/60 p-3.5 rounded-2xl border border-slate-800 flex flex-col sm:flex-row sm:items-center justify-between gap-4 shrink-0">
        <div className="flex items-center space-x-3">
          <div className="p-2 bg-cyan-950/60 rounded-xl border border-cyan-800/50 text-cyan-400">
            <Terminal className="w-5 h-5" />
          </div>
          <div>
            <h1 className="text-sm font-bold text-slate-100">日志中心</h1>
            <p className="text-[10px] text-slate-400 mt-0.5">
              实时系统与同步流水线日志流
            </p>
          </div>
        </div>
      </div>

      {/* 主日志视窗卡片 (自适应高度) */}
      <div className="flex-1 min-h-[480px]">
        <LogTerminal title="全局日志终端" className="h-full min-h-[480px] shadow-2xl" />
      </div>
    </div>
  );
};
