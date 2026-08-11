import React from 'react';
import { LogTerminal } from '../components/LogTerminal';
import { Terminal } from 'lucide-react';

export const LogCenterView: React.FC = () => {
  return (
    <div className="space-y-6 animate-in fade-in duration-300 pb-8 flex flex-col h-full">
      {/* 顶部标题与说明 */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 shrink-0">
        <div>
          <h1 className="text-xl font-bold text-slate-100 flex items-center space-x-2">
            <Terminal className="w-6 h-6 text-cyan-400" />
            <span>系统日志中心</span>
          </h1>
          <p className="text-xs text-slate-400 mt-1">
            实时流式监听全系统与数据引擎 Loguru 业务日志，包含数据抓取、批次落盘、API 路由与异常诊断
          </p>
        </div>
      </div>

      {/* 主日志视窗卡片 (占据全屏主要区域) */}
      <div className="flex-1 min-h-[500px]">
        <LogTerminal title="全局 Loguru 引擎日志流" className="h-full min-h-[500px] shadow-2xl" />
      </div>
    </div>
  );
};
